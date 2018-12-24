package net.qihoo.xitong.xdml.ps

import java.nio.ByteBuffer
import java.util

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.core.{HazelcastInstance, PartitionService}
import net.qihoo.xitong.xdml.updater.Updater
import net.qihoo.xitong.xdml.conf.PSDataType
import net.qihoo.xitong.xdml.conf.PSDataType.PSDataType
import net.qihoo.xitong.xdml.task._
import net.qihoo.xitong.xdml.utils.XDMLException

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

class PSClient[K: TypeTag, V: TypeTag](val ps: PS) {
	//hz client
	var client: HazelcastInstance = _
	var pullTask: PullTask[Long, V] = _
	var pushTask: PushTask[Long, V] = _
	var partService: PartitionService = _
	private var hasUpdater = false
	private var vClazz: PSDataType = _
	private var updater: Updater[Long, V] = _
	var psDataLength: Int = _
	var clientLocalDataType: PSDataType = _
	var pullRangeStart: Int = _
	var pullRangeEnd: Int = _
	var pullRangeEnable: Boolean = _

	init()

	//get hz client and initialize all variables
	def init(): Unit = {
		val clientConfig = new ClientConfig()
		clientConfig.getGroupConfig.setName(ps.psConf.getHzGroupName).setPassword(ps.psConf.getHzGroupPassword)
		clientConfig.setProperty("hazelcast.clientengine.thread.count", ps.psConf.getHzClientThreadCount.toString)
		clientConfig.setProperty("hazelcast.logging.type", ps.psConf.getHzLoggingType)
		clientConfig.getNetworkConfig.setAddresses(ps.getIPList.toList)
		vClazz = ps.psConf.getPsDataType
		psDataLength = ps.psConf.getPsDataLength
		this.client = HazelcastClient.newHazelcastClient(clientConfig)
		this.pullTask = new PullTask[Long, V]()
		this.pushTask = new PushTask[Long, V]()
		this.pushTask.setVClazz(vClazz)
		this.pushTask.setDataLength(psDataLength)
		this.partService = this.client.getPartitionService
		this.pullRangeStart = 0
		this.pullRangeEnd = psDataLength
		this.pullRangeEnable = false
		validDataType()
	}

	//valid ps-client's data type whether equals PS's data type,if not throw exceptions
	def validDataType(): Unit = {
		typeOf[V] match {
			case t if t <:< typeOf[Int] => clientLocalDataType = PSDataType.INT
			case t if t <:< typeOf[Long] => clientLocalDataType = PSDataType.LONG
			case t if t <:< typeOf[Float] => clientLocalDataType = PSDataType.FLOAT
			case t if t <:< typeOf[Float] => clientLocalDataType = PSDataType.DOUBLE
			case t if t <:< typeOf[Array[Float]] => clientLocalDataType = PSDataType.FLOAT_ARRAY
			case t if t <:< typeOf[Array[Double]] => clientLocalDataType = PSDataType.DOUBLE_ARRAY
		}
		if (vClazz != clientLocalDataType)
			throw new XDMLException("Client data type is not matched with PS.")
	}

	def setPullRange(start: Int, end: Int): this.type = {
		if (PSDataType.isArrayType(vClazz)) {
			this.pullRangeStart = start
			this.pullRangeEnd = end
			pullRangeEnable = true
		} else
			throw new XDMLException("PSDataType config is not a array type.")
		this
	}

	def setPullRange(index: Int): this.type = {
		if (PSDataType.isArrayType(vClazz)) {
			this.pullRangeStart = index
			this.pullRangeEnd = index + 1
			pullRangeEnable = true
		} else
			throw new XDMLException("PSDataType config is not a array type.")
		this
	}

	def setUpdater(updater: Updater[Long, V]): this.type = {
		this.updater = updater
		this.hasUpdater = true
		this
	}

	//shutdown ps client
	def shutDown(): Unit = {
		this.client.shutdown()
	}

	//get V from byte by ps client's data type
	def getRangeVFromBytes(bytes: Array[Byte]): V = {
		val valueBuff = ByteBuffer.wrap(bytes)
		vClazz match {
			case PSDataType.INT => valueBuff.getInt().asInstanceOf[V]
			case PSDataType.LONG => valueBuff.getLong().asInstanceOf[V]
			case PSDataType.FLOAT => valueBuff.getFloat().asInstanceOf[V]
			case PSDataType.DOUBLE => valueBuff.getDouble().asInstanceOf[V]
			case PSDataType.FLOAT_ARRAY => {
				val arr = new Array[Float](psDataLength)
				for (index <- arr.indices) {
					arr(index) = valueBuff.getFloat(index << 2)
				}
				if (pullRangeEnable)
					arr.slice(pullRangeStart, pullRangeEnd).asInstanceOf[V]
				else
					arr.asInstanceOf[V]
			}
			case PSDataType.DOUBLE_ARRAY => {
				val arr = new Array[Double](psDataLength)
				for (index <- arr.indices) {
					arr(index) = valueBuff.getDouble(index << 3)
				}
				if (pullRangeEnable)
					arr.slice(pullRangeStart, pullRangeEnd).asInstanceOf[V]
				else
					arr.asInstanceOf[V]
			}
			case _ => throw new IllegalArgumentException("data type error!")
		}
	}

	//pull weights from hz,return data type is your ps's setting
	def pull(featureId: util.HashSet[Long], tableName: String = ps.psConf.getPsTableName()): util.Map[Long, V] = {
		val execService = this.client.getExecutorService(tableName)
		val initCapacity: Int = (featureId.size() / 0.75f + 1).toInt
		val resMap = new util.HashMap[Long, Array[Byte]](initCapacity)
		pullTask.setTableName(tableName)
		featureId.groupBy(x => this.partService.getPartition(x).getOwner).map { memberSet =>
			pullTask.setPullSet(memberSet._2)
			execService.submitToMember(pullTask, memberSet._1)
		}.foreach(futureMap => resMap.putAll(futureMap.get()))
		resMap.map {
			case (k, bytes) =>
				(k, getRangeVFromBytes(bytes))
		}
	}

	//push weights to ps by push push-function
	def push(weightMap: util.Map[Long, V], tableName: String = ps.psConf.getPsTableName()): Unit = {
		val execService = this.client.getExecutorService(tableName)
		pushTask.setTableName(tableName)
		if (this.hasUpdater) {
			pushTask.setUpdater(this.updater)
		}
		weightMap.groupBy(x => this.partService.getPartition(x).getOwner).map { memberMap =>
			pushTask.setPushMap(memberMap._2)
			execService.submitToMember(pushTask, memberMap._1)
		}
	}
}
