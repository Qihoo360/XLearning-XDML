package net.qihoo.xitong.xdml.mapstore

import java.nio.ByteBuffer
import java.{lang, util}

import com.hazelcast.core.MapStore
import net.qihoo.xitong.xdml.conf.PSDataType.PSDataType
import net.qihoo.xitong.xdml.conf.{PSConfiguration, PSDataType}
import org.apache.kudu.Schema
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._

import scala.collection.JavaConversions._
import scala.util.Random

class PSMapStore(psConf: PSConfiguration, tableName: String) extends MapStore[Long, Array[Byte]] {
	var client: KuduClient = _
	var table: KuduTable = _
	var session: KuduSession = _
	var schema: Schema = _
	//pull num count
	var count: Long = 0L
	var maxHzCacheSizePerPartition: Long = _
	//random number generator
	var random: Random = _
	var kuduCols: util.ArrayList[String] = _
	var scanBuilder: KuduScannerBuilder = _
	//V's bytes array size
	var byteArraySize: Int = _
	//initial value when pull count < cache size or kudu table doesn't have the key
	var initValue: ByteBuffer = _
	//ps conf data type
	var valueDataType: PSDataType = _
	var valueDataLength: Int = _
	// whether need random initial values
	var needRandInit: Boolean = _
	var randomMin:Int = _
	var randomMax:Int = _
	var kuduForceSparse:Boolean = _

	init()
	//initialize all variables and init value
	def init(): Unit = {
		client = new KuduClient.KuduClientBuilder(psConf.getKuduMaster).build()
		session = client.newSession()
		table = client.openTable(tableName)
		scanBuilder = client.newScannerBuilder(table)
		schema = table.getSchema
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
		session.setIgnoreAllDuplicateRows(false)
		session.setMutationBufferSpace(5000)
		kuduCols = new util.ArrayList[String]
		kuduCols.add(psConf.getKuduKeyColName)
		kuduCols.add(psConf.getKuduValueColName)
		random = new Random()
		valueDataType = psConf.getPsDataType
		valueDataLength = psConf.getPsDataLength
		byteArraySize = PSDataType.sizeOf(valueDataType) * valueDataLength
		println("kudu bytes size: " + byteArraySize)
		needRandInit = psConf.getKuduRandomInit
		randomMin = psConf.getKuduRandomMin
		randomMax = psConf.getKuduRandomMax
		initValue = ByteBuffer.allocate(byteArraySize)
		maxHzCacheSizePerPartition = psConf.getHzMaxCacheSizePerPartition
		kuduForceSparse = psConf.getForceSparse
		//need random initial value
		if (needRandInit) {
			valueDataType match {
				case PSDataType.FLOAT => initValue.putFloat(random.nextFloat() * (randomMax - randomMin) + randomMin)
				case PSDataType.DOUBLE => initValue.putDouble(random.nextDouble() * (randomMax - randomMin) + randomMin)
				case PSDataType.FLOAT_ARRAY => {
					for (index <- 0 until valueDataLength) {
						initValue.putFloat(index << 2,random.nextFloat()*(randomMax - randomMin) + randomMin)
					}
				}
				case PSDataType.DOUBLE_ARRAY => {
					for (index <- 0 until valueDataLength) {
						initValue.putDouble(index << 3,random.nextDouble()*(randomMax - randomMin) + randomMin)
					}
				}
			}
		}
	}

	override def store(key: Long, value: Array[Byte]): Unit = {
		val op = table.newUpsert()
		op.getRow.addLong(0, key)
		op.getRow.addBinary(1, value)
		session.apply(op)
	}

	override def storeAll(map: util.Map[Long, Array[Byte]]): Unit = {
		for ((k: Long, v: Array[Byte]) <- map if v.length == byteArraySize) {
				this.store(k, v)
		}
	}

	override def load(key: Long): Array[Byte] = {
		if (count < (maxHzCacheSizePerPartition - 2)) {
			count += 1
		} else {
			val idsPredicate = KuduPredicate.newComparisonPredicate(schema.getColumn(psConf.getKuduKeyColName), KuduPredicate.ComparisonOp.EQUAL, key)
			val scanner = scanBuilder.setProjectedColumnNames(kuduCols).addPredicate(idsPredicate).build()
			while (scanner.hasMoreRows) {
				val results = scanner.nextRows()
				while (results.hasNext) {
					val value = results.next().getBinaryCopy(1)
					if (value.length == byteArraySize) {
						return value
					} else {
						return initValue.array()
					}
				}
			}
			scanner.close()
		}
		initValue.array()
	}

	override def loadAll(keys: util.Collection[Long]): util.Map[Long, Array[Byte]] = {
		val initCapacity: Int = (keys.size() / 0.75f + 1).toInt
		val loadResultMap = new util.HashMap[Long, Array[Byte]](initCapacity)
		if (count < (maxHzCacheSizePerPartition - 2)) {
			for (key <- keys)
				loadResultMap.put(key, initValue.array())
			count += keys.size()
		} else {
			val idsPredicate = KuduPredicate.newInListPredicate(schema.getColumn(psConf.getKuduKeyColName), keys.toList)
			val scanner = client.newScannerBuilder(table).setProjectedColumnNames(kuduCols).addPredicate(idsPredicate).build()
			while (scanner.hasMoreRows) {
				val results = scanner.nextRows()
				while (results.hasNext) {
					val result = results.next()
					val id = result.getLong(0)
					val weight = result.getBinaryCopy(1)
					if (weight.length == byteArraySize) {
						loadResultMap.put(id, weight)
					} else {
						loadResultMap.put(id, initValue.array())
					}
				}
			}
			scanner.close()
		}
		loadResultMap
	}

	override def loadAllKeys(): lang.Iterable[Long] = {null}

	override def delete(key: Long): Unit = {}

	override def deleteAll(keys: util.Collection[Long]): Unit = {}
}
