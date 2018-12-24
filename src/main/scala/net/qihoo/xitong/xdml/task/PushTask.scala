package net.qihoo.xitong.xdml.task

import java.nio.ByteBuffer
import java.util.concurrent.Callable

import net.qihoo.xitong.xdml.conf.PSDataType
import net.qihoo.xitong.xdml.conf.PSDataType.PSDataType
import net.qihoo.xitong.xdml.updater.Updater
import net.qihoo.xitong.xdml.utils.XDMLException

import scala.collection.JavaConversions._

class PushTask[K, V] extends Task with Callable[Unit] {
	//PSDataType of V
	private var vClazz: PSDataType = _
	private var pushMap: java.util.Map[Long, V] = _
	private var psDatalength:Int = _
	private var updater:Updater[Long, V] = _
	private var hasUpdater = false
	//V's length
	def setDataLength(length: Int): Unit ={
		this.psDatalength = length
	}

	def setVClazz(dataType:PSDataType) : Unit = {
		this.vClazz = dataType
	}

	def setUpdater(updater:Updater[Long, V]): Unit ={
		this.updater = updater
		this.hasUpdater = true
	}

	def setPushMap(pushMap: java.util.Map[Long, V]): Unit = {
		this.pushMap = pushMap
	}
	//get V from bytes
	def getValueFromBytes(bytes:Array[Byte]): V = {
		val valueBuff = ByteBuffer.wrap(bytes)
		vClazz match{
			case PSDataType.INT => valueBuff.getInt().asInstanceOf[V]
			case PSDataType.LONG => valueBuff.getLong().asInstanceOf[V]
			case PSDataType.FLOAT => valueBuff.getFloat().asInstanceOf[V]
			case PSDataType.DOUBLE => valueBuff.getDouble().asInstanceOf[V]
			case PSDataType.FLOAT_ARRAY => {
				val arr = new Array[Float](psDatalength)
				for(index <- arr.indices){
					arr(index) = valueBuff.getFloat(index << 2)
				}
				arr.asInstanceOf[V]
			}
			case PSDataType.DOUBLE_ARRAY => {
				val arr = new Array[Double](psDatalength)
				for(index <- arr.indices){
					arr(index) = valueBuff.getDouble(index << 3)
				}
				arr.asInstanceOf[V]
			}
			case _ => throw new XDMLException("data type error!")
		}
	}
	//get bytes from V
	def getBytesFromValue(value: V): Array[Byte] = {
		val byteSize = PSDataType.sizeOf(vClazz) * psDatalength
		val byteBuff = ByteBuffer.allocate(byteSize)
		vClazz match {
			case PSDataType.INT => {
				byteBuff.putInt(value.asInstanceOf[Int])
			}
			case PSDataType.LONG => {
				byteBuff.putLong(value.asInstanceOf[Long])
			}
			case PSDataType.FLOAT => {
				byteBuff.putFloat(value.asInstanceOf[Float])
			}
			case PSDataType.DOUBLE => {
				byteBuff.putDouble(value.asInstanceOf[Double])
			}
			case PSDataType.FLOAT_ARRAY => {
				val arr = value.asInstanceOf[Array[Float]]
				arr.indices.map { index =>
					byteBuff.putFloat(index << 2, arr(index))
				}
			}
			case PSDataType.DOUBLE_ARRAY => {
				val arr = value.asInstanceOf[Array[Double]]
				arr.indices.map { index =>
					byteBuff.putDouble(index << 3, arr(index))
				}
			}
			case _ => throw new IllegalArgumentException("data type error!")
		}
		byteBuff.array()
	}
	//push task
	override def call(): Unit = {
		val weightMap = hazelcastIns.getMap[Long, Array[Byte]](tableName)
		val localMap = weightMap.getAll(pushMap.keySet).map {
			case (k, bytes) => (k, getValueFromBytes(bytes))
		}
		if(hasUpdater)
			pushMap = updater.update(localMap,pushMap)
		val bytesMap = pushMap.map {
			case (k, v) => (k, getBytesFromValue(v))
		}
		weightMap.putAll(bytesMap)
	}

}