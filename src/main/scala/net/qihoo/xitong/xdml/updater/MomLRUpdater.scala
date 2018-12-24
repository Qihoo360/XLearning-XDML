package net.qihoo.xitong.xdml.updater

import java.util
import scala.collection.JavaConversions._

class MomLRUpdater extends Updater[Long, Array[Float]] {
	private var coff = 0.1F
	private var learningRate = 0.01F

	override def update(originalWeightMap: util.Map[Long, Array[Float]], gradientMap: util.Map[Long, Array[Float]]): util.Map[Long, Array[Float]] = {
		val updateMap = originalWeightMap.map { case (k, v) =>
			val delta = -learningRate * gradientMap(k)(0) + coff * v(1)
			v(0) = v(0) + delta
			v(1) = delta
//			println("update map: V0: " + v(0) + "----> V1: " + v(1))
			(k, v)
		}
		updateMap
	}

	//set learning rate
	def setLearningRate(lr:Float):this.type ={
		this.learningRate = lr
		this
	}

	def setCoff(coff:Float):this.type ={
		this.coff = coff
		this
	}
}
