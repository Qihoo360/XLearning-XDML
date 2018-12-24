package net.qihoo.xitong.xdml.updater

import scala.collection.JavaConversions._

class LRUpdater extends Updater[Long, Float]{

	private var learningRate:Float = 0.01F
	//normal lr push function to deal with last weights and gradients
	override def update(lastWeightMap:java.util.Map[Long,Float], gradientMap:java.util.Map[Long,Float]): java.util.Map[Long,Float] = {
		val updateMap = lastWeightMap.map { case (k, v) =>
			val delta = -learningRate * gradientMap(k)
			(k, v + delta)
		}
		updateMap
	}
	//set learning rate
	def setLearningRate(lr:Float):this.type ={
		this.learningRate = lr
		this
	}
}
