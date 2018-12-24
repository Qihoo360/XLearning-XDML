package net.qihoo.xitong.xdml.updater
import java.util
import scala.collection.JavaConversions._

class DCASGDUpdater extends Updater[Long,Array[Float]]{
	private var coff = 0.1F
	private var learningRate = 0.01F

	override def update(originalWeightMap: util.Map[Long, Array[Float]], gradientMap: util.Map[Long, Array[Float]]): util.Map[Long, Array[Float]] = {
		val updateMap = originalWeightMap.map { case (k, v) => {
			val grad = gradientMap(k)(0)
			val delta = -learningRate * grad - coff * grad * grad * (v(0) - v(1))
			v(1) = v(0)
			v(0) = v(1) + delta
			(k, v)
		}
		}
		updateMap
	}

	def setLearningRate(lr:Float):this.type ={
		this.learningRate = lr
		this
	}

	def setCoff(coff:Float):this.type ={
		this.coff = coff
		this
	}
}
