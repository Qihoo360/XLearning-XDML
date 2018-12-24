package net.qihoo.xitong.xdml.updater
import java.util
import scala.collection.JavaConversions._

class FFMUpdater extends Updater[Long,Array[Float]]{

	private var learningRate = 0.01F

	override def update(originalWeightMap: util.Map[Long, Array[Float]], gradientMap: util.Map[Long, Array[Float]]): util.Map[Long, Array[Float]] = {
		val size = gradientMap.head._2.length
		val updateMap = originalWeightMap.map{
			case(k, v) => for(index <- 0 until size){
					v(index) = v(index) - learningRate * gradientMap(k)(index)
			}
				(k,v)
		}
		updateMap
	}

	//set learning rate
	def setLearningRate(lr:Float):this.type ={
		this.learningRate = lr
		this
	}
}
