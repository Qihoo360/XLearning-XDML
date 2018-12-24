package net.qihoo.xitong.xdml.updater

import java.util
import scala.collection.JavaConversions._

class LRFTRLUpdater extends Updater[Long, Array[Float]] {
  private var alpha = 1F
  private var beta = 1F
  private var lambda1 = 1F
  private var lambda2 = 1F

  override def update(originalWeightMap: util.Map[Long, Array[Float]], gradientMap: util.Map[Long, Array[Float]]): util.Map[Long, Array[Float]] = {
    val updateMap = originalWeightMap.map {
      case (k, v) => {
        val grad = gradientMap(k)(0)
        val pValue = 1f / alpha * (Math.sqrt(v(1) + grad * grad) - Math.sqrt(v(1)))
        v(0) = (v(0) + grad - pValue * v(2)).toFloat
        v(1) = v(1) + grad * grad
        if (Math.abs(v(0)) > lambda1)
          v(2) = ((-1) * (1.0 / (lambda2 + (beta + Math.sqrt(v(1))) / alpha)) * (v(0) - Math.signum(v(0)).toInt * lambda1)).toFloat
        else
          v(2) = 0F

        (k, v)
      }
    }
    updateMap
  }

  def setAlpha(alpha: Float): this.type = {
    this.alpha = alpha
    this
  }

  def setBeta(beta: Float): this.type = {
    this.beta = beta
    this
  }

  def setLambda1(lambda1: Float): this.type = {
    this.lambda1 = lambda1
    this
  }

  def setLambda2(lambda2: Float): this.type = {
    this.lambda2 = lambda2
    this
  }
}
