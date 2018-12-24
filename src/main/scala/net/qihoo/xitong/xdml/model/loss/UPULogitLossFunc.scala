package net.qihoo.xitong.xdml.model.loss

import net.qihoo.xitong.xdml.model.linalg.BLAS
import net.qihoo.xitong.xdml.model.util.MLUtils
import org.apache.spark.mllib.linalg.Vector


class UPULogitLossFunc(posRatioPrior: Double, posRatio: Double, unlabeledRatio: Double) extends LossFunc {

  val posRatioPriorFrac = - posRatioPrior / posRatio
  println("unlabeledRatio: "+unlabeledRatio)
  println("posRatio: "+posRatio)
  println("posRatioPrior: "+posRatioPrior)

  //////////////////////////  without intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val margin = BLAS.dot(weights, data)
    if (label > 0.5) {
      posRatioPriorFrac * margin
    } else {
      MLUtils.log1pExp(margin) / unlabeledRatio
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    if (label > 0.5) {
      posRatioPriorFrac
    } else {
      val margin = BLAS.dot(weights, data)
      1.0 / (1.0 + math.exp(-margin)) / unlabeledRatio
    }
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val margin = BLAS.dot(weights, data)
    if (label > 0.5) {
      (posRatioPriorFrac, posRatioPriorFrac * margin)
    } else {
      (1.0 / (1.0 + math.exp(-margin)) / unlabeledRatio, MLUtils.log1pExp(margin) / unlabeledRatio)
    }
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    if (label > 0.5) {
      posRatioPriorFrac
    } else {
      1.0 / (1.0 + math.exp(-dot)) / unlabeledRatio
    }
  }

  //////////////////////////  with intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
    val margin = BLAS.dot(weights, data) + intercept
    if (label > 0.5) {
      posRatioPriorFrac * margin
    } else {
      MLUtils.log1pExp(margin) / unlabeledRatio
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
    if (label > 0.5) {
      posRatioPriorFrac
    } else {
      val margin = BLAS.dot(weights, data) + intercept
      1.0 / (1.0 + math.exp(-margin)) / unlabeledRatio
    }
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector,
                                intercept: Double): (Double, Double) = {
    val margin = BLAS.dot(weights, data) + intercept
    if (label > 0.5) {
      (posRatioPriorFrac, posRatioPriorFrac * margin)
    } else {
      (1.0 / (1.0 + math.exp(-margin)) / unlabeledRatio, MLUtils.log1pExp(margin) / unlabeledRatio)
    }
  }

}





