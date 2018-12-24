package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector
import net.qihoo.xitong.xdml.model.util.MLUtils
import net.qihoo.xitong.xdml.model.linalg.BLAS


class LogitLossFunc extends LossFunc {

  def sigmoid(z: Double): Double = {
    1.0 / (1.0 + math.exp(-z))
  }

  //////////////////////////  without intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val margin = - BLAS.dot(weights, data)
    if (label > 0.5) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      MLUtils.log1pExp(margin)
    } else {
      MLUtils.log1pExp(margin) - margin
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val margin = - BLAS.dot(weights, data)
    val factor = (1.0 / (1.0 + math.exp(margin))) - label
    factor
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val margin = - BLAS.dot(weights, data)
    val factor = (1.0 / (1.0 + math.exp(margin))) - label
    val loss = if (label > 0.5) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      MLUtils.log1pExp(margin)
    } else {
      MLUtils.log1pExp(margin) - margin
    }
    (factor, loss)
  }

  override def gradientFromDot(dot: Double,
                                label: Double): Double = {
    sigmoid(dot) - label
  }

  //////////////////////////  with intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
//    val margin = - BLAS.dot(weights, data)
    val margin = - BLAS.dot(weights, data) - intercept
    if (label > 0.5) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      MLUtils.log1pExp(margin)
    } else {
      MLUtils.log1pExp(margin) - margin
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
//    val margin = - BLAS.dot(weights, data)
    val margin = - BLAS.dot(weights, data) - intercept
    val factor = (1.0 / (1.0 + math.exp(margin))) - label
    factor
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector,
                                intercept: Double): (Double, Double) = {
//    val margin = - BLAS.dot(weights, data)
    val margin = - BLAS.dot(weights, data) - intercept
    val factor = (1.0 / (1.0 + math.exp(margin))) - label
    val loss = if (label > 0.5) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      MLUtils.log1pExp(margin)
    } else {
      MLUtils.log1pExp(margin) - margin
    }
    (factor, loss)
  }

}

