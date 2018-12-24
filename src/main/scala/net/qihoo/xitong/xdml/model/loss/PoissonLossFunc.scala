package net.qihoo.xitong.xdml.model.loss

import net.qihoo.xitong.xdml.model.linalg.BLAS
import org.apache.spark.mllib.linalg.Vector


class PoissonLossFunc extends LossFunc {

  //////////////////////////  without intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val margin = BLAS.dot(weights, data)
    math.exp(margin) - label * margin
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val margin = BLAS.dot(weights, data)
    math.exp(margin) - label
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val margin = BLAS.dot(weights, data)
    val expMargin = math.exp(margin)
    (expMargin - label, expMargin - label * margin)
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    math.exp(dot) - label
  }

  //////////////////////////  with intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
    val margin = BLAS.dot(weights, data) + intercept
    math.exp(margin) - label * margin
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
    val margin = BLAS.dot(weights, data) + intercept
    math.exp(margin) - label
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector,
                                intercept: Double): (Double, Double) = {
    val margin = BLAS.dot(weights, data) + intercept
    val expMargin = math.exp(margin)
    (expMargin - label, expMargin - label * margin)
  }

}


