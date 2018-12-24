package net.qihoo.xitong.xdml.model.loss

import net.qihoo.xitong.xdml.model.linalg.BLAS
import org.apache.spark.mllib.linalg.Vector


class L2LossFunc extends LossFunc {

  //////////////////////////  without intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val margin = BLAS.dot(weights, data)
    math.pow(margin - label, 2) / 2
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val margin = BLAS.dot(weights, data)
    margin - label
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val margin = BLAS.dot(weights, data)
    (margin - label, math.pow(margin - label, 2) / 2)
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    dot - label
  }

  //////////////////////////  with intercept  //////////////////////////////////

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
    val margin = BLAS.dot(weights, data) + intercept
    math.pow(margin - label, 2) / 2
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
    val margin = BLAS.dot(weights, data) + intercept
    margin - label
  }

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector,
                                intercept: Double): (Double, Double) = {
    val margin = BLAS.dot(weights, data) + intercept
    (margin - label, math.pow(margin - label, 2) / 2)
  }

}

