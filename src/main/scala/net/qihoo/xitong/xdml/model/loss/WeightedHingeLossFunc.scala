package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector
import net.qihoo.xitong.xdml.model.linalg.BLAS


class WeightedHingeLossFunc(posWeight: Double) extends LossFunc {

  //////////////////////////  without intercept  //////////////////////////////////

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      (- labelScaled * (label * (posWeight - 1) + 1), (1.0 - labelScaled * dotProduct) * (label * (posWeight - 1) + 1))
    } else {
      (0.0, 0.0)
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      0.0
    }
  }

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      (1.0 - labelScaled * dotProduct) * (label * (posWeight - 1) + 1)
    } else {
      0.0
    }
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dot) {
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      0.0
    }
  }

  //////////////////////////  with intercept  //////////////////////////////////

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector,
                                intercept: Double): (Double, Double) = {
    //    val dotProduct = BLAS.dot(weights, data)
    val dotProduct = BLAS.dot(weights, data) + intercept
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      (- labelScaled * (label * (posWeight - 1) + 1), (1.0 - labelScaled * dotProduct) * (label * (posWeight - 1) + 1))
    } else {
      (0.0, 0.0)
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
    //    val dotProduct = BLAS.dot(weights, data)
    val dotProduct = BLAS.dot(weights, data) + intercept
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      0.0
    }
  }

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
    //    val dotProduct = BLAS.dot(weights, data)
    val dotProduct = BLAS.dot(weights, data) + intercept
    val labelScaled = 2 * label - 1.0
    if (1.0 > labelScaled * dotProduct) {
      (1.0 - labelScaled * dotProduct) * (label * (posWeight - 1) + 1)
    } else {
      0.0
    }
  }

}