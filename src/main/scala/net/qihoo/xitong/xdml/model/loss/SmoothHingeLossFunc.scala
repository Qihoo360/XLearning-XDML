package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector
import net.qihoo.xitong.xdml.model.linalg.BLAS


class SmoothHingeLossFunc extends LossFunc {

  //////////////////////////  without intercept  //////////////////////////////////

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      (- labelScaled, max - 0.5)
    } else {
      (- labelScaled * max, max * max / 2)
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      - labelScaled
    } else {
      - labelScaled * max
    }
  }

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      max - 0.5
    } else {
      max * max / 2
    }
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dot)
    if (labelScaled * dot < 0) {
      - labelScaled
    } else {
      - labelScaled * max
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
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      (- labelScaled, max - 0.5)
    } else {
      (- labelScaled * max, max * max / 2)
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector,
                        intercept: Double): Double = {
    //    val dotProduct = BLAS.dot(weights, data)
    val dotProduct = BLAS.dot(weights, data) + intercept
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      - labelScaled
    } else {
      - labelScaled * max
    }
  }

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector,
                    intercept: Double): Double = {
    //    val dotProduct = BLAS.dot(weights, data)
    val dotProduct = BLAS.dot(weights, data) + intercept
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      max - 0.5
    } else {
      max * max / 2
    }
  }

}

