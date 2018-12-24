package net.qihoo.xitong.xdml.model.loss

import net.qihoo.xitong.xdml.model.linalg.BLAS
import org.apache.spark.mllib.linalg.Vector


class WeightedSmoothHingeLossFunc(posWeight: Double) extends LossFunc {

  //////////////////////////  without intercept  //////////////////////////////////

  override def gradientWithLoss(data: Vector,
                                label: Double,
                                weights: Vector): (Double, Double) = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      (- labelScaled * (label * (posWeight - 1) + 1), (max - 0.5) * (label * (posWeight - 1) + 1))
    } else {
      (- labelScaled * max * (label * (posWeight - 1) + 1), max * max / 2 * (label * (posWeight - 1) + 1))
    }
  }

  override def gradient(data: Vector,
                        label: Double,
                        weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      - labelScaled * max * (label * (posWeight - 1) + 1)
    }
  }

  override def loss(data: Vector,
                    label: Double,
                    weights: Vector): Double = {
    val dotProduct = BLAS.dot(weights, data)
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dotProduct)
    if (labelScaled * dotProduct < 0) {
      (max - 0.5) * (label * (posWeight - 1) + 1)
    } else {
      max * max / 2 * (label * (posWeight - 1) + 1)
    }
  }

  override def gradientFromDot(dot: Double,
                               label: Double): Double = {
    val labelScaled = 2 * label - 1.0
    val max = math.max(0.0, 1.0 - labelScaled * dot)
    if (labelScaled * dot < 0) {
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      - labelScaled * max * (label * (posWeight - 1) + 1)
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
      (- labelScaled * (label * (posWeight - 1) + 1), (max - 0.5) * (label * (posWeight - 1) + 1))
    } else {
      (- labelScaled * max * (label * (posWeight - 1) + 1), max * max / 2 * (label * (posWeight - 1) + 1))
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
      - labelScaled * (label * (posWeight - 1) + 1)
    } else {
      - labelScaled * max * (label * (posWeight - 1) + 1)
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
      (max - 0.5) * (label * (posWeight - 1) + 1)
    } else {
      max * max / 2 * (label * (posWeight - 1) + 1)
    }
  }

}



