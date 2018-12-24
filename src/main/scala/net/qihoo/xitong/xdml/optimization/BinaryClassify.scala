package net.qihoo.xitong.xdml.optimization

import breeze.numerics.sigmoid
import net.qihoo.xitong.xdml.linalg.BLAS._

object BinaryClassify extends Optimizer {

    def train(label: Double,
              feature: (Array[Long], Array[Float]),
              localW: Map[Long, Float],
              subsampling_rate: Double = 1.0,
              subsampling_label: Double = 0.0
             ): (Map[Long, Float], Double) = {

        val (pre, loss) = computePreAndLoss(label, feature, localW)
        val newGradient = getGradLoss(feature, label, pre, subsampling_rate, subsampling_label)
        (newGradient, loss)
    }

    def predict(feature: (Array[Long], Array[Float]),
                weight: Map[Long, Float]
               ): Double = {
        sigmoid(dot(feature, weight))
    }

    def getGradLoss(feature: (Array[Long], Array[Float]),
                    label: Double,
                    pre: Float,
                    subsampling_rate: Double = 1.0,
                    subsampling_label: Double = 0.0): Map[Long, Float] = {
        val p = sigmoid(pre)
        var w = 1.0
        if (Math.abs(label - subsampling_label) < 0.001) {
            w = 1 / subsampling_rate
        }
        var gradLossMap: Map[Long, Float] = Map()
        (0 until feature._1.size).foreach(id => gradLossMap += (feature._1(id) -> ((p - label) * feature._2(id) * w).toFloat))
        gradLossMap
    }

    def computePreAndLoss(label: Double,
                          feature: (Array[Long], Array[Float]),
                          weight: Map[Long, Float]
                         ): (Float, Double) = {
        val dotVal = dot(feature, weight)
        val loss = if (label > 0) {
            if (dotVal > 13)
                2e-6
            else if (dotVal < -13)
                (-dotVal)
            else
                math.log1p(math.exp(-dotVal))
        } else {
            if (dotVal > 13)
                (-dotVal)
            else if (dotVal < -13)
                2e-6
            else
                math.log1p(math.exp(-dotVal)) + dotVal
        }
        (dotVal, loss)
    }

}
