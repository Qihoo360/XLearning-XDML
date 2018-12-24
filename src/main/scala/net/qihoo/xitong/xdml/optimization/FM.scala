package net.qihoo.xitong.xdml.optimization

import net.qihoo.xitong.xdml.linalg.BLAS._


object FM extends Optimizer {
  def train(label: Double,
            feature: (Array[Long], Array[Float]),
            W0: Float,
            W: Map[Long, Float],
            V: Map[Long, Array[Float]],
            rank: Int,
            subsampling_rate: Double = 1.0,
            subsampling_label: Double = 0.0): (Map[Long, Array[Float]], Double) = {
    //pred是预测值y，先加上常数项和一次项
    var pred = W0 + dot(feature, W)
    val array = Array.fill(rank)(0.0f)
    val sumArray = Array.fill(rank)(0.0f)
    // res1 = v_i,f * x_i
    // res2 = (v_i,f * x_i)^2
    (0 until rank).foreach { f =>
      var (res1, res2) = (0f, 0f)
      feature._1.indices.foreach { id =>
        val fId = feature._1(id)
        val fValue = feature._2(id)
        val tmp = fValue * V.getOrElse(fId, array)(f)
        res1 += tmp
        res2 += tmp * tmp
      }
      sumArray(f) = res1
      pred += 0.5f * (res1 * res1 - res2)
    }

    /*
    val loss = if (label > 0) {
      if (pred > 13)
        2e-6
      else if (pred < -13)
        -pred
      else
        math.log1p(math.exp(-pred))
    } else {
      if (pred > 13)
        -pred
      else if (pred < -13)
        2e-6
      else
        math.log1p(math.exp(-pred)) + pred
    }
    */
    val z = label * pred
    //计算loss
    var loss = 0.0
    if (z > 18)
      loss = math.exp(-z)
    else if (z < -18)
      loss = -z
    else
      loss = math.log1p(math.exp(-z))

    val p = deltaGrad(label, pred).toFloat
    var w = 1.0f
    if (math.abs(label - subsampling_label) < 0.001)
      w = (1 / subsampling_rate).toFloat

    var gradLossMap: Map[Long, Array[Float]] = Map()
    val wArray = Array.fill(rank + 1)(0.0f)
    wArray(0) = p * w
    gradLossMap += (0L -> wArray)
    feature._1.indices.foreach { id =>
      val fId = feature._1(id)
      val fValue = feature._2(id)
      val wArray = Array.fill(rank + 1)(0.0f)
      if (fId != 0L) {
        wArray(0) = fValue * p * w
        (0 until rank).foreach { f =>
          val vGrad = fValue * sumArray(f) - V.getOrElse(fId, array)(f) * fValue * fValue
          wArray(f + 1) = vGrad * p * w
        }
        gradLossMap += (fId -> wArray)
      }
    }
    (gradLossMap, loss)
  }

  def predict(feature: (Array[Long], Array[Float]),
              W0: Float,
              W: Map[Long, Float],
              V: Map[Long, Array[Float]],
              rank: Int): Double = {
    var pred = W0 + dot(feature, W)
    val array = Array.fill(rank)(0.0F)
    for (f <- 0 until rank) {
      var (res1, res2) = (0f, 0f)
      feature._1.indices.foreach { id =>
        val tmp = feature._2(id) * V.getOrElse(feature._1(id), array)(f)
        res1 += tmp
        res2 += tmp * tmp
      }
      pred += 0.5f * (res1 * res1 - res2)
    }
    sigmoid(pred)
  }

  // - exp(-label * pred) / (1 + exp(-label * pred)) * label
  def deltaGrad(label: Double,
                pred: Float): Double = {
    val sigma = label * pred
    if (sigma > 18)
      -label * math.exp(-sigma)
    else if (sigma < -18)
      -label
    else {
      val ex = Math.pow(2.718281828, sigma)
      -label / (1 + ex)
    }
  }

}