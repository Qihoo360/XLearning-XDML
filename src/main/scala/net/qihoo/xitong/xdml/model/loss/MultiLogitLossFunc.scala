package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector


class MultiLogitLossFunc(numClasses: Int) extends MultiLossFunc {

  def gradientFromMargins(margins: Array[Double], label: Double): Array[Double] = {
    // marginY is margins(label - 1) in the formula.
    var marginY = 0.0
    var maxMargin = Double.NegativeInfinity
    var maxMarginIndex = 0
    for(i <- 0 until (numClasses-1)) {
      if (i == label.toInt - 1) marginY = margins(i)
      if (margins(i) > maxMargin) {
        maxMargin = margins(i)
        maxMarginIndex = i
      }
    }

    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until numClasses - 1) {
          margins(i) -= maxMargin
          if (i == maxMarginIndex) {
            temp += math.exp(-maxMargin)
          } else {
            temp += math.exp(margins(i))
          }
        }
      } else {
        for (i <- 0 until numClasses - 1) {
          temp += math.exp(margins(i))
        }
      }
      temp
    }

    for (i <- 0 until numClasses - 1) {
      margins(i) = math.exp(margins(i)) / (sum + 1.0) - { if (label != 0.0 && label == i + 1) 1.0 else 0.0 }
    }

    margins
  }

  //////////////////////////  with intercept  //////////////////////////////////

  def gradientWithLoss(data: Vector, label: Double,
                       weightsArr: Array[Vector],
                       interceptArr: Array[Double]): (Array[Double], Double) = {
    // marginY is margins(label - 1) in the formula.
    var marginY = 0.0
    var maxMargin = Double.NegativeInfinity
    var maxMarginIndex = 0
    val margins = interceptArr.clone()
    for(i <- 0 until (numClasses-1)) {
      data.foreachActive { (index, value) => if (value != 0.0) margins(i) += value * weightsArr(i)(index) }
      if (i == label.toInt - 1) marginY = margins(i)
      if (margins(i) > maxMargin) {
        maxMargin = margins(i)
        maxMarginIndex = i
      }
    }

    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until numClasses - 1) {
          margins(i) -= maxMargin
          if (i == maxMarginIndex) {
            temp += math.exp(-maxMargin)
          } else {
            temp += math.exp(margins(i))
          }
        }
      } else {
        for (i <- 0 until numClasses - 1) {
          temp += math.exp(margins(i))
        }
      }
      temp
    }

    for (i <- 0 until numClasses - 1) {
      margins(i) = math.exp(margins(i)) / (sum + 1.0) - { if (label != 0.0 && label == i + 1) 1.0 else 0.0 }
    }

    var loss = if (label > 0.0) math.log1p(sum) - marginY else math.log1p(sum)
    if (maxMargin > 0) {
      loss += maxMargin
    }

    (margins, loss)
  }


  //////////////////////////  without intercept  //////////////////////////////////

  def gradientWithLoss(data: Vector, label: Double,
                       weightsArr: Array[Vector]): (Array[Double], Double) = {
    // marginY is margins(label - 1) in the formula.
    var marginY = 0.0
    var maxMargin = Double.NegativeInfinity
    var maxMarginIndex = 0
    val margins = Array.fill(numClasses-1)(0.0)
    for(i <- 0 until (numClasses-1)) {
      data.foreachActive { (index, value) => if (value != 0.0) margins(i) += value * weightsArr(i)(index) }
      if (i == label.toInt - 1) marginY = margins(i)
      if (margins(i) > maxMargin) {
        maxMargin = margins(i)
        maxMarginIndex = i
      }
    }

    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until numClasses - 1) {
          margins(i) -= maxMargin
          if (i == maxMarginIndex) {
            temp += math.exp(-maxMargin)
          } else {
            temp += math.exp(margins(i))
          }
        }
      } else {
        for (i <- 0 until numClasses - 1) {
          temp += math.exp(margins(i))
        }
      }
      temp
    }

    for (i <- 0 until numClasses - 1) {
      margins(i) = math.exp(margins(i)) / (sum + 1.0) - { if (label != 0.0 && label == i + 1) 1.0 else 0.0 }
    }

    var loss = if (label > 0.0) math.log1p(sum) - marginY else math.log1p(sum)
    if (maxMargin > 0) {
      loss += maxMargin
    }

    (margins, loss)
  }



}
