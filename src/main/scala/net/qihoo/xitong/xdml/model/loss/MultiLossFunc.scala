package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector


abstract class MultiLossFunc extends Serializable {

  def gradientFromMargins(margins: Array[Double], label: Double): Array[Double]

  //////////////////////////  with intercept  //////////////////////////////////

  def gradientWithLoss(data: Vector, label: Double,
                       weightsArr: Array[Vector],
                       interceptArr: Array[Double]): (Array[Double], Double)

  //////////////////////////  without intercept  //////////////////////////////////

  def gradientWithLoss(data: Vector, label: Double,
                       weightsArr: Array[Vector]): (Array[Double], Double)

}
