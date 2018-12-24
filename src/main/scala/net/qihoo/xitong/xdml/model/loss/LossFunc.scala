package net.qihoo.xitong.xdml.model.loss

import org.apache.spark.mllib.linalg.Vector


abstract class LossFunc extends Serializable {

  //////////////////////////  without intercept  //////////////////////////////////

  def gradient(data: Vector, label: Double, weights: Vector): Double

  def loss(data: Vector, label: Double, weights: Vector): Double

  def gradientWithLoss(data: Vector, label: Double, weights: Vector): (Double, Double)

  def gradientFromDot(dot: Double, label: Double): Double

  //////////////////////////  with intercept  //////////////////////////////////

  def gradient(data: Vector, label: Double, weights: Vector, intercept: Double): Double

  def loss(data: Vector, label: Double, weights: Vector, intercept: Double): Double

  def gradientWithLoss(data: Vector, label: Double, weights: Vector, intercept: Double): (Double, Double)

}

