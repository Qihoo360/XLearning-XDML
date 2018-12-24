package net.qihoo.xitong.xdml.linalg

import scala.collection.mutable

object BLAS extends Serializable {

  /**
    * dot(x, y)
    */

  def dot(x: Map[Long, Float], y: Map[Long, Float]): Float = {
    var result = 0f
    x.keys.foreach(k => result += x(k) * y.getOrElse(k, 0f))
    result
  }

  def dot(x: (Array[Long], Array[Float]), y: Map[Long, Float]): Float = {
    var result = 0f
    (0 until x._1.size).foreach(id => result += x._2(id) * y.getOrElse(x._1(id), 0f))
    result
  }

  def axpy(a: Double, x: (Array[Long], Array[Float]), y: mutable.Map[Long, Float]): Unit = {
    val xIndices = x._1
    val xValues = x._2
    val nnz = xIndices.length
    if (Math.abs(a - 1) < 1e-6) {
      var k = 0
      while (k < nnz) {
        if (y.contains(xIndices(k))) {
          y(xIndices(k)) += xValues(k)
        } else {
          y += (xIndices(k) -> xValues(k))
        }
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        if (y.contains(xIndices(k))) {
          y(xIndices(k)) += (a * xValues(k)).toFloat
        } else {
          y += (xIndices(k) -> (a * xValues(k)).toFloat)
        }
        k += 1
      }
    }
  }


  def sigmoid(value: Float): Double = {
    if (value > 30)
      1f
    else if (value < (-30))
      1e-6
    else {
      val ex = Math.pow(2.718281828, value)
      (ex / (1.0 + ex))
    }
  }

}
