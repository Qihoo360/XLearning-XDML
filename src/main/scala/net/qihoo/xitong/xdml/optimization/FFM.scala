package net.qihoo.xitong.xdml.optimization

import scala.collection.mutable

object FFM extends Optimizer {
	val lamda = 0.1F
	val lr = 0.1F

	def train(fieldNum: Int,
			  rank: Int,
			  label: Double,
			  feature: (Array[Int], Array[Long], Array[Float]),
			  VMap: mutable.Map[Long, Array[Float]]): (mutable.Map[Long, Array[Float]], Double) = {
		calcGradAndLoss(fieldNum, rank, label, feature, VMap)
	}

	def predict(): (Double,Double) ={
		(0.0,0.0)
	}

	//calculate dot
	def vectorDotWithField(rank: Int,
						   vector1: Array[Float],
						   field1: Int,
						   vector2: Array[Float],
						   field2: Int): Float = {
		var res = 0.0F
		val idx1 = rank * field1
		val idx2 = rank * field2
		for (i <- 0 until rank) {
			res = res + vector1(idx1 + i) * vector2(idx2 + i)
		}
		println("vec1: " + vector1.mkString(","))
		println("vec2: " + vector2.mkString(","))
		println("dot res: " + res)
		res
	}

	//calculate phi
	def calcPhi(fieldNum: Int,
				rank: Int,
				oneData: (Array[Int], Array[Long], Array[Float]),
				VMap: mutable.Map[Long, Array[Float]]): Float = {
		val size = oneData._1.length
		val field = oneData._1
		val id = oneData._2
		val value = oneData._3
		var resPhi = 0.0F
		val zeros = Array.fill(rank * fieldNum)(0.01f)
		for (i <- 0 until size) {
			for (j <- i + 1 until size) {
				val w1 = VMap.getOrElse(id(i), zeros)
				val w2 = VMap.getOrElse(id(j), zeros)
				println("w1: " + w1.mkString(",") + "       w2: " + w2.mkString(","))
				resPhi = resPhi + vectorDotWithField(rank, w1, field(i), w2, field(j)) * value(i) * value(j)
			}
		}
		println("phi: " + resPhi)
		resPhi
	}

	//calculate grad and loss
	def calcGradAndLoss(fieldNum: Int,
						rank: Int,
						label: Double,
						oneData: (Array[Int], Array[Long], Array[Float]),
						weightMap: mutable.Map[Long, Array[Float]]): (mutable.Map[Long, Array[Float]], Double) = {
		val pred = calcPhi(fieldNum, rank, oneData, weightMap)
		val loss = math.log1p(math.exp(-pred * label))
		println("pred: " + pred + "    label: " + label)
		val size = oneData._1.length
		val field = oneData._1
		val id = oneData._2
		val value = oneData._3
		val gradMap = new mutable.HashMap[Long, Array[Float]]
		val zeros = Array.fill(rank * fieldNum + fieldNum)(0.01f)
		val k = -label / math.log1p(label * pred)
		for (i <- 0 until size) {
			for (j <- i + 1 until size) {
				val w1 = weightMap.getOrElse(id(i), zeros)
				val w2 = weightMap.getOrElse(id(j), zeros)
				val (id1, id2, f1, f2, v1, v2) = (id(i), id(j), field(i), field(j), value(i), value(j))
				val grad = calcGrad(fieldNum, rank, w1, f1, v1, w2, f2, v2, k)
				val start1 = f2 * rank
				val start2 = f1 * rank
				for (index <- 0 until rank) {
					w1(start1 + index) = grad._1(index)
					w2(start2 + index) = grad._2(index)
				}
				gradMap.put(id1, w1)
				gradMap.put(id2, w2)
			}
		}
		(gradMap, loss)
	}

	//calculate grad
	def calcGrad(fieldNum: Int,
				 rank: Int,
				 w1: Array[Float],
				 w1Field: Int,
				 x1: Float,
				 w2: Array[Float],
				 w2Field: Int,
				 x2: Float,
				 k: Double): (Array[Float], Array[Float]) = {
		val w1f2Grad = Array.fill(rank)(0.01f)
		val w2f1Grad = Array.fill(rank)(0.01f)
		val v1f2Start = rank * w2Field
		val v2f1Start = rank * w1Field
		for (i <- 0 until rank) {
			w1f2Grad(i) = lamda * w1(v1f2Start + i) + k.toFloat * w2(v2f1Start + i) * x1 * x2
			w2f1Grad(i) = lamda * w2(v2f1Start + i) + k.toFloat * w1(v1f2Start + i) * x1 * x2
		}
		println("grad1: " + w1f2Grad.mkString(","))
		println("grad2: " + w2f1Grad.mkString(","))
		(w1f2Grad, w2f1Grad)
	}
	//update map
	def updateGradMap(fieldNum: Int,
					  rank: Int,
					  targetGradMap: mutable.Map[Long, Array[Float]],
					  grad: (Array[Float], Array[Float]),
					  field1: Int,
					  field2: Int,
					  id1: Long,
					  id2: Long): Unit = {
		val zeros = Array.fill(rank)(0.0f)
		val gradId1 = targetGradMap.getOrElse(id1, zeros)
		val gradId2 = targetGradMap.getOrElse(id2, zeros)
		val (w1f2Grad, w2f1Grad) = (grad._1, grad._2)
		val (start1, start2) = (field2 * rank, field1 * rank)
		val (lastGradAcc1, lastGradAcc2) = (w1f2Grad(rank * fieldNum - 1 + field1), w2f1Grad(rank * fieldNum - 1 + field2))
		val lr1 = -1 / math.sqrt(1 + lastGradAcc1).toFloat
		val lr2 = -1 / math.sqrt(1 + lastGradAcc2).toFloat
		var grad2norm1 = 0.0F
		var grad2norm2 = 0.0F
		for (i <- 0 until rank) {
			gradId1(start1 + i) = gradId1(start1 + i) + lr1 * w1f2Grad(i)
			gradId2(start2 + i) = gradId2(start2 + i) + lr2 * w2f1Grad(i)
			grad2norm1 += math.pow(w1f2Grad(i), 2).toFloat
			grad2norm2 += math.pow(w2f1Grad(i), 2).toFloat

		}
		gradId1(rank * fieldNum - 1 + field1) = grad2norm1 + lastGradAcc1
		gradId2(rank * fieldNum - 1 + field2) = grad2norm2 + lastGradAcc2
		targetGradMap.put(id1, gradId1)
		targetGradMap.put(id2, gradId2)
	}

	//calc vec2
	def calc2norm(fieldNum: Int, rank: Int, vector: Array[Float], field: Int): Float = {
		var res = 0.0F
		val start = rank * field
		for (i <- 0 until rank) {
			res += math.pow(vector(start + i), 2).toFloat
		}
		res
	}
}
