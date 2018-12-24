package net.qihoo.xitong.xdml.ml

import java.io.Serializable
import java.util

import net.qihoo.xitong.xdml.optimization.BinaryClassify
import net.qihoo.xitong.xdml.ps.{PS, PSClient}
import net.qihoo.xitong.xdml.updater.LRUpdater
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class LogisticRegression(ps: PS) extends Serializable {
	private var iterNum = 1
	private var batchSize = 50
	private var learningRate: Float = 0.01F

	def fit(data: RDD[(Double, Array[Long], Array[Float])]): (Map[Int, Double], (Long, Long)) = {
		LogisticRegression.runMiniBatchSGD(ps, data, iterNum, batchSize, learningRate)
	}

	def predict(data: RDD[(Double, Array[Long], Array[Float])]): RDD[(Double, Double)] = {
		LogisticRegression.predict(ps, data, batchSize)
	}

	def setIterNum(iterNum: Int): this.type = {
		this.iterNum = iterNum
		this
	}

	def setBatchSize(batchSize: Int): this.type = {
		this.batchSize = batchSize
		this
	}

	def setLearningRate(lr: Float): this.type = {
		this.learningRate = lr
		this
	}
}

object LogisticRegression {

	def runMiniBatchSGD(ps: PS,
						data: RDD[(Double, Array[Long], Array[Float])],
						numIterations: Int,
						batchSize: Int,
						learningRate: Float
					   ): (Map[Int, Double], (Long, Long)) = {
		//train
		var trainInfo: Map[Int, Double] = Map()
		var posNum: Long = 0
		var negNum: Long = 0
		for (iterNum <- 0 until numIterations) {
			val result = data.mapPartitions { iter =>
				val client = new PSClient[Long, Float](ps)
				val updater = new LRUpdater()
					.setLearningRate(learningRate)
				client.setUpdater(updater)
				var (count, totalCount, posNum, negNum, batchId, totalLoss) = (0L, 0L, 0L, 0L, 0, 0.0D)
				val weightIndex = new util.HashSet[Long]()
				val localData = new util.ArrayList[(Double, Array[Long], Array[Float])]()
				while (iter.hasNext) {
					val dataLine = iter.next()
					localData.add(dataLine)
					dataLine._2.foreach(weightIndex.add)
					if (iterNum == 0) {
						if (dataLine._1 > 0.0)
							posNum += 1
						else
							negNum += 1
					}
					count += 1
					if (count == batchSize || !iter.hasNext) {
						var s = System.nanoTime()
						val localMap = client.pull(weightIndex).toMap
						val totalGrad = new util.HashMap[Long, Float]((weightIndex.size / 0.75f + 1).toInt)
						localData.foreach { dataIter =>
							val (grad, loss) = BinaryClassify.train(
								dataIter._1,
								(dataIter._2, dataIter._3),
								localMap)
							// update the inc
							grad.foreach { case (id, v) =>
								totalGrad.put(id, totalGrad.getOrElse(id, 0F) + v.toFloat)
							}
							totalLoss += loss
						}
						val pushMap = totalGrad.map { case (k, v) => (k, v / count) }.toMap
						client.push(pushMap)
						s = System.nanoTime()
						batchId += 1
						totalCount += count
						count = 0
						localData.clear()
						weightIndex.clear()
					}
				}
				println("loss:" + totalLoss)
				client.shutDown()
				Iterator((totalLoss, totalCount, posNum, negNum))
			}
			val totalResult = result.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
			posNum = totalResult._3
			negNum = totalResult._4
			trainInfo += ((iterNum + 1) -> (totalResult._1 / totalResult._2))
		}
		(trainInfo, (posNum, negNum))
	}

	def predict(ps: PS,
				data: RDD[(Double, Array[Long], Array[Float])],
				batchSize: Int
			   ): RDD[(Double, Double)] = {

		data.mapPartitions { iter => {
			val client = new PSClient[Long, Float](ps)
			var preList = List[(Double, Double)]()
			var count = 0
			val weightIndex = new util.HashSet[Long]()
			val localData = new util.ArrayList[(Double, Array[Long], Array[Float])]()
			var batchId = 0
			while (iter.hasNext) {
				val dataLine = iter.next()
				localData.add(dataLine)
				dataLine._2.foreach(weightIndex.add)
				count += 1
				if (count == batchSize || !iter.hasNext) {
					val localWeight = client.pull(weightIndex)
					println("localWeight: " + localWeight.mkString(","))
					//预测label
					localData.foreach{x => preList +:= (BinaryClassify.predict((x._2, x._3), localWeight.toMap), x._1)}
					count = 0
					batchId += 1
					localData.clear()
					weightIndex.clear()
				}
			}
			client.shutDown()
			preList.iterator
		}
		}
	}
}