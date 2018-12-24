package net.qihoo.xitong.xdml.ml

import java.io.Serializable
import java.util

import bloomfilter.mutable.BloomFilter
import net.qihoo.xitong.xdml.optimization.FFM
import net.qihoo.xitong.xdml.ps.{PS, PSClient}
import net.qihoo.xitong.xdml.updater.FFMUpdater
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class FieldawareFactorizationMachine(ps:PS) extends Serializable {
	private var iterNum = 1
	private var batchSize = 50
	private var learningRate:Float = 0.01F
	private var rank = 1
	private var field = 1
	def fit(data: RDD[(Double, Array[Int], Array[Long], Array[Float])]): (Map[Int, Double], (Long, Long)) = {
		FieldawareFactorizationMachine.runMiniBatchSGD(ps,data, iterNum, batchSize, rank, field, learningRate)
	}
	def setIterNum(iterNum:Int):this.type = {
		this.iterNum = iterNum
		this
	}

	def setBatchSize(batchSize:Int):this.type = {
		this.batchSize = batchSize
		this
	}

	def setLearningRate(lr:Float):this.type ={
		this.learningRate = lr
		this
	}

	def setRank(rank:Int):this.type ={
		this.rank = rank
		this
	}

	def setField(field:Int):this.type ={
		this.field = field
		this
	}
}


object FieldawareFactorizationMachine{
	def runMiniBatchSGD(ps:PS,
						data: RDD[(Double, Array[Int], Array[Long], Array[Float])],
						iter: Int,
						batchSize: Int,
						rank: Int,
						fieldNum: Int,
						learningRate: Float
					   ): (Map[Int, Double], (Long, Long)) = {
		println(s"Start the hz cluster ...")
		var trainInfo: Map[Int, Double] = Map()
		var posNum = 0L
		var negNum = 0L
		for (iterNum <- 0 until iter) {
			val result = data.mapPartitions { iter =>
				val updater = new FFMUpdater()
    				.setLearningRate(learningRate)
				val client = new PSClient[Long, Array[Float]](ps)
    				.setUpdater(updater)
				var count = 0
				val weightIndex = new util.HashSet[Long]()
				val localData = new util.ArrayList[(Double, Array[Int], Array[Long], Array[Float])]()
				var batchId = 0
				var flag = false
				var totalLoss = 0.0
				var totalCount = 0L
				var posNum = 0L
				var negNum = 0L
				var batchLoss = 0.0
				val expectedElements = 100000000
				val falsePositiveRate = 0.1
				val bf = BloomFilter[Long](expectedElements, falsePositiveRate)
				var partBatchSize = batchSize

				while (iter.hasNext) {
					val dataLine = iter.next()
					localData.add(dataLine)
					dataLine._3.foreach(x =>
						if (bf.mightContain(x)) {
							weightIndex.add(x)
						} else {
							bf.add(x)
						}
					)
					if (iterNum == 0) {
						if (dataLine._1 > 0.0)
							posNum += 1
						else
							negNum += 1
					}
					count += 1
					if (count == partBatchSize || !iter.hasNext) {
						println(s"weightIndex.size: ${weightIndex.size()}")

						val localV = client.pull(weightIndex)
						println("localV: " + localV.map{
							case(k,v) => k + "-->" + v.mkString(",")
						})

						var totalGrad: Map[Long, Array[Float]] = Map()
						localData.foreach { dataIter =>
							// train the data, get the grad and loss
							val (grad, loss) = FFM.train(
								fieldNum,
								rank,
								dataIter._1,
								(dataIter._2, dataIter._3, dataIter._4),
								localV
							)
							println("loss: " + loss)
							// update the inc
							grad.foreach { case (k, v) =>
								val gradArray = totalGrad.getOrElse(k, Array.fill(rank * fieldNum + fieldNum)(0.0f))
								for(index <- 0 until rank*fieldNum) {
									//一组batch的梯度累加
									gradArray(index) = gradArray(index) + v(index)
								}
								totalGrad += (k -> gradArray)
							}
							batchLoss += loss
						}
						println(s"TotalLoss: ${batchLoss} . SampleNum: ${localData.size()}. Average Loss: ${batchLoss / count}")
						val us = System.nanoTime()
						client.push(totalGrad)

						val ue = System.nanoTime()
						println(s"update weight time consume is: ${(ue - us) / 1e9}")
						batchId += 1
						flag = true
						totalLoss += batchLoss
						totalCount += count
						count = 0
						batchLoss = 0.0
						localData.clear()
						weightIndex.clear()
					}

					if (batchId % 50 == 0 && flag) {
						val s = System.nanoTime()
						println(s"epoch ${iterNum} sync begin.")
						Thread.sleep(2000)
						val e = System.nanoTime()
						println(s"epoch ${iterNum} sync end, consume is: ${(e - s) / 1e9}")
						flag = false
					}
				}
				Iterator((totalLoss, totalCount, posNum, negNum))
			}
			if (iterNum == 0) {
				val totalResult = result.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
				posNum = totalResult._3
				negNum = totalResult._4
				trainInfo += ((iterNum + 1) -> (totalResult._1 / totalResult._2))
			} else {
				val totalResult = result.map(x => (x._1, x._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
				trainInfo += ((iterNum + 1) -> (totalResult._1 / totalResult._2))
			}
		}
		Thread.sleep(60000)
		(trainInfo, (posNum, negNum))
	}
}