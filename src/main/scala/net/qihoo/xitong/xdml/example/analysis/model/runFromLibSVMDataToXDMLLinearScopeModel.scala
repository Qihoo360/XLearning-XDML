package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.LogHandler
import org.apache.spark.ml.model.supervised.LinearScope
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.data.DataProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Row, SparkSession}

object runFromLibSVMDataToXDMLLinearScopeModel {
  def main(args: Array[String]) {
    LogHandler.avoidLog()
    val spark = SparkSession
      .builder()
      .appName("runFromLibSVMDataToXDMLLinearScopeModel")
      .getOrCreate()

    val trainPath = args(0).toString
    val validPath = args(1).toString
    val numFeatures = args(2).toInt
    val numPartitions = args(3).toInt
    val oneBased = args(4).toBoolean
    val needSort = args(5).toBoolean
    val rescaleBinaryLabel = args(6).toBoolean
    val stepSize = args(7).toDouble
    val numIterations = args(8).toInt
    val regParam = args(9).toDouble
    val elasticNetParam = args(10).toDouble
    val factor = args(11).toDouble
    val fitIntercept = args(12).toBoolean
    val lossType = args(13).toString
    val posWeight = args(14).toDouble

    val trainData = DataProcessor.readLibSVMDataAsDF(spark, trainPath, numFeatures, numPartitions,
      oneBased, needSort, rescaleBinaryLabel)
    trainData.show(2)

    val validData =
      if (validPath.trim.length > 0) {
        DataProcessor.readLibSVMDataAsDF(spark, validPath, numFeatures, numPartitions,
          oneBased, needSort, rescaleBinaryLabel)
      } else {
        null
      }
    if(validData!=null) {
      validData.show(2)
    }

    val convergenceTol = 0.2
    val linearScope = new LinearScope()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setMaxIter(numIterations)
        .setStepSize(stepSize)
        .setLossFunc(lossType)
        .setRegParam(regParam)
        .setElasticNetParam(elasticNetParam)
        .setFactor(factor)
        .setFitIntercept(fitIntercept)
        .setPosWeight(posWeight)
        .setConvergenceTol(convergenceTol)
        .setNumPartitions(numPartitions)

    val model = linearScope.fit(trainData)
   /***
     * also can set initial weights of w
    val initialWeights = Vectors.zeros(numFeatures)
    val model = linearScope.fit(trainData,initialWeights)*/
    model.setRawPredictionCol("prediction")
         .setProbabilityCol("probability")
    if (validPath.trim.length > 0){
      val df = model.transform(validData)
      df.show(5)
      val predRDD=df.select(col("prediction"),col("label").cast(DoubleType)).rdd.map{case Row(pred:Double,label:Double)=>{(pred,label)}}
      val metrics = new BinaryClassificationMetrics(predRDD)
      println("Model Validating AUROC: " + metrics.areaUnderROC())
    }else{
      val df = model.transform(trainData)
      df.show(5)
      val predRDD=df.select(col("prediction"),col("label").cast(DoubleType)).rdd.map{case Row(pred:Double,label:Double)=>{(pred,label)}}
      val metrics = new BinaryClassificationMetrics(predRDD)
      println("Model Training AUROC: " + metrics.areaUnderROC())
    }
    spark.stop()
  }

}
