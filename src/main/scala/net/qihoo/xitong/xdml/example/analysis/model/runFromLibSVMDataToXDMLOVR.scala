package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.LogHandler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.data.DataProcessor
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.model.supervised.OVRLinearScope

object runFromLibSVMDataToXDMLOVR {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession
      .builder()
      .appName("runFromLibSVMDataToXDMLOVR")
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
    val numClasses = args(9).toInt
    val lossType = args(10).toString

    val trainDF = DataProcessor.readLibSVMDataAsDF(spark, trainPath, numFeatures, numPartitions,
      oneBased, needSort, rescaleBinaryLabel)
    val validDF =
      if (validPath.trim.length > 0) {
        DataProcessor.readLibSVMDataAsDF(spark, validPath, numFeatures, numPartitions,
          oneBased, needSort, rescaleBinaryLabel)
      } else {
        null
      }

    val ovrls = new OVRLinearScope()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setStepSize(stepSize)
      .setMaxIter(numIterations)
      .setNumClasses(numClasses)
      .setNumPartitions(numPartitions)
      .setLossFunc(lossType)

    val ovrlsModel = ovrls.fit(trainDF)

    if (validPath.trim.length > 0) {

      val df = ovrlsModel.transform(validDF)
      df.show(false)

      if (numClasses > 2) {
        /*********** MulticlassClassificationEvaluator *************/
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val acc = evaluator.evaluate(df)
        println("Model Validating accuracy: " + acc)
      } else {
        /*********** BinaryClassificationEvaluator *************/
        val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setRawPredictionCol("rawPrediction")
          .setMetricName("areaUnderROC")
        val auroc = evaluator.evaluate(df)
        println("Model Validating AUROC: " + auroc)
      }

    }

    spark.stop()

  }

}