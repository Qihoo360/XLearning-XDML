package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.model.supervised.{H2OGBM, H2OGBMModel}
import org.apache.spark.sql.SparkSession

object runFromDenseDataToXDMLH2OGBM {

  def main(args: Array[String]) {
    LogHandler.avoidLog()

    val spark = SparkSession
      .builder()
      .appName("runFromDenseDataToXDMLH2OGBM")
      .getOrCreate()

    // data
    val trainPath = args(0).toString
    val validPath = args(1).toString
    val dataDelimiter = args(2).toString
    val schemaPath = args(3).toString
    val schemaDelimiter = args(4).toString
    val numPartition = args(5).toInt
    val nullValue = args(6).toString

    //  model path
    val modelPath = args(7).toString

    // hyperparameter
    val maxDepth = args(8).toInt
    val numTrees = args(9).toInt
    val maxBinsForCat = args(10).toInt
    val maxBinsForNum = args(11).toInt
    val minInstancesPerNode = args(12).toInt
    val categoricalEncodingScheme = args(13).toString
    val histogramType = args(14).toString
    val learnRate = args(15).toDouble
    val learnRateAnnealing = args(16).toDouble
    val distribution = args(17).toString
    val scoreTreeInterval = args(18).toInt

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgTrainDataDF = DataHandler.readData(spark, trainPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    orgTrainDataDF.show()
    val orgValidDataDF =
      if (validPath.trim.length > 0) {
        DataHandler.readData(spark, validPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
      } else {
        null
      }

    val h2oGBM = new H2OGBM()
      .setLabelCol(schemaHandler.labelColName(0))
      .setCatFeatColNames(schemaHandler.catFeatColNames)
      .setIgnoreFeatColNames(schemaHandler.otherColNames)
      .setMaxDepth(maxDepth)
      .setNumTrees(numTrees)
      .setMaxBinsForCat(maxBinsForCat)
      .setMaxBinsForNum(maxBinsForNum)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setCategoricalEncodingScheme(categoricalEncodingScheme)
      .setHistogramType(histogramType)
      .setLearnRate(learnRate)
      .setLearnRateAnnealing(learnRateAnnealing)
      .setDistribution(distribution)
      .setScoreTreeInterval(scoreTreeInterval)

    val gbmModel = h2oGBM.fit(orgTrainDataDF)
    gbmModel.write.overwrite().save(modelPath)
    val gbmModel2 = H2OGBMModel.load(modelPath)
    gbmModel2.transformSchema(orgValidDataDF.schema).printTreeString()

    val predDataDF = gbmModel2.transform(orgValidDataDF)
    predDataDF.show(false)

    spark.stop()
  }

}
