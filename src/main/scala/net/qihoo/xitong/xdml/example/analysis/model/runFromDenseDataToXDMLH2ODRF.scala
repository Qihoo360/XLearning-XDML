package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.model.supervised.{H2ODRF, H2ODRFModel}
import org.apache.spark.sql.SparkSession

object runFromDenseDataToXDMLH2ODRF {

  def main(args: Array[String]) {
    LogHandler.avoidLog()

    val spark = SparkSession
      .builder()
      .appName("runFromDenseDataToXDMLH2ODRF")
      .getOrCreate()

    // data
    val trainPath = args(0).toString
    val validPath = args(1).toString
    val dataDelimiter = args(2).toString
    val schemaPath = args(3).toString
    val schemaDelimiter = args(4).toString
    val numPartition = args(5).toInt
    val nullValue = args(6).toString
    val modelPath = args(7).toString

    // hyperparameter
    val maxDepth = args(8).toInt
    val numTrees = args(9).toInt
    val maxBinsForCat = args(10).toInt
    val maxBinsForNum = args(11).toInt
    val minInstancesPerNode = args(12).toInt
    val categoricalEncodingScheme = args(13).toString
    val histogramType = args(14).toString
    val distribution = args(15).toString
    val scoreTreeInterval = args(16).toInt

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

    val h2oDRF = new H2ODRF()
      .setLabelCol(schemaHandler.labelColName(0))
      .setCatFeatColNames(schemaHandler.catFeatColNames)
      .setIgnoreFeatColNames(schemaHandler.otherColNames)
      .setMaxDepth(maxDepth)
      .setNumTrees(numTrees)
      .setMaxBinsForCat(maxBinsForCat)
      .setMaxBinsForNum(maxBinsForNum)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setCategoricalEncodingScheme(categoricalEncodingScheme.trim())
      .setHistogramType(histogramType.trim())
      .setDistribution(distribution.trim())
      .setScoreTreeInterval(scoreTreeInterval)

    val drfModel = h2oDRF.fit(orgTrainDataDF)
    drfModel.write.overwrite().save(modelPath)
    val drfModel2 = H2ODRFModel.load(modelPath)
    drfModel2.transformSchema(orgValidDataDF.schema).printTreeString()

    val predDataDF = drfModel2.transform(orgValidDataDF)
    predDataDF.show(false)

    spark.stop()
  }
}
