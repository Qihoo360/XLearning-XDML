package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.model.supervised.{H2OGLM, H2OGLMModel}
import org.apache.spark.sql.SparkSession


object runFromDenseDataToXDMLH2OGLM {

  def main(args: Array[String]) {
    LogHandler.avoidLog()

    val spark = SparkSession
      .builder()
      .appName("runFromDenseDataToXDMLH2OGLM")
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
    val family = args(8).toString
    val maxIter = args(9).toInt
    val alpha = args(10).toDouble
    val lambda = args(11).toDouble
    val missingValuesHandling = args(12).toString
    val solver = args(13).toString
    val standardization = args(14).toBoolean
    val fitIntercept = args(15).toBoolean


    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgTrainDataDF = DataHandler.readData(spark, trainPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    orgTrainDataDF.show()
    val orgValidDataDF =
      if (validPath.trim.length > 0) {
        DataHandler.readData(spark, validPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
      } else {
        null
      }

    val h2oGLM = new H2OGLM()
      .setLabelCol(schemaHandler.labelColName(0))
      .setCatFeatColNames(schemaHandler.catFeatColNames)
      .setIgnoreFeatColNames(schemaHandler.otherColNames)
      .setFamily(family)
      .setMaxIter(maxIter)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setFitIntercept(fitIntercept)
      .setStandardization(standardization)
      .setMissingValueHandling(missingValuesHandling)

    val glmModel = h2oGLM.fit(orgTrainDataDF)
    glmModel.write.overwrite().save(modelPath)
    val glmModel2 = H2OGLMModel.load(modelPath)

    val predDF = glmModel2.transform(orgValidDataDF)
    predDF.show(false)
    spark.stop()
  }
}
