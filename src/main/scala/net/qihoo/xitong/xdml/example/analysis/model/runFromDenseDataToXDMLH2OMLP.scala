package net.qihoo.xitong.xdml.example.analysis.model

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.model.supervised.{H2OMLP, H2OMLPModel}
import org.apache.spark.sql.SparkSession

object runFromDenseDataToXDMLH2OMLP {

  def main(args: Array[String]) {
    LogHandler.avoidLog()

    val spark = SparkSession
      .builder()
      .appName("runFromDenseDataToXDMLH2OMLP")
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
    val missingValuesHandling = args(8).toString
    val categoricalEncodingScheme = args(9).toString
    val distribution = args(10).toString
    val hidden = args(11).toString.split(",").map(_.toInt)
    val activation = args(12).toString
    val epochs = args(13).toDouble
    val learningRate = args(14).toDouble
    val momentumStart = args(15).toDouble
    val momentumStable = args(16).toDouble
    val l1 = args(17).toDouble
    val l2 = args(18).toDouble
    val hiddenDropoutRatiosText = args(19).toString
    val elasticAveraging = args(20).toBoolean
    val standardization = args(21).toBoolean

    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgTrainDataDF = DataHandler.readData(spark, trainPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    orgTrainDataDF.show()
    val orgValidDataDF =
      if (validPath.trim.length > 0) {
        DataHandler.readData(spark, validPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
      } else {
        null
      }

    val hiddenDropoutRatios = if(hiddenDropoutRatiosText.trim.length > 0) hiddenDropoutRatiosText.split(",").map{ _.toDouble} else Array[Double]()

    val h2oMLP = new H2OMLP()
      .setLabelCol(schemaHandler.labelColName(0))
      .setCatFeatColNames(schemaHandler.catFeatColNames)
      .setIgnoreFeatColNames(schemaHandler.otherColNames)
      .setMissingValueHandling(missingValuesHandling)
      .setCategoricalEncodingScheme(categoricalEncodingScheme)
      .setDistribution(distribution)
      .setHidden(hidden)
      .setActivation(activation)
      .setEpochs(epochs)
      .setLearnRate(learningRate)
      .setMomentumStart(momentumStart)
      .setMomentumStable(momentumStable)
      .setL1(l1)
      .setL2(l2)
      .setHiddenDropoutRatios(hiddenDropoutRatios)
      .setElasticAveraging(elasticAveraging)
      .setStandardization(standardization)

    val mlpModel = h2oMLP.fit(orgTrainDataDF,null)
    mlpModel.write.overwrite().save(modelPath)
    val mlpModel2 = H2OMLPModel.load(modelPath)
    mlpModel2.transformSchema(orgValidDataDF.schema).printTreeString()

    val predDataDF = mlpModel2.transform(orgValidDataDF)
    predDataDF.show(false)

    spark.stop()
  }
}
