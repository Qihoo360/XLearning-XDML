package net.qihoo.xitong.xdml.example.analysis.feature.process

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.feature.NumericStandardizer
import org.apache.spark.sql.SparkSession


object runNumericStandardizer {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runNumericStandardizer").getOrCreate()

    // data
    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val numPartition = args(4).toInt
    val nullValue = args(5).toString

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    println("input table")
    orgDataDF.show(false)

    val numericStandardizer = new NumericStandardizer()
      .setInputCols(schemaHandler.numFeatColNames)
      .setOutputCols(schemaHandler.numFeatColNames.map(name => name + "XDMLStandardized"))

    val startTime = System.currentTimeMillis()
    val numericStandardizerModel = numericStandardizer.fit(orgDataDF)
    val fitTime = System.currentTimeMillis()

    val processedDataDF = numericStandardizerModel.transform(orgDataDF)
    println("output table")
    processedDataDF.show(false)
    println("dataCount: " + processedDataDF.count())

    val transTime = System.currentTimeMillis()
    println("time for fitting: "+(fitTime-startTime)/1000.0)
    println("time for transforming: "+(transTime-fitTime)/1000.0)

    spark.stop()

  }

}
