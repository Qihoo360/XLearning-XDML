package net.qihoo.xitong.xdml.example.analysis.feature.process

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.feature.NumericBucketer
import org.apache.spark.sql.SparkSession

object runNumericBucketer {

  def main(args:Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runNumericBucketer").getOrCreate()

    // data
    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val numPartition = args(4).toInt
    val nullValue = args(5).toString

    val numBucketsArray = args(6).toString.split(",").map(_.toInt)

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    println("input table")
    orgDataDF.show(false)

    require(numBucketsArray.length == schemaHandler.numFeatColNames.length, "invalid numBucketsArray size")

    val numericBucketer = new NumericBucketer()
      .setInputCols(schemaHandler.numFeatColNames)
      .setOutputCols(schemaHandler.numFeatColNames.map(name=>name+"xdmlBucketed"))
      .setNumBucketsArray(numBucketsArray)
      .setIndexOnly(false)
      .setOutputSparse(true)
      .setDropInputCols(true)

    val startTime = System.currentTimeMillis()
    val numericBucketerModel = numericBucketer.fit(orgDataDF)
    val fitTime = System.currentTimeMillis()

    val processedDataDF = numericBucketerModel.transform(orgDataDF)
    println("output table")
    processedDataDF.show(false)
    println("dataCount: " + processedDataDF.count())

    val transTime = System.currentTimeMillis()
    println("time for fitting: "+(fitTime-startTime)/1000.0)
    println("time for transforming: "+(transTime-fitTime)/1000.0)

    spark.stop()
  }

}