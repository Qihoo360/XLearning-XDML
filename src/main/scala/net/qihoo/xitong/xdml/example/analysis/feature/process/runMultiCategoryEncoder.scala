package net.qihoo.xitong.xdml.example.analysis.feature.process

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.feature.MultiCategoryEncoder
import org.apache.spark.sql.SparkSession


object runMultiCategoryEncoder {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("testMultiCategoryEncoder").getOrCreate()

    // data
    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val numPartition = args(4).toInt
    val nullValue = args(5).toString
    val multiCatDelimiter = args(6).toString

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    println("input table")
    orgDataDF.show(false)

    val encoder = new MultiCategoryEncoder()
      .setInputCols(schemaHandler.multiCatFeatColNames)
      .setOutputCols(schemaHandler.multiCatFeatColNames.map(name => name + "xdmlEncoded"))
      .setDelimiter(multiCatDelimiter)
      .setIndexOnly(true)
//      .setDropInputCols(true)

    val startTime = System.currentTimeMillis()
    val encoderModel = encoder.fit(orgDataDF)
    val fitTime = System.currentTimeMillis()
    val processedDataDF = encoderModel.transform(orgDataDF)
    println("output table")
    processedDataDF.show(false)
    println("dataCount: " + processedDataDF.count())

    val transTime = System.currentTimeMillis()
    println("time for fitting: "+(fitTime-startTime)/1000.0)
    println("time for transforming: "+(transTime-fitTime)/1000.0)

    spark.stop()

  }

}
