package net.qihoo.xitong.xdml.example.analysis.feature.process

import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml.feature.CategoryEncoder
import org.apache.spark.sql.SparkSession


object runCategoryEncoder {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("testCategoryEncoder").getOrCreate()

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

    val indexer = new CategoryEncoder()
      .setInputCols(schemaHandler.catFeatColNames)
      .setOutputCols(schemaHandler.catFeatColNames.map(name => name + "xdmlIndexed"))
      .setIndexOnly(true)
//      .setDropInputCols(true)
//      .setCategoriesReserved(5)

    val startTime = System.currentTimeMillis()
    val indexerModel = indexer.fit(orgDataDF)
    val fitTime = System.currentTimeMillis()
//    indexerModel.labels.foreach{ seq =>
//      println(seq.toArray.mkString(", "))
//    }
    val processedDataDF = indexerModel.transform(orgDataDF)
    println("output table")
    processedDataDF.show(false)
    println("dataCount: " + processedDataDF.count())

    val transTime = System.currentTimeMillis()
    println("time for fitting: "+(fitTime-startTime)/1000.0)
    println("time for transforming: "+(transTime-fitTime)/1000.0)

    spark.stop()

  }

}
