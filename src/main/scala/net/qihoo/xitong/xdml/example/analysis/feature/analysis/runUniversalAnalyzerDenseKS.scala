package net.qihoo.xitong.xdml.example.analysis.feature.analysis

import net.qihoo.xitong.xdml.feature.analysis.UniversalAnalyzer
import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.sql.SparkSession

object runUniversalAnalyzerDenseKS {

  def main(args: Array[String]): Unit={

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runUniversalAnalyzerDenseKS").getOrCreate()

    val orgDataPath = args(0).toString
    val orgDataDelimiter = args(1).toString
    val orgSchemaPath = args(2).toString
    val orgSchemaDelimiter = args(3).toString
    val cmpDataPath = args(4).toString
    val cmpDataDelimiter = args(5).toString
    val cmpSchemaPath = args(6).toString
    val cmpSchemaDelimiter = args(7).toString
    val numPartition = args(8).toInt
    val nullValue = args(9).toString
    val withHeader = args(10).toBoolean
    val savePath = args(11).toString

    val orgSchemaHandler = SchemaHandler.readSchema(spark.sparkContext, orgSchemaPath, orgSchemaDelimiter)
    val cmpSchemaHandler = SchemaHandler.readSchema(spark.sparkContext, cmpSchemaPath, cmpSchemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, orgDataPath, orgDataDelimiter, orgSchemaHandler.schema, nullValue, numPartition, withHeader)
    val cmpDataDF = DataHandler.readData(spark, cmpDataPath, cmpDataDelimiter, cmpSchemaHandler.schema, nullValue, numPartition, withHeader)

    val time1 = System.currentTimeMillis()
    val ksValues = UniversalAnalyzer.fitDenseKSForNum(orgDataDF, orgSchemaHandler.numFeatColNames, cmpDataDF, cmpSchemaHandler.numFeatColNames)
    val time2 = System.currentTimeMillis()
    println("spent time", (time2 - time1))
    spark.sparkContext.parallelize(Seq(ksValues.mkString("\n")))
      .repartition(1).saveAsTextFile(savePath)

    spark.stop()
  }

}
