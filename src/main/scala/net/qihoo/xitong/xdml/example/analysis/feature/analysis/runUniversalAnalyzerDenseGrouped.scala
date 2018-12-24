package net.qihoo.xitong.xdml.example.analysis.feature.analysis

import net.qihoo.xitong.xdml.feature.analysis.UniversalAnalyzer
import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.sql.SparkSession

object runUniversalAnalyzerDenseGrouped {

  def main(args: Array[String]): Unit = {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runUniversalAnalyzerDenseGrouped").getOrCreate()

    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val numPartition = args(4).toInt
    val nullValue = args(5).toString

    val groupColName = args(6).toString
    val topKStr = args(7).toString
    val topKDelimiter = args(8).toString

    val topKs = topKStr.split(topKDelimiter).map(_.toInt)

    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    orgDataDF.show()

    val groupFeatSummaryArray = UniversalAnalyzer.fitDenseGrouped(orgDataDF,
                                                                      schemaHandler.labelColName(0),
                                                                      groupColName,
                                                                      schemaHandler.numFeatColNames,
                                                                      topKs,
                                                                      gainFunc,
                                                                      discountFunc)
    println("-------------------metric result----------------------")
    groupFeatSummaryArray.foreach{ summary =>
      println("\n=====================")
      println(summary.name)
      println("reversePairRate: " + summary.reversePairRate)
      println("ndcgs: " + summary.ndcgMap.toString())
    }
    spark.stop()
  }

  def gainFunc(rating: Int): Double = {
    val gains = Array(0, 1, 3, 7, 15)
    val gainSize = gains.length
    if (rating < 0) {
      0
    } else if (rating < gainSize) {
      gains(rating)
    } else {
      0
      // gains.max + (rating - gainSize)
    }
  }

  def discountFunc(index: Int): Double = {
    val log2 = math.log(2)
    math.log(index + 2) / log2
  }

}
