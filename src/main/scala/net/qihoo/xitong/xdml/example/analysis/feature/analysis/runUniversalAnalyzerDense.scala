package net.qihoo.xitong.xdml.example.analysis.feature.analysis

import net.qihoo.xitong.xdml.feature.analysis._
import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.sql.SparkSession

object runUniversalAnalyzerDense {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runUniversalAnalyzerDense").getOrCreate()

    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val hasLabel = args(4).toBoolean
    val numPartition = args(5).toInt
    val nullValue = args(6).toString
    val fineness = args(7).toInt

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    orgDataDF.show()

    val labelColName =
      if(hasLabel){
        schemaHandler.labelColName(0)
      }else{
        ""
      }

    val (catFeatSummaryArray, numFeatSummaryArray) = UniversalAnalyzer.fitDense(orgDataDF, hasLabel, labelColName,
                                                      schemaHandler.catFeatColNames, schemaHandler.numFeatColNames,
                                                      Range(0, fineness+1).toArray.map{ d => 1.0*d/fineness })

    catFeatSummaryArray.foreach{ summary =>
      println("\n===========================================")
      println(summary.name)
      println("countAll: " + summary.countAll)
      println("countNotNull: " + summary.countNotNull)
      println(summary.concernedCategories.map(tup => tup._1).mkString(","))
      println("mi: " + summary.mi)
      println("auc: " + summary.auc)
    }

    numFeatSummaryArray.foreach{ summary =>
      println("\n===========================================")
      println(summary.name)
      println("countAll: " + summary.countAll)
      println("countNotNull: " + summary.countNotNull)
      println("mean: " + summary.mean)
      println("std: " + summary.std)
      println("skewness: " + summary.skewness)
      println("kurtosis: " + summary.kurtosis)
      println("min: " + summary.min)
      println("max: " + summary.max)
      println("quantiles: " + summary.quantileArray.mkString(","))
      println("corr: " + summary.corr)
      println("auc: " + summary.auc)
    }

    spark.stop()

  }

}
