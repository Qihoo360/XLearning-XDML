package net.qihoo.xitong.xdml.example.analysis.feature.analysis

import net.qihoo.xitong.xdml.feature.analysis._
import net.qihoo.xitong.xdml.model.data.{LogHandler, SchemaHandler}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap

object runUniversalAnalyzerSparse {

  trait ParseBase extends Serializable {
    def parse(str: String): HashMap[String, String]
  }

  class StandardParse(stringDelimiter: String = ",", pairDelimiter: String = ":") extends ParseBase {
    def parse(str: String): HashMap[String, String] = {
      val arrOfTuple2 = str.split(stringDelimiter).map{ elem =>
        val elemSplit = elem.split(pairDelimiter)
        (elemSplit(0), elemSplit(1))
      }
      HashMap[String, String]() ++= arrOfTuple2
    }
  }

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runUniversalAnalyzerSparse").getOrCreate()

    val dataPath = args(0).toString
    val parseType = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val hasLabel = args(4).toBoolean
    val numPartitions = args(5).toInt
    val sparseType = args(6).toString
    val fineness = args(7).toInt

    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val tmpDataRDD = spark.sparkContext.textFile(dataPath, numPartitions)
    val orgDataRDD = parseType match {
      case "standard" =>
        val standardParse = new StandardParse()
        tmpDataRDD.map{ str => standardParse.parse(str) }
      case _ => throw new IllegalArgumentException("parse type unknown")
    }

    val labelColName =
      if(hasLabel){
        schemaHandler.labelColName(0)
      }else{
        ""
      }

    val (catFeatSummaryArray, numFeatSummaryArray) = UniversalAnalyzer.fitSparse(orgDataRDD, hasLabel,
      labelColName, schemaHandler.catFeatColNames, schemaHandler.numFeatColNames,
      sparseType, Range(0, fineness+1).toArray.map{ d => 1.0*d/fineness })

    catFeatSummaryArray.foreach{ summary =>
      println("\n===========================================")
      println(summary.name)
      println(summary.concernedCategories.map(tup => tup._1).mkString(","))
      println("mi: " + summary.mi)
      println("auc: " + summary.auc)
    }

    numFeatSummaryArray.foreach{ summary =>
      println("\n===========================================")
      println(summary.name)
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
