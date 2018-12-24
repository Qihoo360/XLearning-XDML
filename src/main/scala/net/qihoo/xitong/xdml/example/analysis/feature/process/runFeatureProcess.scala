package net.qihoo.xitong.xdml.example.analysis.feature.process

import net.qihoo.xitong.xdml.feature.process.FeatureProcessor
import net.qihoo.xitong.xdml.model.data.{DataHandler, LogHandler, SchemaHandler}
import org.apache.spark.ml._
import org.apache.spark.sql.SparkSession


object runFeatureProcess {

  def main(args: Array[String]) {

    LogHandler.avoidLog()

    val spark = SparkSession.builder()
      .appName("runFeatureProcess").getOrCreate()

    // data
    val dataPath = args(0).toString
    val dataDelimiter = args(1).toString
    val schemaPath = args(2).toString
    val schemaDelimiter = args(3).toString
    val numPartition = args(4).toInt
    val nullValue = args(5).toString

    // job type
    val jobType = args(6).toString

    val methodForNum = args(7).toString
    val ifOnehotForNum = args(8).toBoolean
    val ifOnehotForCat = args(9).toBoolean
    val ifOnehotForMultiCat = args(10).toBoolean

    val numBuckets = args(11).toInt
    val categoriesReservedForCat = args(12).toInt
    val categoriesReservedForMultiCat = args(13).toInt
    val multiCatDelimiter = args(14).toString

    // pipeline model path
    val pipelineModelPath = args(15)
    // processed data path
    val processedDataPath = args(16)

    // data input
    val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
    val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
    println("input table")
    orgDataDF.show(false)

    val resDF = jobType match {
      case "fit_transform" => {
        val (resDFTmp, pipelineModelTmp) = FeatureProcessor.pipelineFitTransform(orgDataDF, schemaHandler,
          methodForNum, ifOnehotForNum, ifOnehotForCat, ifOnehotForMultiCat, true,
          numBuckets, categoriesReservedForCat, categoriesReservedForMultiCat, multiCatDelimiter)
        pipelineModelTmp.write.overwrite().save(pipelineModelPath)
        resDFTmp
      }
      case "transform" => {
        val pipelineModelTmp = PipelineModel.load(pipelineModelPath)
        val resDFTmp = pipelineModelTmp.transform(orgDataDF)
        resDFTmp
      }
      case _ => throw new IllegalArgumentException(s"Does not support job type: $jobType")
    }

    println("output table")
    resDF.show(false)
    DataHandler.writeLibSVMData(resDF, FeatureProcessor.labelProcessedColName,
      FeatureProcessor.featsProcessedColName, processedDataPath)

    spark.stop()

  }

}
