package net.qihoo.xitong.xdml.model.data

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DataHandler extends Serializable {

  // WARNING: there could be a problem if the number of partitions is very low
  // TODO: test big file with only one part
  // TODO: test nullValue
  def readData(spark: SparkSession,
               dataPath: String,
               dataDelimiter: String,
               structType: StructType,
               nullValue: String,
               numPartitions: Int = -1,
               header: Boolean = false): DataFrame = {
    val df = spark.read
      .option("sep", dataDelimiter)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("nullValue", nullValue)
      .option("header", header)
      .schema(structType)
      .csv(dataPath)
    if (numPartitions > 1) {
      df.repartition(numPartitions)
    } else {
      df
    }
  }

  def writeData(df: DataFrame,
                path: String,
                delimiter: String,
                numPartitions: Int = -1,
                header: Boolean = false): Unit = {
    val dfTarget = if (numPartitions > 1) df.repartition(numPartitions) else df
    dfTarget.write.option("sep", delimiter).option("header", header).csv(path)
  }

  // WARNING: there could be a problem if the number of partitions is very low
  def readLibSVMData(spark: SparkSession,
                     dataPath: String,
                     numFeatures: Int = -1,
                     numPartitions: Int = -1,
                     labelColName: String = "labelLibSVM",
                     featuresColName: String = "featuresLibSVM"): DataFrame = {
    val df = spark.read.format("libsvm").option("numFeatures", numFeatures).load(dataPath)
      .withColumnRenamed("label", labelColName)
      .withColumnRenamed("features", featuresColName)
    if (numPartitions > 1) {
      df.repartition(numPartitions)
    } else {
      df
    }
  }

  def writeLibSVMData(df: DataFrame,
                      labelColName: String,
                      featuresColName: String,
                      path: String,
                      numPartitions: Int = -1): Unit = {
    val dfTarget = if (numPartitions > 1) df.repartition(numPartitions) else df
    val resRDD = dfTarget.select(col(labelColName).cast(DoubleType), col(featuresColName)).rdd.map{
      case Row(label: Double, features: Vector) => {
        features match {
          case sv: SparseVector => {
            val kvs = sv.indices.zip(sv.values)
            label.toString + " " + kvs.map{ case (k, v) => k.toString + ":" + v.toString }.mkString(" ")
          }
          case dv: DenseVector => {
            label.toString + " " + dv.toArray.zipWithIndex.map{ case (v, k) => k.toString + ":" + v.toString }.mkString(" ")
          }
        }
      }
    }
    resRDD.saveAsTextFile(path)
  }

  // TODO: to be checked
  // WARNING: there could be a problem if the number of partitions is very low
  def readHiveData(spark: SparkSession,
                   tableName: String,
                   numPartitions: Int = -1): DataFrame = {
    val df = spark.read.table(tableName)
    if (numPartitions > 1) {
      df.repartition(numPartitions)
    } else {
      df
    }
  }

}
