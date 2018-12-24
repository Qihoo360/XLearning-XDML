package org.apache.spark.sql.data

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD


object DataProcessor extends Serializable {

  // WARNING: there could be a problem if the number of partitions is very low
  // TODO: test big file with only one part
  // TODO: test nullValue
  def readData(spark: SparkSession,
               dataPath: String,
               dataDelimiter: String,
               structType: StructType,
               nullValue: String,
               numPartitions: Int = -1): DataFrame = {
    val df = spark.read
      .option("sep", dataDelimiter)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("nullValue", nullValue)
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
                numPartitions: Int = -1): Unit = {
    val dfTarget = if (numPartitions > 1) df.repartition(numPartitions) else df
    dfTarget.write.option("sep", delimiter).csv(path)
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
    val resRDD = dfTarget.select(labelColName, featuresColName).rdd.map{
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


  /*
  * spark requires binary label of {0,1}
  * DataFrameReader / MLUtils.loadLibSVMFile requires one-based sorted indices
  * */
  def readLibSVMDataAsRDD(spark: SparkSession,
                             dataPath: String,
                             numFeatures: Int,
                             numPartitions: Int = 2,
                             oneBased: Boolean = false,
                             needSort: Boolean = false,
                             rescaleBinaryLabel: Boolean = false,
                             extDelimiter: String = " ",
                             intDelimiter: String = ":",
                             ifCheck: Boolean = false): RDD[(Double, Vector)] = {
    if (ifCheck) {
      readLibSVMDataAsRDDWithCheck(spark, dataPath, numFeatures, numPartitions, oneBased, needSort, rescaleBinaryLabel, extDelimiter, intDelimiter)
    } else {
      readLibSVMDataAsRDDWithoutCheck(spark, dataPath, numFeatures, numPartitions, oneBased, needSort, rescaleBinaryLabel, extDelimiter, intDelimiter)
    }
  }

  def readLibSVMDataAsRDDWithoutCheck(spark: SparkSession,
                                      dataPath: String,
                                      numFeatures: Int,
                                      numPartitions: Int = 2,
                                      oneBased: Boolean = false,
                                      needSort: Boolean = false,
                                      rescaleBinaryLabel: Boolean = false,
                                      extDelimiter: String = " ",
                                      intDelimiter: String = ":"): RDD[(Double, Vector)] = {
    if (needSort) {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      }
    } else {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      }
    }
  }

  def readLibSVMDataAsRDDWithCheck(spark: SparkSession,
                                   dataPath: String,
                                   numFeatures: Int,
                                   numPartitions: Int = 2,
                                   oneBased: Boolean = false,
                                   needSort: Boolean = false,
                                   rescaleBinaryLabel: Boolean = false,
                                   extDelimiter: String = " ",
                                   intDelimiter: String = ":"): RDD[(Double, Vector)] = {
    if (needSort) {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      }
    } else {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, Vectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, Vectors.sparse(numFeatures, kvs))
          }
        }
      }
    }
  }

  def readLibSVMDataAsDF(spark: SparkSession,
                         dataPath: String,
                         numFeatures: Int,
                         numPartitions: Int = 2,
                         oneBased: Boolean = false,
                         needSort: Boolean = false,
                         rescaleBinaryLabel: Boolean = false,
                         extDelimiter: String = " ",
                         intDelimiter: String = ":",
                         ifCheck: Boolean = false,
                         labelColName: String = "label",
                         featuresColName: String = "features"): DataFrame = {
    val dataRDD = readLibSVMDataAsRDD(spark, dataPath, numFeatures, numPartitions, oneBased, needSort, rescaleBinaryLabel, extDelimiter, intDelimiter, ifCheck)
    val rowRDD = dataRDD.map{ tup => Row.fromTuple(tup) }
    val schema = StructType(Array(new StructField(labelColName, DoubleType, false), new StructField(featuresColName, new VectorUDT, false)))
    spark.createDataFrame(rowRDD, schema)
  }


  def readLibSVMDataAsOldRDD(spark: SparkSession,
                             dataPath: String,
                             numFeatures: Int,
                             numPartitions: Int = 2,
                             oneBased: Boolean = false,
                             needSort: Boolean = false,
                             rescaleBinaryLabel: Boolean = false,
                             extDelimiter: String = " ",
                             intDelimiter: String = ":",
                             ifCheck: Boolean = false): RDD[(Double, OldVector)] = {
    if (ifCheck) {
      readLibSVMDataAsOldRDDWithCheck(spark, dataPath, numFeatures, numPartitions, oneBased, needSort, rescaleBinaryLabel, extDelimiter, intDelimiter)
    } else {
      readLibSVMDataAsOldRDDWithoutCheck(spark, dataPath, numFeatures, numPartitions, oneBased, needSort, rescaleBinaryLabel, extDelimiter, intDelimiter)
    }
  }

  def readLibSVMDataAsOldRDDWithoutCheck(spark: SparkSession,
                                         dataPath: String,
                                         numFeatures: Int,
                                         numPartitions: Int = 2,
                                         oneBased: Boolean = false,
                                         needSort: Boolean = false,
                                         rescaleBinaryLabel: Boolean = false,
                                         extDelimiter: String = " ",
                                         intDelimiter: String = ":"): RDD[(Double, OldVector)] = {
    if (needSort) {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      }
    } else {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            (kvArr(0).toInt, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      }
    }
  }


  def readLibSVMDataAsOldRDDWithCheck(spark: SparkSession,
                                      dataPath: String,
                                      numFeatures: Int,
                                      numPartitions: Int = 2,
                                      oneBased: Boolean = false,
                                      needSort: Boolean = false,
                                      rescaleBinaryLabel: Boolean = false,
                                      extDelimiter: String = " ",
                                      intDelimiter: String = ":"): RDD[(Double, OldVector)] = {
    if (needSort) {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt, kvArr(1).toDouble)
          }.sortBy(_._1)
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      }
    } else {
      if (oneBased) {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt-1, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      } else {
        spark.sparkContext.textFile(dataPath, numPartitions).map{ line =>
          val strArr = line.split(extDelimiter)
          require(strArr.length > 1, s"require more than one elements but get ${line}")
          val kvs = strArr.slice(1, strArr.length).map{ kv =>
            val kvArr = kv.split(intDelimiter)
            require(kvArr.length > 1, s"index-value pair broken: ${kv}")
            (kvArr(0).toInt, kvArr(1).toDouble)
          }
          if (rescaleBinaryLabel) {
            ((strArr(0).toDouble+1)/2, OldVectors.sparse(numFeatures, kvs))
          } else {
            (strArr(0).toDouble, OldVectors.sparse(numFeatures, kvs))
          }
        }
      }
    }
  }

}
