package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, _}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.{util => ju}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}

import scala.collection.mutable.ArrayBuffer


trait NumericBucketerBase extends Params
  with HasInputCols with HasOutputCols {

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  final val dropInputCols: BooleanParam = new BooleanParam(this, "dropInputCols", "If drop input columns after transforming data.")

  def setDropInputCols(value: Boolean): this.type = set(dropInputCols, value)

  setDefault(dropInputCols, false)

  final val numBucketsArray: IntArrayParam = new IntArrayParam(this, "numBucketsArray", "Number of buckets for each feature",(arrayOfNumBuckets: Array[Int]) => arrayOfNumBuckets.forall(ParamValidators.gtEq(2)))

  def setNumBucketsArray(values: Array[Int]): this.type = set(numBucketsArray, values)

  setDefault(numBucketsArray -> Array())

  final val relativeError: DoubleParam = new DoubleParam(this, "relativeError", "Relative error")

  def setRelativeError(value: Double): this.type = set(relativeError, value)

  setDefault(relativeError, 0.0005)

  final val indexOnly: BooleanParam = new BooleanParam(this, "indexOnly", "If index only.")

  def setIndexOnly(value: Boolean): this.type = set(indexOnly, value)

  setDefault(indexOnly, true)

  final val outputSparse: BooleanParam = new BooleanParam(this, "outputSparse", "Output Format: sparse or dense.")

  def setOutputSparse(value: Boolean): this.type = set(outputSparse, value)

  setDefault(outputSparse, true)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be " +
        s"the same as the number of output columns ${outputColNames.length}.")
    require(${numBucketsArray}.length > 0,s"numBucketsArray is empty, please set numBucketsArray")
    require(inputColNames.length == $(numBucketsArray).length,
      s"The number of input columns ${inputColNames.length} must be " +
      s"the same as the number of BucketsArray columns ${${numBucketsArray}.length}")
    inputColNames.foreach { inputColName =>
      val inputDataType = schema(inputColName).dataType
      require(inputDataType.isInstanceOf[NumericType],
        s"The input column $inputColName must be numeric type, but got $inputDataType.")
    }
    outputColNames.foreach { outputColName =>
      require(!schema.fieldNames.contains(outputColName), s"Output column $outputColName already exists.")
    }
    val outputFields = new ArrayBuffer[StructField]()
    if ($(dropInputCols)) {
      for (structField <- schema.fields) {
        if (!inputColNames.contains(structField.name)) {
          outputFields += structField
        }
      }
    } else {
      outputFields ++= schema.fields
    }
    if($(indexOnly)) {
      outputFields ++= outputColNames.map(name => StructField(name, IntegerType, true))
    } else {
      outputFields ++= outputColNames.map(name => StructField(name, new VectorUDT, true))
    }
    StructType(outputFields.toArray)
  }
}


class NumericBucketer(override val uid: String)
  extends Estimator[NumericBucketerModel]
    with NumericBucketerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("NumericBucketer"))

  private def getDistinctSplits(splits: Array[Double]): Array[Double] = {
    splits(0) = Double.NegativeInfinity
    splits(splits.length - 1) = Double.PositiveInfinity
    val distinctSplits = splits.distinct
    if (splits.length != distinctSplits.length) {
      log.warn(s"Some quantiles were identical. Bucketing to ${distinctSplits.length - 1}" +
        s" buckets as a result.")
      println(s"Some quantiles were identical. Bucketing to ${distinctSplits.length - 1}" +
        s" buckets as a result.")
    }
    distinctSplits.sorted
  }

  override def fit(dataset: Dataset[_]): NumericBucketerModel = {

    transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val columns = inputColNames.map { inputColName =>
      col(inputColName).cast(DoubleType)
    }

    val emptySummaries = Array.fill(inputColNames.length)(
      new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, $(relativeError)))

    def seqOp(summaries: Array[QuantileSummaries], row: Row): Array[QuantileSummaries] = {
      var i = 0
      while (i < summaries.length) {
        if (!row.isNullAt(i)) {
          val v = row.getDouble(i)
          if (!v.isNaN) summaries(i) = summaries(i).insert(v)
        }
        i += 1
      }
      summaries
    }

    def comOp(summaries1: Array[QuantileSummaries],
              summaries2: Array[QuantileSummaries]): Array[QuantileSummaries] = {
      summaries1.zip(summaries2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
    }

    val summaries = dataset.select(columns: _*).rdd.treeAggregate(emptySummaries)(seqOp, comOp)

    val arrProbArr = $(numBucketsArray).map(numBuckets => (0.0 to 1.0 by 1.0 / numBuckets).toArray)

    val arrSplitArr = arrProbArr.zip(summaries)
      .map{ case (probArr, summary) => getDistinctSplits(probArr.flatMap(summary.query)).toSeq }.toSeq

    val model = new NumericBucketerModel(uid, arrSplitArr).setParent(this)
    copyValues(model)

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): NumericBucketer = defaultCopy(extra)

}


object NumericBucketer extends DefaultParamsReadable[NumericBucketer] {

  override def load(path: String): NumericBucketer = super.load(path)

}


class NumericBucketerModel(override val uid: String, val arrSplitArr: Seq[Seq[Double]])
  extends Model[NumericBucketerModel] with NumericBucketerBase with MLWritable {

  import NumericBucketerModel._

  def getSplitsArray: Array[Array[Double]] = arrSplitArr.map{ split => split.toArray}.toArray

  protected def binarySearchForBuckets(splits: Array[Double], feature: Double, keepInvalid: Boolean): Double = {
    if (feature.isNaN) {
      splits.length - 1
    } else if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  protected def getBucketNumber(splitArr: Array[Double], feature: Any, keepInvalid: Boolean): Double = {
    if (feature.isInstanceOf[Int]) {
      binarySearchForBuckets(splitArr, feature.asInstanceOf[Int], true)
    } else if (feature.isInstanceOf[Long]) {
      binarySearchForBuckets(splitArr, feature.asInstanceOf[Long], true)
    } else {
      binarySearchForBuckets(splitArr, feature.asInstanceOf[Double], true)
    }
  }

  protected def bucketIndexing(dataset: Dataset[_],
                               arrSplitArr: Broadcast[Seq[Seq[Double]]],
                               fieldIndices: Array[Int],
                               fieldIndicesExt: Array[Int],
                               schemaNew: StructType): DataFrame = {
    val dataBinned = dataset.toDF.rdd.map { row =>
      val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
      val rowSelectedBucketed = rowSelected.zip(arrSplitArr.value).map { case (elem, splitArr) =>
        getBucketNumber(splitArr.toArray, elem, true).toInt
      }
      if ($(dropInputCols)) {
        val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
        Row.fromSeq(rowExt.toSeq ++ rowSelectedBucketed.toSeq)
      } else {
        Row.fromSeq(row.toSeq ++ rowSelectedBucketed.toSeq)
      }
    }
    dataset.sparkSession.createDataFrame(dataBinned, schemaNew)
  }

  protected def bucketEncoding(dataset: Dataset[_],
                               arrSplitArr: Broadcast[Seq[Seq[Double]]],
                               fieldIndices: Array[Int],
                               fieldIndicesExt: Array[Int],
                               schemaNew: StructType): DataFrame = {
    val dataBinned = if ($(outputSparse)) {
      dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedBucketed = rowSelected.zip(arrSplitArr.value).map { case (elem, splitArr) =>
          val binnedNumber = getBucketNumber(splitArr.toArray, elem, true)
          val indices = Array(binnedNumber.toInt)
          val values = Array(1.0)
          Vectors.sparse(splitArr.length, indices, values)
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedBucketed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedBucketed.toSeq)
        }
      }
    } else {
      dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedBucketed = rowSelected.zip(arrSplitArr.value).map { case (elem, splitArr) =>
          val binnedNumber = getBucketNumber(splitArr.toArray, elem, true)
          val values = new Array[Double](splitArr.length)
          values(binnedNumber.toInt) = 1.0
          Vectors.dense(values)
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedBucketed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedBucketed.toSeq)
        }
      }
    }
    dataset.sparkSession.createDataFrame(dataBinned, schemaNew)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schemaNew = transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val fieldIndices = inputColNames.map { inputColName => dataset.schema.fieldIndex(inputColName) }
    val fieldIndicesExt = dataset.schema.indices.filterNot(ind => fieldIndices.contains(ind)).toArray
    val bcArrSplitArr = dataset.sparkSession.sparkContext.broadcast(arrSplitArr)
    if ($(indexOnly)) {
       bucketIndexing(dataset, bcArrSplitArr, fieldIndices, fieldIndicesExt, schemaNew)
    } else {
       bucketEncoding(dataset, bcArrSplitArr, fieldIndices, fieldIndicesExt, schemaNew)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): NumericBucketerModel = {
    val copied = new NumericBucketerModel(uid, arrSplitArr)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new NumericBucketerModelWriter(this)

}

object NumericBucketerModel extends MLReadable[NumericBucketerModel] {

  private[NumericBucketerModel]
  class NumericBucketerModelWriter(instance: NumericBucketerModel) extends MLWriter {

    private case class Data(arrSplitArr: Seq[Seq[Double]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.arrSplitArr)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }

  }

  private
  class NumericBucketerModelReader extends MLReader[NumericBucketerModel] {

    private val className = classOf[NumericBucketerModel].getName

    override def load(path: String): NumericBucketerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(arrSplitArr: Seq[Seq[Double]]) = data.select("arrSplitArr").head()
      val model = new NumericBucketerModel(metadata.uid, arrSplitArr)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

  override def read: MLReader[NumericBucketerModel] = new NumericBucketerModelReader

  override def load(path: String): NumericBucketerModel = super.load(path)
}