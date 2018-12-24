package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


class NumericalStatistics extends Serializable {

  // non-null count
  var n: Long = 0L
  // central moments of order 1
  var M1: Double = 0.0
  // central moments of order 2
  var M2: Double = 0.0

  def countForNotNull(): Long = n

  def mean(): Double = M1

  def biasedVar(): Double = M2/n

  def unbiasedVar(): Double = M2/(n-1)

  def biasedStd(): Double = math.sqrt(biasedVar())

  def unbiasedStd(): Double = math.sqrt(unbiasedVar())

  def invert(x: Double): NumericalStatistics = {
    val n_org = n
    n += 1
    val delta = x - M1
    val delta_n = delta / n
    val term1 = delta * delta_n * n_org
    M1 += delta_n
    M2 += term1
    this
  }

  def merge(ns: NumericalStatistics): NumericalStatistics = {
    val combined = new NumericalStatistics
    combined.n = n + ns.n
    val delta = ns.M1 - M1
    val delta2 = delta * delta
    val na = 1.0 * n / combined.n
    val nb = 1.0 * ns.n / combined.n
    combined.M1 = na * M1 + nb * ns.M1
    combined.M2 = M2 + ns.M2 + delta2 * na * ns.n
    combined
  }

}


trait NumericStandardizerBase extends Params
  with HasInputCols with HasOutputCols {

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  final val dropInputCols: BooleanParam = new BooleanParam(this, "dropInputCols", "If drop input columns after transforming data.")

  def setDropInputCols(values: Boolean): this.type = set(dropInputCols, values)

  setDefault(dropInputCols, false)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be " +
        s"the same as the number of output columns ${outputColNames.length}.")
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
    outputFields ++= outputColNames.map(name => StructField(name, DoubleType, true))
    StructType(outputFields.toArray)
  }

}


class NumericStandardizer(override val uid: String)
  extends Estimator[NumericStandardizerModel]
    with NumericStandardizerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("NumericStandardizer"))

  override def fit(dataset: Dataset[_]): NumericStandardizerModel = {

    transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val columns = inputColNames.map { inputColName =>
      col(inputColName)
    }

    val emptySummaries = Array.fill(inputColNames.length)(new NumericalStatistics)

    def seqOp(summaries: Array[NumericalStatistics], row: Row): Array[NumericalStatistics] = {
      var i = 0
      while (i < summaries.length) {
        if (!row.isNullAt(i)) {
          val v = row.getDouble(i)
          if (!v.isNaN) {
            summaries(i).invert(v)
          }
        }
        i += 1
      }
      summaries
    }

    def comOp(summaries1: Array[NumericalStatistics],
              summaries2: Array[NumericalStatistics]): Array[NumericalStatistics] = {
      summaries1.zip(summaries2).map { case (s1, s2) => s1.merge(s2) }
    }

    val summaries = dataset.select(columns: _*).rdd.treeAggregate(emptySummaries)(seqOp, comOp)
    val stds = summaries.map(summary => summary.unbiasedStd())
    val means = summaries.map(summary => summary.mean())
    val model = new NumericStandardizerModel(uid, stds.toSeq, means.toSeq).setParent(this)
    copyValues(model)

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): NumericStandardizer = defaultCopy(extra)

}


object NumericStandardizer extends DefaultParamsReadable[NumericStandardizer] {

  override def load(path: String): NumericStandardizer = super.load(path)

}


class NumericStandardizerModel(override val uid: String, val std: Seq[Double], val mean: Seq[Double])
  extends Model[NumericStandardizerModel] with NumericStandardizerBase with MLWritable {

  import NumericStandardizerModel._

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schemaNew = transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val fieldIndices = inputColNames.map { inputColName => dataset.schema.fieldIndex(inputColName) }
    val fieldIndicesExt = dataset.schema.indices.filterNot(ind => fieldIndices.contains(ind)).toArray
    val meanAndStd = mean.zip(std)
    val meanAndStdWorker = dataset.sparkSession.sparkContext.broadcast(meanAndStd)
    val datasetPlus = dataset.toDF.rdd.map { row =>
      val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
      val rowSelectedStandardized = rowSelected.zip(meanAndStdWorker.value).map { case (any, elem) =>
        if (any == null) {
          Double.NaN
        } else {
          (any.toString.toDouble - elem._1) / elem._2
        }
      }
      if ($(dropInputCols)) {
        val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
        Row.fromSeq(rowExt.toSeq ++ rowSelectedStandardized.toSeq)
      } else {
        Row.fromSeq(row.toSeq ++ rowSelectedStandardized.toSeq)
      }
    }
    dataset.sparkSession.createDataFrame(datasetPlus, schemaNew)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): NumericStandardizerModel = {
    val copied = new NumericStandardizerModel(uid, std, mean)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new NumericStandardizerModelWriter(this)

}


object NumericStandardizerModel extends MLReadable[NumericStandardizerModel] {

  private[NumericStandardizerModel]
  class NumericStandardizerModelWriter(instance: NumericStandardizerModel) extends MLWriter {

    private case class Data(std: Seq[Double], mean: Seq[Double])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.std, instance.mean)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }

  }

  private
  class NumericStandardizerModelReader extends MLReader[NumericStandardizerModel] {

    private val className = classOf[NumericStandardizerModel].getName

    override def load(path: String): NumericStandardizerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(std: Seq[Double], mean: Seq[Double]) = data.select("std", "mean").head()
      val model = new NumericStandardizerModel(metadata.uid, std, mean)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

  override def read: MLReader[NumericStandardizerModel] = new NumericStandardizerModelReader

  override def load(path: String): NumericStandardizerModel = super.load(path)
}