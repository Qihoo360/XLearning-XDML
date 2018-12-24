package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait CategoryEncoderBase extends Params
  with HasInputCols with HasOutputCols {

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  final val indexOnly: BooleanParam = new BooleanParam(this, "indexOnly", "If index only.")

  def setIndexOnly(values: Boolean): this.type = set(indexOnly, values)

  setDefault(indexOnly, true)

  final val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "Unseen category / NULL handling. " +
      "Supported Options are 'error' (throw an error) or 'keep' (put invalid data in a additional bucket).",
    ParamValidators.inArray(CategoryEncoder.supportedHandleInvalids))

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  setDefault(handleInvalid, CategoryEncoder.KEEP_INVALID)

  final val stringOrderType: Param[String] = new Param[String](this, "stringOrderType",
    "Ordering of indexing strings. Index starts from 0. " +
      s"Supported options: ${CategoryEncoder.supportedStringOrderTypes.mkString(", ")}.",
    ParamValidators.inArray(CategoryEncoder.supportedStringOrderTypes))

  def setStringOrderType(value: String): this.type = set(stringOrderType, value)

  setDefault(stringOrderType, CategoryEncoder.frequencyDesc)

  final val dropInputCols: BooleanParam = new BooleanParam(this, "dropInputCols", "If drop input columns after transforming data.")

  def setDropInputCols(values: Boolean): this.type = set(dropInputCols, values)

  setDefault(dropInputCols, false)

  final val categoriesReserved: IntParam = new IntParam(this, "CategoriesReserved", "Number of categories to be reserved.")

  def setCategoriesReserved(values: Int): this.type = set(categoriesReserved, values)

  setDefault(categoriesReserved, 0)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be " +
        s"the same as the number of output columns ${outputColNames.length}.")
    inputColNames.foreach { inputColName =>
      val inputDataType = schema(inputColName).dataType
      require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
        s"The input column $inputColName must be either string type or numeric type, but got $inputDataType.")
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
    val outputFieldsPlus = if ($(indexOnly)) {
      outputColNames.map(name => StructField(name, IntegerType, true))
    } else {
      outputColNames.map(name => StructField(name, new VectorUDT, true))
    }
    outputFields ++= outputFieldsPlus
    StructType(outputFields.toArray)
  }

}


class CategoryEncoder(override val uid: String)
  extends Estimator[CategoryEncoderModel]
    with DefaultParamsWritable with CategoryEncoderBase {

  def this() = this(Identifiable.randomUID("CategoryEncoder"))

  override def fit(dataset: Dataset[_]): CategoryEncoderModel = {

    transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val columns = inputColNames.map { inputColName =>
      col(inputColName).cast(StringType)
    }

    val initMaps = Array.fill(inputColNames.length)(mutable.HashMap[String, Int]())

    def seqOp(maps: Array[mutable.HashMap[String, Int]],
              row: Row): Array[mutable.HashMap[String, Int]] = {
      for (i <- maps.indices) {
        val map = maps(i)
        if (!row.isNullAt(i)) {
          val feat = row.getString(i)
          if (map.contains(feat)) {
            map(feat) += 1
          } else {
            map(feat) = 1
          }
        }
      }
      maps
    }

    def comOp(maps1: Array[mutable.HashMap[String, Int]],
              maps2: Array[mutable.HashMap[String, Int]]): Array[mutable.HashMap[String, Int]] = {
      maps1.zip(maps2).foreach {
        case (map1, map2) => {
          map2.foreach { case (k, v) => if (map1.contains(k)) map1(k) += v else map1(k) = v }
        }
      }
      maps1
    }

    val resMaps = dataset.select(columns: _*).rdd.treeAggregate(initMaps)(seqOp, comOp)

    //    resMaps.foreach{ map => println(map) }

    val sortedRes = $(stringOrderType) match {
      case CategoryEncoder.frequencyDesc => resMaps.map { map => map.toSeq.sortBy(-_._2).map(_._1) }
      case CategoryEncoder.frequencyAsc => resMaps.map { map => map.toSeq.sortBy(_._2).map(_._1) }
      case CategoryEncoder.alphabetDesc => resMaps.map { map => map.toSeq.sortWith(_._1 > _._1).map(_._1) }
      case CategoryEncoder.alphabetAsc => resMaps.map { map => map.toSeq.sortWith(_._1 < _._1).map(_._1) }
    }

    val sortedResCut = if ($(categoriesReserved) > 0) {
      sortedRes.map(seq => seq.slice(0, $(categoriesReserved)))
    } else {
      sortedRes
    }

    val model = new CategoryEncoderModel(uid, sortedResCut).setParent(this)
    copyValues(model)

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): CategoryEncoder = defaultCopy(extra)

}


object CategoryEncoder extends DefaultParamsReadable[CategoryEncoder] {

  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(ERROR_INVALID, KEEP_INVALID)
  private[feature] val frequencyDesc: String = "frequencyDesc"
  private[feature] val frequencyAsc: String = "frequencyAsc"
  private[feature] val alphabetDesc: String = "alphabetDesc"
  private[feature] val alphabetAsc: String = "alphabetAsc"
  private[feature] val supportedStringOrderTypes: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)

  override def load(path: String): CategoryEncoder = super.load(path)

}


class CategoryEncoderModel(override val uid: String, val labels: Array[Seq[String]])
  extends Model[CategoryEncoderModel] with CategoryEncoderBase with MLWritable {

  import CategoryEncoderModel._

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    inputColNames.foreach { inputColName =>
      require(schema.fieldNames.contains(inputColName),
        s" ${inputColName} cannot be found in schema fields.")
    }
    validateAndTransformSchema(schema)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schemaNew = transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val fieldIndices = inputColNames.map { inputColName => dataset.schema.fieldIndex(inputColName) }
    val fieldIndicesExt = dataset.schema.indices.filterNot(ind => fieldIndices.contains(ind)).toArray
    val maps = labels.map { seq =>
      val map = new mutable.HashMap[String, Int]()
      seq.zipWithIndex.foreach { case (str, ind) => map(str) = ind }
      map
    }
    val mapsAtWorker = dataset.sparkSession.sparkContext.broadcast(maps)
    // TODO: ERROR_INVALID
    val datasetPlus = if ($(indexOnly)) {
      dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedIndexed = rowSelected.zip(mapsAtWorker.value).map { case (any, map) =>
          if (any == null) {
            map.size
          } else {
            map.getOrElse(any.toString, map.size)
          }
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedIndexed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedIndexed.toSeq)
        }
      }
    } else {
      dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedIndexed = rowSelected.zip(mapsAtWorker.value).map { case (any, map) =>
          if (any == null) {
            Vectors.sparse(map.size+1, Array(map.size), Array(1.0))
          } else {
            Vectors.sparse(map.size+1, Array(map.getOrElse(any.toString, map.size)), Array(1.0))
          }
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedIndexed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedIndexed.toSeq)
        }
      }
    }
    dataset.sparkSession.createDataFrame(datasetPlus, schemaNew)
  }

  override def copy(extra: ParamMap): CategoryEncoderModel = {
    val copied = new CategoryEncoderModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new CategoryEncoderModelWriter(this)

}


object CategoryEncoderModel extends MLReadable[CategoryEncoderModel] {

  private[CategoryEncoderModel]
  class CategoryEncoderModelWriter(instance: CategoryEncoderModel) extends MLWriter {

    private case class Data(labels: Array[Seq[String]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labels)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }

  }

  private
  class CategoryEncoderModelReader extends MLReader[CategoryEncoderModel] {

    private val className = classOf[CategoryEncoderModel].getName

    override def load(path: String): CategoryEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val row = sparkSession.read.parquet(dataPath).select("labels").head()
      val labels = row.getAs[Seq[Seq[String]]](0).toArray
      val model = new CategoryEncoderModel(metadata.uid, labels)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

  override def read: MLReader[CategoryEncoderModel] = new CategoryEncoderModelReader

  override def load(path: String): CategoryEncoderModel = super.load(path)

}