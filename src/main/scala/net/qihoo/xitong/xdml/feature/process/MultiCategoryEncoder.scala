package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util.{DefaultParamsReadable, _}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait MultiCategoryEncoderBase extends Params
  with HasInputCols with HasOutputCols {

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  def setDelimiter(values: String): this.type = set(delimiter, values)

  final val delimiter: Param[String] = new Param[String](this, "delimiter", "Delimiter in column.")

  def setIndexOnly(values: Boolean): this.type = set(indexOnly, values)

  final val indexOnly: BooleanParam = new BooleanParam(this, "indexOnly", "If index only.")

  def setOutputSparse(values: Boolean): this.type = set(outputSparse, values)

  final val outputSparse: BooleanParam = new BooleanParam(this, "outputSparse", "Output Format: sparse or dense.")

  def setDropInputCols(values: Boolean): this.type = set(dropInputCols, values)

  final val dropInputCols: BooleanParam = new BooleanParam(this, "dropInputCols", "If drop input columns after transforming data.")

  def setCategoriesReserved(values: Int): this.type = set(categoriesReserved, values)

  final val categoriesReserved: IntParam = new IntParam(this, "CategoriesReserved", "Number of categories to be reserved.")

  setDefault(delimiter -> ",", indexOnly -> true, outputSparse -> true, dropInputCols -> false, categoriesReserved -> 0)

  def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)
    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be " +
        s"the same as the number of output columns ${outputColNames.length}.")
    inputColNames.foreach { inputColName =>
      require(schema.fieldNames.contains(inputColName) && (schema(inputColName).dataType == StringType),
        s" ${inputColName} should be found in schema fields and be type of string.")
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
    outputFields ++= outputColNames.map(outputColName => StructField(outputColName, new VectorUDT, true))
    StructType(outputFields.toArray)
  }

}


class MultiCategoryEncoder(override val uid: String)
  extends Estimator[MultiCategoryEncoderModel]
    with DefaultParamsWritable with MultiCategoryEncoderBase {

  def this() = this(Identifiable.randomUID("MultiCategoryEncoder"))

  override def fit(dataset: Dataset[_]): MultiCategoryEncoderModel = {
    transformSchema(dataset.schema, logging = true)
    val inputColNames = $(inputCols)
    val columns = $(inputCols).map { inputColName =>
      col(inputColName)
    }

    val initMaps = Array.fill(inputColNames.length)(mutable.HashMap[String, Int]())

    def seqOp(maps: Array[mutable.HashMap[String, Int]],
              row: Row): Array[mutable.HashMap[String, Int]] = {
      for (i <- maps.indices) {
        val map = maps(i)
        if (!row.isNullAt(i)) {
          val featArr = row.getString(i).split($(delimiter))
          featArr.foreach { feat =>
            if (map.contains(feat)) {
              map(feat) += 1
            } else {
              map(feat) = 1
            }
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

    val sortedRes = resMaps.map { map => map.toSeq.sortBy(-_._2).map(_._1) }

    val sortedResCut = if ($(categoriesReserved) > 0) {
      sortedRes.map(seq => seq.slice(0, $(categoriesReserved)))
    } else {
      sortedRes
    }

    val model = new MultiCategoryEncoderModel(uid, sortedResCut).setParent(this)
    copyValues(model)

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MultiCategoryEncoder = defaultCopy(extra)

}


object MultiCategoryEncoder extends DefaultParamsReadable[MultiCategoryEncoder] {
  override def load(path: String): MultiCategoryEncoder = super.load(path)
}


class MultiCategoryEncoderModel(override val uid: String, val labels: Array[Seq[String]])
  extends Model[MultiCategoryEncoderModel] with MultiCategoryEncoderBase with MLWritable {

  import MultiCategoryEncoderModel._

  def multiHotIndexing(dataset: Dataset[_],
                        fieldIndices: Array[Int],
                        fieldIndicesExt: Array[Int],
                        bcMaps: Broadcast[Array[mutable.HashMap[String, Int]]],
                        schemaNew: StructType): DataFrame = {
    val datasetPlus = dataset.toDF.rdd.map { row =>
      val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
      val rowSelectedIndexed = rowSelected.zip(bcMaps.value).map { case (any, map) =>
        if (any == null) {
          // TODO: Vector with no elements / null /  map.size
          null
        } else {
          val indices = any.toString.split($(delimiter)).map { item => map.getOrElse(item, map.size) }.distinct.sorted
          Vectors.dense(indices.map(_.toDouble))
        }
      }
      if ($(dropInputCols)) {
        val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
        Row.fromSeq(rowExt.toSeq ++ rowSelectedIndexed.toSeq)
      } else {
        Row.fromSeq(row.toSeq ++ rowSelectedIndexed.toSeq)
      }
    }
    dataset.sparkSession.createDataFrame(datasetPlus, schemaNew)
  }

  def multiHotEncoding(dataset: Dataset[_],
                        fieldIndices: Array[Int],
                        fieldIndicesExt: Array[Int],
                        bcMaps: Broadcast[Array[mutable.HashMap[String, Int]]],
                        schemaNew: StructType): DataFrame = {
    if ($(outputSparse)) {
      val datasetPlus = dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedIndexed = rowSelected.zip(bcMaps.value).map { case (any, map) =>
          if (any == null) {
            // TODO: Vector with no elements / null /  map.size
            null
          } else {
            val indices = any.toString.split($(delimiter)).map { item => map.getOrElse(item, map.size) }.distinct.sorted
            Vectors.sparse(map.size+1, indices, Array.fill(indices.length)(1.0))
          }
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedIndexed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedIndexed.toSeq)
        }
      }
      dataset.sparkSession.createDataFrame(datasetPlus, schemaNew)
    } else {
      val datasetPlus = dataset.toDF.rdd.map { row =>
        val rowSelected = fieldIndices.map { fieldIndex => row.get(fieldIndex) }
        val rowSelectedIndexed = rowSelected.zip(bcMaps.value).map { case (any, map) =>
          if (any == null) {
            // TODO: Vector with no elements / null /  map.size
            null
          } else {
            val values = new Array[Double](map.size+1)
            any.toString.split($(delimiter)).foreach { item =>
              val index = map.getOrElse(item, map.size)
              values(index) = 1.0
            }
            Vectors.dense(values)
          }
        }
        if ($(dropInputCols)) {
          val rowExt = fieldIndicesExt.map { fieldIndex => row.get(fieldIndex) }
          Row.fromSeq(rowExt.toSeq ++ rowSelectedIndexed.toSeq)
        } else {
          Row.fromSeq(row.toSeq ++ rowSelectedIndexed.toSeq)
        }
      }
      dataset.sparkSession.createDataFrame(datasetPlus, schemaNew)
    }
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
    val datasetTarget = if ($(indexOnly)) {
      multiHotIndexing(dataset, fieldIndices, fieldIndicesExt, mapsAtWorker, schemaNew)
    } else {
      multiHotEncoding(dataset, fieldIndices, fieldIndicesExt, mapsAtWorker, schemaNew)
    }
    datasetTarget
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    inputColNames.foreach { inputColName =>
      require(schema.fieldNames.contains(inputColName),
        s" ${inputColName} cannot be found in schema fields.")
    }
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MultiCategoryEncoderModel = {
    val copied = new MultiCategoryEncoderModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new MultiCategoryEncoderModelWriter(this)

}


object MultiCategoryEncoderModel extends MLReadable[MultiCategoryEncoderModel] {

  private[MultiCategoryEncoderModel]
  class MultiCategoryEncoderModelWriter(instance: MultiCategoryEncoderModel) extends MLWriter {

    private case class Data(labels: Array[Seq[String]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labels)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private
  class MultiCategoryEncoderModelReader extends MLReader[MultiCategoryEncoderModel] {

    private val className = classOf[MultiCategoryEncoderModel].getName

    override def load(path: String): MultiCategoryEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val row = sparkSession.read.parquet(dataPath).select("labels").head()
      val labels = row.getAs[Seq[Seq[String]]](0).toArray
      val model = new MultiCategoryEncoderModel(metadata.uid, labels)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

  override def read: MLReader[MultiCategoryEncoderModel] = new MultiCategoryEncoderModelReader

  override def load(path: String): MultiCategoryEncoderModel = super.load(path)
}