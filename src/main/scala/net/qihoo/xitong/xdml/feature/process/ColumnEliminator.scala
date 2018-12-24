package net.qihoo.xitong.xdml.feature.process

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer

class ColumnEliminator(override val uid: String)
  extends Transformer with HasInputCols with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ColumnEliminator"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.drop($(inputCols):_*).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    for (colName <- inputColNames) {
      if (!schema.fieldNames.contains(colName)) {
        logWarning("missing column " + colName)
      }
    }
    val fieldsEliminated = new ArrayBuffer[StructField]()
    for (structField <- schema.fields) {
      if (!inputColNames.contains(structField.name)) {
        fieldsEliminated += structField
      }
    }
    StructType(fieldsEliminated.toArray)
  }

  override def copy(extra: ParamMap): ColumnEliminator = defaultCopy(extra)

}


object ColumnEliminator extends DefaultParamsReadable[ColumnEliminator] {

  override def load(path: String): ColumnEliminator = super.load(path)
}

