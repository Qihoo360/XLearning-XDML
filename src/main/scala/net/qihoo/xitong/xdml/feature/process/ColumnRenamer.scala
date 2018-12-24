package net.qihoo.xitong.xdml.feature.process

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}

class ColumnRenamer(override val uid: String)
  extends Transformer with HasInputCols with HasOutputCols with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ColumnRenamer"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    var df = dataset
    val existingZipNewColNames = $(inputCols).zip($(outputCols))
    for(en <- existingZipNewColNames) {
      df = df.withColumnRenamed(en._1, en._2)
    }
    df.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val inputZipOutputColNames = $(inputCols).zip($(outputCols)).toMap
    StructType(schema.fields.map{ structField =>
      if (inputColNames.contains(structField.name)) {
        StructField(inputZipOutputColNames(structField.name), structField.dataType, structField.nullable)
      } else {
        structField
      }
    })
  }

  override def copy(extra: ParamMap): ColumnRenamer = defaultCopy(extra)

}

object ColumnRenamer extends DefaultParamsReadable[ColumnRenamer] {

  override def load(path: String): ColumnRenamer = super.load(path)
}
