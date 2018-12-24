package net.qihoo.xitong.xdml.model.data

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

import scala.io.Source


class SchemaHandler(val schema: StructType, val namesAndTypes: Array[Array[String]], checkLabel: Boolean = false) extends Serializable {

  val keyColName: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Key)).map(x => x(0))
  val labelColName: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Label)).map(x => x(0))
  val featColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Cat)
                                                            || x(1).equals(SchemaHandler.Num)
                                                            || x(1).equals(SchemaHandler.Text)
                                                            || x(1).equals(SchemaHandler.MultiCat)).map(x => x(0))
  val catFeatColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Cat)).map(x => x(0))
  val multiCatFeatColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.MultiCat)).map(x => x(0))
  val numFeatColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Num)).map(x => x(0))
  val textFeatColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Text)).map(x => x(0))
  val otherColNames: Array[String] = namesAndTypes.filter(x => x(1).equals(SchemaHandler.Other)).map(x => x(0))
  if (checkLabel) assert(labelColName.length == 1, "one and only one label needs to be provided")

}


object SchemaHandler extends Serializable {

  val Text = "Text"
  val Num = "Num"
  val Cat = "Cat"
  val MultiCat = "MultiCat"
  val Key = "Key"
  val Label = "Label"
  val Other = "Other"

  def readSchema(sc: SparkContext, schemaPath: String, delimiter: String): SchemaHandler = {
    val namesAndTypes = if(schemaPath.startsWith("jar://")) {
      val jarPath = schemaPath.substring("jar://".length())
      val stream = getClass().getClassLoader().getResourceAsStream(jarPath)
      val lines = Source.fromInputStream(stream).getLines
      lines.toArray.map(line => line.split(delimiter))
    } else {
      sc.textFile(schemaPath, 1).collect().map(line => line.split(delimiter))
    }
    val sfArr = namesAndTypes.map(arr => {
      arr(1) match {
        case Num => StructField(arr(0), DoubleType, true)
        case Cat => StructField(arr(0), StringType, true)
        case MultiCat => StructField(arr(0), StringType, true)
        case Text => StructField(arr(0), StringType, true)
        case Key => StructField(arr(0), StringType, true)
        case Label => StructField(arr(0), StringType, true)
        case Other => StructField(arr(0), StringType, true)
        case _ => throw new IllegalArgumentException("data type unknown")
      }
    })
    val schema = StructType(sfArr)
    new SchemaHandler(schema, namesAndTypes)
  }

}


