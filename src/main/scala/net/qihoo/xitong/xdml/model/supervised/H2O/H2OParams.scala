package org.apache.spark.ml.model.supervised

import org.apache.spark.ml.param._
import org.apache.spark.sql.types.{StringType, StructType}


trait H2OParams extends Params {

  final val labelCol: Param[String] = new Param[String](this, "labelCol", "Label column name")

  final val catFeatColNames: StringArrayParam = new StringArrayParam(this, "catFeatColNames","Categorical feature column names")

  final val ignoredFeatColNames: StringArrayParam = new StringArrayParam(this, "ignoredFeatColNames", "Ignored feature column names")

  protected def validateSchema(schema: StructType): StructType={
    ${catFeatColNames}.foreach { catFeatName => require(schema.fieldNames.contains(catFeatName), s"Column $catFeatName cannot find in dataFrame.") }
    ${catFeatColNames}.foreach { catFeatName => require(schema(catFeatName).dataType == StringType, s"The categorical feature column $catFeatName must be string type.")}
    schema
  }
}

trait H2OTreeParams extends H2OParams{

  final val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "Maximum depth of the tree. (>= 0)" +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.",
      ParamValidators.gtEq(0))

  final val numTrees: IntParam = new IntParam(this, "numTrees", "Number of trees to train (>= 1)",
    ParamValidators.gtEq(1))

  final val maxBinsForCat: IntParam = new IntParam(this, "maxBinsForCat", "Max number of bins for" +
    " category features.  Must be >=2 and >= number of categories for any" +
    " categorical feature.", ParamValidators.gtEq(2))

  final val maxBinsForNum: IntParam = new IntParam(this, "maxBinsForNum", "Max number of bins for" +
    " numeric features.  Must be >=2 and >= number of category for any" +
    " numeric feature.", ParamValidators.gtEq(2))

  final val minInstancesPerNode: IntParam = new IntParam(this, "minInstancesPerNode", "Minimum" +
    " number of instances each child must have after split. If a split causes the left or right" +
    " child to have fewer than minInstancesPerNode, the split will be discarded as invalid." +
    " Should be >= 1.", ParamValidators.gtEq(1))

  final val categoricalEncodingScheme: Param[String] = new Param[String](this, "categoricalEncodingScheme", "Encoding scheme " +
    "for categorical features. Supported options: " + H2OTreeParams.supportedCategoricalEncoding.mkString(", "),
    ParamValidators.inArray(H2OTreeParams.supportedCategoricalEncoding))

  final val histogramType: Param[String] = new Param[String](this, "histogramType"," Type of histogram. Supported options: " +
    H2OTreeParams.supportedHistogramType.mkString(", "), ParamValidators.inArray(H2OTreeParams.supportedHistogramType))

  final val distribution: Param[String] = new Param[String](this, "distribution", "Distribution for dataSet. Supported options: " +
    H2OTreeParams.supportedDistribution.mkString(", "), ParamValidators.inArray(H2OTreeParams.supportedDistribution))

  final val scoreTreeInterval: IntParam = new IntParam (this, "scoreTreeInterval", "Score the model after every so many trees." +
    " Disabled if set to 0.", ParamValidators.gtEq(0))
}

object H2OTreeParams{

  final val supportedCategoricalEncoding: Array[String] = Array("AUTO", "Enum", "LabelEncoder", "OneHotExplicit", "SortByResponse")

  final val supportedHistogramType: Array[String] = Array("AUTO", "UniformAdaptive", "QuantilesGlobal")

  final val supportedDistribution: Array[String] = Array("bernoulli", "multinomial", "gaussian")
}

trait H2OTreeEstimatorParams extends H2OTreeParams{

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setCatFeatColNames(values: Array[String]): this.type = set(catFeatColNames, values)

  def setIgnoreFeatColNames(values: Array[String]): this.type = set(ignoredFeatColNames, values)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  def setMaxBinsForCat(value: Int): this.type = set(maxBinsForCat, value)

  def setMaxBinsForNum(value: Int): this.type = set(maxBinsForNum, value)

  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  def setCategoricalEncodingScheme(value: String): this.type = set(categoricalEncodingScheme, value)

  def setHistogramType(value: String): this.type = set(histogramType, value)

  def setDistribution(value: String): this.type = set(distribution, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  setDefault(catFeatColNames -> Array(), ignoredFeatColNames -> Array())
}


