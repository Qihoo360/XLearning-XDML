package org.apache.spark.ml.model.supervised

import _root_.hex.genmodel.utils.DistributionFamily
import hex.Model.Parameters.CategoricalEncodingScheme
import hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import org.apache.hadoop.fs.Path
import org.apache.spark.h2o._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import water.support.ModelSerializationSupport

import scala.collection.mutable.ArrayBuffer


class H2ODRF (override val uid: String) extends Estimator[H2ODRFModel] with H2OTreeEstimatorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("H2ODRF"))

  setDefault(maxDepth -> 20, numTrees -> 50, maxBinsForCat -> 256, maxBinsForNum -> 20, minInstancesPerNode -> 1, categoricalEncodingScheme -> "Enum",
    histogramType -> "QuantilesGlobal", distribution -> "bernoulli", scoreTreeInterval -> 0)

  //TODO: cast label column from string to double may cause mass data manipulation
  //Warning: response missing will cause prediction failed.( for example , in regression problem, predict failed when label is categorical .)
  private def convertLabelColumnType(data: DataFrame): DataFrame={
    // convert label to double in regression problem.
    if($(distribution).equals("gaussian")) {
      val selectedCols = data.schema.fieldNames.map(colName =>
        if (colName.equals($ {
          labelCol
        })) {
          col(colName).cast(DoubleType)
        } else {
          col(colName)
        }
      )
      data.select(selectedCols: _*)
    }else{
      data
    }
  }

  private def constructDRFParameters(trainData: DataFrame, validData: DataFrame): DRFParameters={
    val h2oContext = H2OContext.getOrCreate(trainData.sparkSession)
    import h2oContext.implicits._
    val trainDataH2O: H2OFrame = trainData
    val validDataH2O: H2OFrame = if (validData == null) null else validData
    val drfParameters = new DRFParameters()
    val enumColNames = new ArrayBuffer[String]()
    if (!${distribution}.equals("gaussian")){
      enumColNames += ${labelCol}
    }
    ${catFeatColNames}.map(catFeatName => enumColNames += catFeatName)
    val enumColNamesArr = enumColNames.toArray
    trainDataH2O.colToEnum(enumColNamesArr)
    drfParameters._train = trainDataH2O
    if(validData != null){
      validDataH2O.colToEnum(enumColNamesArr)
      drfParameters._valid = validDataH2O
    }
    drfParameters._response_column = ${labelCol}
    drfParameters._ignored_columns = ${ignoredFeatColNames}
    drfParameters._max_depth = ${maxDepth}
    drfParameters._ntrees = ${numTrees}
    drfParameters._nbins_cats = ${maxBinsForCat}
    drfParameters._nbins = ${maxBinsForNum}
    drfParameters._min_rows = ${minInstancesPerNode}.toDouble
    drfParameters._categorical_encoding = ${categoricalEncodingScheme} match {
      case "AUTO" => CategoricalEncodingScheme.AUTO
      case "Enum" => CategoricalEncodingScheme.Enum
      case "LabelEncoder" => CategoricalEncodingScheme.LabelEncoder
      case "OneHotExplicit" => CategoricalEncodingScheme.OneHotExplicit
      case "SortByResponse" => CategoricalEncodingScheme.SortByResponse
      case _ => throw new IllegalArgumentException("not implemented")
    }
    drfParameters._histogram_type = ${histogramType} match {
      case "AUTO" => HistogramType.AUTO
      case "UniformAdaptive" => HistogramType.UniformAdaptive
      case "QuantilesGlobal" => HistogramType.QuantilesGlobal
      case _ => throw new IllegalArgumentException("not implemented")
    }
    drfParameters._distribution = ${distribution} match {
      case "bernoulli" => DistributionFamily.bernoulli
      case "multinomial" => DistributionFamily.multinomial
      case "gaussian" => DistributionFamily.gaussian
    }
    drfParameters._score_tree_interval = ${scoreTreeInterval}
    drfParameters
  }

  private def train(trainData: DataFrame, validData: DataFrame): DRFModel={
    val drfParameters = constructDRFParameters(trainData, validData)
    val drf = new DRF(drfParameters)
    val drfModel = drf.trainModel.get
    drfModel
  }

  override def fit(dataset: Dataset[_]): H2ODRFModel = {
    transformSchema(dataset.schema)
    val data = convertLabelColumnType(dataset.toDF())
    val drfModel = train(data, null)
    val model = new H2ODRFModel(uid, drfModel)
    copyValues(model)
  }

  def fit(trainDataSet: Dataset[_], validDataSet: Dataset[_]): H2ODRFModel = {
    transformSchema(trainDataSet.schema)
    val trainData = convertLabelColumnType(trainDataSet.toDF())
    val validData =
      if (validDataSet != null) {
        transformSchema(validDataSet.schema)
        convertLabelColumnType(validDataSet.toDF())
      } else {
        null
      }
    val drfModel = train(trainData, validData)
    val model = new H2ODRFModel(uid, drfModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains(${labelCol}), "Cannot find label column '%s' in DataFrame.".format(${labelCol}))
    validateSchema(schema)
  }

  override def copy(extra: ParamMap): H2ODRF = defaultCopy(extra)
}

object H2ODRF extends DefaultParamsReadable[H2ODRF] {
  override def load(path: String): H2ODRF = super.load(path)
}

class H2ODRFModel(override val uid: String, val model: DRFModel) extends Model[H2ODRFModel] with H2OTreeParams
with MLWritable{

  import H2ODRFModel._

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val h2oContext = H2OContext.getOrCreate(dataset.sparkSession)
    import h2oContext.implicits._
    h2oContext.asDataFrame(model.score(dataset.toDF()))(dataset.sqlContext)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    val numOfClasses = model._output.nclasses()
    val outputFields = new ArrayBuffer[StructField]()
    outputFields += StructField("predict", DoubleType, false)
    if(!${distribution}.equals("gaussian")) {
      (0 until numOfClasses).foreach { classIndex =>
        val colName = "p" + classIndex
        outputFields += StructField(colName, DoubleType, false)
      }
    }
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): H2ODRFModel = {
    val coiped = new H2ODRFModel(uid, model)
    copyValues(coiped, extra).setParent(parent)
  }

  override def write: MLWriter = new H2ODRFModelWriter(this)
}

object H2ODRFModel extends MLReadable[H2ODRFModel] {

  private[H2ODRFModel]
  class H2ODRFModelWriter(instance: H2ODRFModel) extends MLWriter{

    override protected def saveImpl(path: String): Unit={
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val modelPath = new Path(path, "model").toString
      instance.model.exportBinaryModel(modelPath, true)
    }
  }

  private[H2ODRFModel]
  class H2ODRFModelReader extends MLReader[H2ODRFModel] {

    private val className = classOf[H2ODRFModel].getName

    override  def load(path: String): H2ODRFModel={
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "model").toString
      val drfModel =  ModelSerializationSupport.loadH2OModel(dataPath).asInstanceOf[DRFModel]
      val model = new H2ODRFModel(metadata.uid, drfModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[H2ODRFModel] = new H2ODRFModelReader

  override def load(path: String):  H2ODRFModel= super.load(path)
}