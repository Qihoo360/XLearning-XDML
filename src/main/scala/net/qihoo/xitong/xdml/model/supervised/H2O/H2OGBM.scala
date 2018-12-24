package org.apache.spark.ml.model.supervised

import hex.Model.Parameters.CategoricalEncodingScheme
import hex.genmodel.utils.DistributionFamily
import hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
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


trait H2OGBMParams extends Params{

  final val learnRate: DoubleParam = new DoubleParam(this, "learnRate", "Learn rate to be used for each iteration of optimization (> 0)",
    ParamValidators.inRange(0,1,false,true))

  final val learnRateAnnealing: DoubleParam = new DoubleParam(this, "learnRateAnnealing", "Learn rate annealing for each iteration of optimization (>0)",
    ParamValidators.inRange(0,1,false,true))

}

class H2OGBM(override val uid: String) extends Estimator[H2OGBMModel] with H2OTreeEstimatorParams with H2OGBMParams  with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("H2OGBM"))

  def setLearnRate(value: Double): this.type = set(learnRate, value)

  def setLearnRateAnnealing(value: Double): this.type = set(learnRateAnnealing, value)

  setDefault(maxDepth -> 5, numTrees -> 50, maxBinsForCat -> 256, maxBinsForNum -> 20, minInstancesPerNode -> 10, categoricalEncodingScheme -> "Enum",
    histogramType -> "QuantilesGlobal", learnRate -> 0.1, learnRateAnnealing -> 1, distribution -> "bernoulli", scoreTreeInterval -> 0)

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

  private def constructGBMParameters(trainData: DataFrame, validData: DataFrame): GBMParameters ={
    val h2oContext = H2OContext.getOrCreate(trainData.sparkSession)
    import h2oContext.implicits._
    val trainDataH2O: H2OFrame = trainData
    val validDataH2O: H2OFrame = if (validData == null) null else validData
    val gbmParameters = new GBMParameters()
    val enumColNames = new ArrayBuffer[String]()
    if (!${distribution}.equals("gaussian")) {
      enumColNames += ${labelCol}
    }
    ${catFeatColNames}.foreach(catFeatName => enumColNames += catFeatName)
    val enumColNamesArr = enumColNames.toArray
    trainDataH2O.colToEnum(enumColNamesArr)
    gbmParameters._train = trainDataH2O
    if (validData != null) {
      validDataH2O.colToEnum(enumColNamesArr)
      gbmParameters._valid = validDataH2O
    }
    gbmParameters._response_column = ${labelCol}
    gbmParameters._ignored_columns = ${ignoredFeatColNames}
    gbmParameters._max_depth = ${maxDepth}
    gbmParameters._ntrees = ${numTrees}
    gbmParameters._nbins_cats = ${maxBinsForCat}
    gbmParameters._nbins = ${maxBinsForNum}
    gbmParameters._min_rows = ${minInstancesPerNode}.toDouble
    gbmParameters._categorical_encoding = ${categoricalEncodingScheme} match {
      case "AUTO" => CategoricalEncodingScheme.AUTO
      case "Enum" => CategoricalEncodingScheme.Enum
      case "LabelEncoder" => CategoricalEncodingScheme.LabelEncoder
      case "OneHotExplicit" => CategoricalEncodingScheme.OneHotExplicit
      case "SortByResponse" => CategoricalEncodingScheme.SortByResponse
      case _ => throw new IllegalArgumentException("not implemented")
    }
    gbmParameters._histogram_type = ${histogramType} match {
      case "AUTO" => HistogramType.AUTO
      case "UniformAdaptive" => HistogramType.UniformAdaptive
      case "QuantilesGlobal" => HistogramType.QuantilesGlobal
      case _ => throw new IllegalArgumentException("not implemented")
    }
    gbmParameters._learn_rate = ${learnRate}
    gbmParameters._learn_rate_annealing = ${learnRateAnnealing}
    gbmParameters._distribution = ${distribution} match {
      case "bernoulli" => DistributionFamily.bernoulli
      case "multinomial" => DistributionFamily.multinomial
      case "gaussian" => DistributionFamily.gaussian
    }
    gbmParameters._score_tree_interval = ${scoreTreeInterval}
    gbmParameters
  }

  private def train(trainData: DataFrame, validData: DataFrame): GBMModel = {
    val gbmParameters = constructGBMParameters(trainData, validData)
    val gbm = new GBM(gbmParameters)
    val gbmModel = gbm.trainModel.get
    gbmModel
  }

  override def fit(dataset: Dataset[_]): H2OGBMModel = {
    transformSchema(dataset.schema)
    val data = convertLabelColumnType(dataset.toDF())
    val gbmModel = train(data, null)
    val model = new H2OGBMModel(uid, gbmModel)
    copyValues(model)
  }

  def fit(trainDataSet: Dataset[_], validDataSet: Dataset[_]): H2OGBMModel = {
    transformSchema(trainDataSet.schema)
    val trainData = convertLabelColumnType(trainDataSet.toDF())
    val validData =
      if (validDataSet != null) {
        transformSchema(validDataSet.schema)
        convertLabelColumnType(validDataSet.toDF())
      } else {
        null
      }
    val gbmModel = train(trainData, validData)
    val model = new H2OGBMModel(uid, gbmModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains(${labelCol}), "Cannot find label column '%s' in DataFrame.".format(${labelCol}))
    validateSchema(schema)
  }

  override def copy(extra: ParamMap): H2OGBM = defaultCopy(extra)
}

object H2OGBM extends DefaultParamsReadable[H2OGBM] {
  override def load(path: String): H2OGBM = super.load(path)
}

class H2OGBMModel(override val uid: String, val model: GBMModel) extends Model[H2OGBMModel] with H2OTreeParams with H2OGBMParams
  with MLWritable{

  import H2OGBMModel._

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

  override def copy(extra: ParamMap): H2OGBMModel = {
    val copied = new H2OGBMModel(uid, model)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new H2OGBMModelWriter(this)
}

object H2OGBMModel extends MLReadable[H2OGBMModel] {

  private[H2OGBMModel]
  class H2OGBMModelWriter(instance: H2OGBMModel) extends MLWriter{

    override protected def saveImpl(path: String): Unit ={
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val modelPath = new Path(path, "model").toString
      instance.model.exportBinaryModel(modelPath, true)
    }
  }

  private[H2OGBMModel]
  class H2OGBMModelReader extends MLReader[H2OGBMModel]{

    private val className = classOf[H2OGBMModel].getName

    override  def load(path: String): H2OGBMModel={
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "model").toString
      val gbmModel =  ModelSerializationSupport.loadH2OModel(dataPath).asInstanceOf[GBMModel]
      val model = new H2OGBMModel(metadata.uid, gbmModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[H2OGBMModel] = new H2OGBMModelReader

  override def load(path: String):  H2OGBMModel= super.load(path)
}



