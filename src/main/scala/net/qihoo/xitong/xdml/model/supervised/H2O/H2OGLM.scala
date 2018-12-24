package org.apache.spark.ml.model.supervised

import hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling
import hex.glm.GLMModel.GLMParameters
import hex.glm.{GLM, GLMModel}
import org.apache.hadoop.fs.Path
import org.apache.spark.h2o._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFitIntercept, HasMaxIter, HasStandardization}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import water.support.ModelSerializationSupport

import scala.collection.mutable.ArrayBuffer


trait H2OGLMParams extends H2OParams with HasMaxIter with HasFitIntercept with HasStandardization{

  final val family: Param[String] = new Param[String](this, "family", "The name of family which is a description of the error " +
    "distribution to be used in the model. Supported options: " + H2OGLMParams.supportedFamily,
    ParamValidators.inArray(H2OGLMParams.supportedFamily))

  final val alpha: DoubleParam = new DoubleParam(this, "alpha", "The ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, " +
    "the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty", ParamValidators.inRange(0, 1))

  final val lambda: DoubleParam  = new DoubleParam(this, "lambda", "Regularization parameter (>= 0)", ParamValidators.gtEq(0))

  final val missingValuesHandling: Param[String] = new Param[String](this, "missingValuesHandling", "Handling of missing values. " +
    "Supported options: " + H2OGLMParams.supporetedMissingValuesHandling.mkString(", "),
    ParamValidators.inArray(H2OGLMParams.supporetedMissingValuesHandling))
}

object H2OGLMParams{

  final val supportedFamily: Array[String] = Array("binomial", "multinomial", "gaussian")

  final val supporetedMissingValuesHandling: Array[String] = Array("Skip", "MeanImputation")
}

class H2OGLM(override val uid: String) extends Estimator[H2OGLMModel] with H2OGLMParams
  with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("H2OGLM"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setCatFeatColNames(values: Array[String]): this.type = set(catFeatColNames, values)

  def setIgnoreFeatColNames(values: Array[String]): this.type = set(ignoredFeatColNames, values)

  def setFamily(value: String): this.type = set(family, value)

  def setAlpha(value: Double): this.type = set(alpha, value)

  def setLambda(value: Double): this.type = set(lambda, value)

  def setStandardization(value: Boolean): this.type = set(standardization, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setMissingValueHandling(value: String): this.type = set(missingValuesHandling, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  setDefault(catFeatColNames -> Array(), ignoredFeatColNames -> Array())

  setDefault(family -> "binomial", alpha -> 0.0, lambda -> 0.0, standardization -> true,
    maxIter -> 100, missingValuesHandling -> "MeanImputation", fitIntercept -> true)

  //TODO: cast label column from string to double may cause mass data manipulation
  //Warning: response missing will cause prediction failed.( for example , in regression problem, predict failed when label is categorical .)
  private def convertLabelColumnType(data: DataFrame): DataFrame={
    // convert label to double in regression problem.
    if($(family).equals("gaussian")) {
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

  private def constructGLMParameters(trainData: DataFrame, validData: DataFrame): GLMParameters={
    val h2oContext = H2OContext.getOrCreate(trainData.sparkSession)
    import h2oContext.implicits._
    val trainDataH2O: H2OFrame = trainData
    val validDataH2O: H2OFrame = if(validData == null) null else validData
    val glmParameters = new GLMParameters()
    val enumColNames = new ArrayBuffer[String]()
    if(!${family}.equals("gaussian")){
      enumColNames += ${labelCol}
    }
    ${catFeatColNames}.foreach(catFeatName => enumColNames += catFeatName)
    val enumColNamesArr = enumColNames.toArray
    trainDataH2O.colToEnum(enumColNamesArr)
    glmParameters._train = trainDataH2O
    if(validData != null){
      validDataH2O.colToEnum(enumColNamesArr)
      glmParameters._valid = validDataH2O
    }
    glmParameters._response_column = ${labelCol}
    glmParameters._ignored_columns = ${ignoredFeatColNames}
    glmParameters._family = ${family} match {
      case "binomial" => GLMParameters.Family.binomial
      case "multinomial" => GLMParameters.Family.multinomial
      case "gaussian" => GLMParameters.Family.gaussian
      case _ => throw new IllegalArgumentException("not implemented")
    }
    glmParameters._solver = GLMParameters.Solver.L_BFGS
    glmParameters._max_iterations = ${maxIter}
    glmParameters._alpha = Array(${alpha})
    glmParameters._lambda = Array(${lambda})
    glmParameters._intercept = ${fitIntercept}
    glmParameters._standardize = ${standardization}
    glmParameters._missing_values_handling = ${missingValuesHandling} match {
      case "Skip" => MissingValuesHandling.Skip
      case "MeanImputation" => MissingValuesHandling.MeanImputation
      case _ => throw new IllegalArgumentException("not implemented")
    }
    glmParameters
  }

  private def train(trainData: DataFrame, validData: DataFrame): GLMModel={
    val glmParameters = constructGLMParameters(trainData,validData)
    val glm = new GLM(glmParameters)
    val glmModel = glm.trainModel.get
    glmModel
  }

  override def fit(dataset: Dataset[_]): H2OGLMModel = {
    transformSchema(dataset.schema)
    val data = convertLabelColumnType(dataset.toDF())
    val glmModel = train(data, null)
    val model = new H2OGLMModel(uid, glmModel)
    copyValues(model)
  }

  def fit(trainDataSet: Dataset[_], validDataSet: Dataset[_]): H2OGLMModel ={
    transformSchema(trainDataSet.schema)
    val trainData = convertLabelColumnType(trainDataSet.toDF())
    val validData =
      if (validDataSet != null) {
        transformSchema(validDataSet.schema)
        convertLabelColumnType(validDataSet.toDF())
      } else {
        null
      }
    val glmModel = train(trainData, validData)
    val model = new H2OGLMModel(uid, glmModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains(${labelCol}), "Cannot find label column '%s' in DataFrame.".format(${labelCol}))
    validateSchema(schema)
  }

  override def copy(extra: ParamMap): H2OGLM = defaultCopy(extra)
}

object H2OGLM extends DefaultParamsReadable[H2OGLM]{
  override def load(path: String): H2OGLM = super.load(path)
}

class H2OGLMModel(override val uid: String, val model: GLMModel) extends Model[H2OGLMModel] with H2OGLMParams
  with MLWritable{

  import H2OGLMModel._

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
    if(!${family}.equals("gaussian")) {
      (0 until numOfClasses).foreach { classIndex =>
        val colName = "p" + classIndex
        outputFields += StructField(colName, DoubleType, false)
      }
    }
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): H2OGLMModel = {
    val copied = new H2OGLMModel(uid, model)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new H2OGLMModelWriter(this)
}

object H2OGLMModel extends MLReadable[H2OGLMModel]{

  private[H2OGLMModel]
  class H2OGLMModelWriter(instance: H2OGLMModel) extends MLWriter{

    override protected def saveImpl(path: String): Unit ={
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val modelPath = new Path(path, "model").toString
      instance.model.exportBinaryModel(modelPath, true)
    }
  }

  private[H2OGLMModel]
  class H2OGLMModelReader extends MLReader[H2OGLMModel]{

    private val className = classOf[H2OGLMModel].getName

    override  def load(path: String): H2OGLMModel={
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "model").toString
      val glmModel =  ModelSerializationSupport.loadH2OModel(dataPath).asInstanceOf[GLMModel]
      val model = new H2OGLMModel(metadata.uid, glmModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[H2OGLMModel] = new H2OGLMModelReader

  override def load(path: String):  H2OGLMModel= super.load(path)
}