package org.apache.spark.ml.model.supervised

import hex.Model.Parameters.CategoricalEncodingScheme
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.{Activation, Loss, MissingValuesHandling}
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.genmodel.utils.DistributionFamily
import org.apache.hadoop.fs.Path
import org.apache.spark.h2o.{Dataset => _, _}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasStandardization
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import water.support.ModelSerializationSupport

import scala.collection.mutable.ArrayBuffer


trait H2OMLPParams extends H2OParams with HasStandardization{

  final val missingValuesHandling: Param[String] = new Param[String](this, "missingValuesHandling", "Handling of missing values. " +
    "Supported options: " + H2OMLPParams.supporetedMissingValuesHandling.mkString(", "),
    ParamValidators.inArray(H2OMLPParams.supporetedMissingValuesHandling))

  final val categoricalEncodingScheme: Param[String] = new Param[String](this, "categoricalEncodingScheme", "Encoding scheme " +
    "for categorical features. Supported options : " + H2OMLPParams.supportedCategoricalEncoding.mkString(", "),
    ParamValidators.inArray(H2OMLPParams.supportedCategoricalEncoding))

  final val distribution: Param[String] = new Param[String](this, "distribution", "Distribution for dataSet. " +
    "Supported options: " + H2OMLPParams.supportedDistribution, ParamValidators.inArray(H2OMLPParams.supportedDistribution))

  final val hidden: IntArrayParam = new IntArrayParam(this, "hidden", "Hidden layer sizes", ParamValidators.arrayLengthGt(0))

  final val activation: Param[String] = new Param[String](this, "activation", "Activation function. Supported options: " +
    H2OMLPParams.supportedActivationFuncOptions.mkString(", "), ParamValidators.inArray(H2OMLPParams.supportedActivationFuncOptions))

  final val epochs: DoubleParam = new DoubleParam(this, "epochs", "How many times the dataset should be iterated (streamed), " +
    "can be fractional.")

  final val learnRate: DoubleParam = new DoubleParam(this, "learnRate", "Learn rate to be used for each iteration of optimization (> 0)",
    ParamValidators.inRange(0,1,false,true))

  final val momentumStart: DoubleParam = new DoubleParam(this, "momentumStart", "The initial momentum at the beginning of training.")

  final val momentumStable: DoubleParam = new DoubleParam(this, "momentumStable", "The final momentum after the ramp is over.")

  final val l1: DoubleParam = new DoubleParam(this, "l1", "L1 regularization.", ParamValidators.gtEq(0))

  final val l2: DoubleParam = new DoubleParam(this, "l2", "L2 regularization.", ParamValidators.gtEq(0))

  final val hiddenDropoutRatios: DoubleArrayParam = new DoubleArrayParam(this, "hiddenDropoutRatios", "Hidden layer dropout ratio.")

  final val elasticAveraging: BooleanParam = new BooleanParam(this, "elasticAveraging", "Elastic averaging between compute nodes can " +
    "improve distributed model convergence.")

}

object H2OMLPParams{

   final val supportedCategoricalEncoding: Array[String] = Array("AUTO", "Enum", "LabelEncoder", "OneHotExplicit", "SortByResponse")

   final val supportedActivationFuncOptions: Array[String] = Array("Rectifier", "RectifierWithDropout", "Maxout", "MaxoutWithDropout",
     "ExpRectifier", "ExpRectifierWithDropout", "Tanh", "TanhWithDropout")

   final val supportedDistribution: Array[String] = Array("bernoulli", "multinomial")

   final val supporetedMissingValuesHandling: Array[String] = Array("Skip", "MeanImputation")

}


class H2OMLP(override val uid: String ) extends Estimator[H2OMLPModel] with H2OMLPParams
  with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("H2OMLP"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setCatFeatColNames(values: Array[String]): this.type = set(catFeatColNames, values)

  def setIgnoreFeatColNames(values: Array[String]): this.type = set(ignoredFeatColNames, values)

  def setMissingValueHandling(value: String): this.type = set(missingValuesHandling, value)

  def setCategoricalEncodingScheme(value: String): this.type = set(categoricalEncodingScheme, value)

  def setStandardization(value: Boolean): this.type = set(standardization, value)

  def setDistribution(value: String): this.type = set(distribution, value)

  def setHidden(values: Array[Int]): this.type = set(hidden, values)

  def setActivation(value: String): this.type = set(activation, value)

  def setEpochs(value: Double): this.type =set(epochs, value)

  def setLearnRate(value: Double): this.type = set(learnRate, value)

  def setMomentumStart(value: Double): this.type = set(momentumStart, value)

  def setMomentumStable(value: Double): this.type = set(momentumStable, value)

  def setL1(value: Double): this.type = set(l1, value)

  def setL2(value: Double): this.type = set(l2, value)

  def setHiddenDropoutRatios(values: Array[Double]): this.type = set(hiddenDropoutRatios, values)

  def setElasticAveraging(value: Boolean): this.type = set(elasticAveraging, value)

  setDefault(catFeatColNames -> Array(), ignoredFeatColNames -> Array())

  setDefault(missingValuesHandling -> "MeanImputation", categoricalEncodingScheme -> "Enum", standardization -> true , distribution -> "bernoulli",
    hidden -> Array(200, 200), activation -> "Rectifier", epochs -> 10, learnRate -> 0.1, momentumStart -> 0.5, momentumStable -> 0.99,
    l1 -> 0, l2 -> 0, hiddenDropoutRatios -> Array(0.5), elasticAveraging -> false)

  private def constructMLPParameters(trainData: DataFrame, validData: DataFrame): DeepLearningParameters={
    val h2oContext = H2OContext.getOrCreate(trainData.sparkSession)
    import h2oContext.implicits._
    val trainDataH2O: H2OFrame = trainData
    val validDataH2O: H2OFrame = if(validData == null) null else validData
    val mlpParameters = new DeepLearningParameters()
    val enumColNames = new ArrayBuffer[String]()
    enumColNames += ${labelCol}
    ${catFeatColNames}.foreach(catFeatName => enumColNames += catFeatName)
    val enumColNamesArr = enumColNames.toArray
    trainDataH2O.colToEnum(enumColNamesArr)
    mlpParameters._train = trainDataH2O
    if(validData != null){
      validDataH2O.colToEnum(enumColNamesArr)
      mlpParameters._valid = validDataH2O
    }
    mlpParameters._response_column = ${labelCol}
    mlpParameters._ignored_columns = ${ignoredFeatColNames}
    // feature process
    mlpParameters._missing_values_handling = ${missingValuesHandling} match {
      case "Skip" => MissingValuesHandling.Skip
      case "MeanImputation" => MissingValuesHandling.MeanImputation
      case _ => throw new IllegalArgumentException("not implemented")
    }
    mlpParameters._categorical_encoding = ${categoricalEncodingScheme} match {
      case "AUTO" => CategoricalEncodingScheme.AUTO
      case "OneHotInternal" => CategoricalEncodingScheme.OneHotInternal
      case "LabelEncoder" => CategoricalEncodingScheme.LabelEncoder
      case "SortByResponse" => CategoricalEncodingScheme.SortByResponse
      case _ => throw new IllegalArgumentException("not implemented")
    }
    mlpParameters._standardize = ${standardization}
    // neural network
    mlpParameters._distribution = ${distribution} match {
      case "bernoulli" => DistributionFamily.bernoulli
      case "multinomial" => DistributionFamily.multinomial
      case _ => throw new IllegalArgumentException("not implemented")
    }
    mlpParameters._loss = Loss.CrossEntropy
    mlpParameters._hidden = ${hidden}
    mlpParameters._activation = ${activation} match {
      case "Rectifier" => Activation.Rectifier
      case "RectifierWithDropout" => Activation.RectifierWithDropout
      case "Maxout" => Activation.Maxout
      case "MaxoutWithDropout" => Activation.MaxoutWithDropout
      case "ExpRectifier" => Activation.ExpRectifier
      case "ExpRectifierWithDropout" => Activation.ExpRectifierWithDropout
      case "Tanh" => Activation.Tanh
      case "TanhWithDropout" => Activation.TanhWithDropout
      case _ => throw new IllegalArgumentException("not implemented")
    }
    // optimizer
    val supportedHiddenDropoutActivations = Array("TanhWithDropout", "RectifierWithDropout", "MaxoutWithDropout")
    mlpParameters._epochs = ${epochs}
    mlpParameters._train_samples_per_iteration = 0
    mlpParameters._adaptive_rate = true
    mlpParameters._rho = 0.99
    mlpParameters._epsilon = 1.0e-8
    mlpParameters._rate = ${learnRate}
    mlpParameters._momentum_start = ${momentumStart}
    mlpParameters._momentum_stable = ${momentumStable}
    mlpParameters._l1 = ${l1}
    mlpParameters._l2 = ${l2}
    mlpParameters._hidden_dropout_ratios =
      if (${hiddenDropoutRatios}.length>0  && supportedHiddenDropoutActivations.contains(${activation}))
        ${hiddenDropoutRatios}
      else
        null
    mlpParameters._elastic_averaging = ${elasticAveraging}
    mlpParameters
  }

  private def train(trainData: DataFrame, validData: DataFrame): DeepLearningModel={
    val mlpParameters = constructMLPParameters(trainData, validData)
    val mlp = new DeepLearning(mlpParameters)
    val mlpModel = mlp.trainModel.get
    mlpModel
  }

  override def fit(dataset: Dataset[_]): H2OMLPModel = {
    transformSchema(dataset.schema)
    val mlpModel = train(dataset.toDF(), null)
    val model = new H2OMLPModel(uid, mlpModel)
    copyValues(model)
  }

  def fit(trainDataSet: Dataset[_], validDataSet: Dataset[_]): H2OMLPModel = {
    transformSchema(trainDataSet.schema)
    val validData =
      if (validDataSet != null) {
        transformSchema(validDataSet.schema)
        validDataSet.toDF()
      } else {
        null
      }
    val mlpModel = train(trainDataSet.toDF(), validData)
    val model = new H2OMLPModel(uid, mlpModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains(${labelCol}), "Cannot find label column '%s' in DataFrame.".format(${labelCol}))
    validateSchema(schema)
  }

  override def copy(extra: ParamMap): H2OMLP = defaultCopy(extra)
}

object H2OMLP extends DefaultParamsReadable[H2OMLP]{
  override def load(path: String): H2OMLP = super.load(path)
}

class H2OMLPModel(override val uid: String, val model: DeepLearningModel) extends Model[H2OMLPModel] with H2OMLPParams
  with MLWritable{

  import H2OMLPModel._

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
    (0 until numOfClasses).foreach { classIndex =>
      val colName = "p" + classIndex
      outputFields += StructField(colName, DoubleType, false)
    }
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): H2OMLPModel = {
    val copied = new H2OMLPModel(uid, model)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new H2OMLPModelWriter(this)
}

object H2OMLPModel extends MLReadable[H2OMLPModel]{

  private[H2OMLPModel]
  class H2OMLPModelWriter(instance: H2OMLPModel) extends MLWriter{

    override protected def saveImpl(path: String): Unit ={
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val modelPath = new Path(path, "model").toString
      instance.model.exportBinaryModel(modelPath, true)
    }
  }

  private[H2OMLPModel]
  class H2OMLPModelReader extends MLReader[H2OMLPModel]{

    private val className = classOf[H2OMLPModel].getName

    override  def load(path: String): H2OMLPModel={
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "model").toString
      val mlpModel =  ModelSerializationSupport.loadH2OModel(dataPath).asInstanceOf[DeepLearningModel]
      val model = new H2OMLPModel(metadata.uid, mlpModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[H2OMLPModel] = new H2OMLPModelReader

  override def load(path: String):  H2OMLPModel= super.load(path)
}