package org.apache.spark.ml.model.supervised

import net.qihoo.xitong.xdml.model.linalg.BLAS
import net.qihoo.xitong.xdml.model.loss._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Vector => newVector, VectorUDT => newVectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

trait LinearScopeParams extends Params
  with HasStepSize with HasMaxIter with HasRegParam with HasElasticNetParam with HasAggregationDepth
  with HasLoss with HasTol with HasFitIntercept with HasFeaturesCol with HasLabelCol {

  final val factor: DoubleParam = new DoubleParam(this, "factor", "factor")

  final val numPartitions: IntParam = new IntParam(this, "numPartitions", "num of repartion")

  final val restartFrequency: IntParam = new IntParam(this, "restartFrequency", "restart frequency")

  final val posWeight: DoubleParam = new DoubleParam(this, "posWeight", "weight of positive sample")

}

class LinearScope(override val uid: String) extends Estimator[LinearScopeModel]
  with LinearScopeParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LinearScope"))

  def setStepSize(value: Double): this.type = set(stepSize, value)

  setDefault(stepSize -> 1.0)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  setDefault(maxIter -> 5)

  def setRegParam(value: Double): this.type = set(regParam, value)

  setDefault(regParam -> 0.0)

  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)

  setDefault(elasticNetParam -> 0.0)

  def setFactor(value: Double): this.type = set(factor, value)

  setDefault(factor -> 0.0)

  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  setDefault(numPartitions -> 0)

  def setConvergenceTol(value: Double): this.type = set(tol, value)

  setDefault(tol -> 0.001)

  def setRestartFrequency(value: Int): this.type = set(restartFrequency, value)

  setDefault(restartFrequency -> 100000)

  def setTreeAggregateDepth(value: Int): this.type = set(aggregationDepth, value)

  setDefault(aggregationDepth -> 2)

  def setLossFunc(value: String): this.type = set(loss, value)

  setDefault(loss -> "LR")

  def setPosWeight(value: Double): this.type = set(posWeight, value)

  setDefault(posWeight -> 1.0)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  setDefault(fitIntercept -> true)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new newVectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    schema
  }

  // restartFrequency is only needed by regularization
  protected def restartFrequencyHint(): Unit = {
    if (${regParam} == 0.0) {
      set(restartFrequency->100000000)
    } else {
      set(restartFrequency -> math.round(-4 / math.log10(1 - ${regParam} * ${stepSize})).toInt)
    }
  }

  protected def getLossFunction(lossType: String, posWeight: Double, posRatio: Double, unlabeledRatio: Double): LossFunc = {
    val lossFunc = lossType match {
      case "LR" =>
        if (posWeight != 1.0) {
          new WeightedLogitLossFunc(posWeight)
        } else {
          new LogitLossFunc
        }
      case "SVM" =>
        if (posWeight != 1.0) {
          new WeightedHingeLossFunc(posWeight)
        } else {
          new HingeLossFunc
        }
      case "SSVM" =>
        if (posWeight != 1.0) {
          new WeightedSmoothHingeLossFunc(posWeight)
        } else {
          new SmoothHingeLossFunc
        }
      case "UPULR" =>
        new UPULogitLossFunc(posWeight, posRatio, unlabeledRatio)
      case _ => throw new IllegalArgumentException("not implemented")
    }
    lossFunc
  }

  protected def getUPULRParameter(trainData: RDD[(Double, Vector)]): (Double, Double) = {
    val labelWithCount = trainData.map(tuple => tuple._1).countByValue()
    val posCount = labelWithCount.filter(tuple => tuple._1 > 0.5).values.sum.toDouble
    val unlabeledCount = labelWithCount.filter(tuple => tuple._1 < 0.5).values.sum.toDouble
    val posRatio = posCount / (posCount + unlabeledCount)
    val unlabeledRatio = unlabeledCount / (posCount + unlabeledCount)
    (posRatio, unlabeledRatio)
  }

  protected def reducer(c: Option[Vector], x: Option[Vector]): Option[Vector] = {
    if (c.isDefined && x.isDefined) {
      BLAS.axpy(1.0, x.get, c.get)
      c
    } else if (c.isDefined) {
      c
    } else if (x.isDefined) {
      x
    } else {
      None
    }
  }

  protected def reducerWithIntercept(c: Option[(Vector, Double)], x: Option[(Vector, Double)]): Option[(Vector, Double)] = {
    if (c.isDefined && x.isDefined) {
      BLAS.axpy(1.0, x.get._1, c.get._1)
      Some((c.get._1, c.get._2 + x.get._2))
    } else if (c.isDefined) {
      c
    } else if (x.isDefined) {
      x
    } else {
      None
    }
  }

  protected def multiDot(a: Double, vec1: Vector, b: Double, vec2: Vector, vec3: Vector): Double = {
    (vec1, vec2, vec3) match {
      case (dv1: DenseVector, dv2: DenseVector, sv3: SparseVector) =>
        val sv3Indices = sv3.indices
        val sv3Values = sv3.values
        val dv1Values = dv1.values
        val dv2Values = dv2.values
        var sum = 0.0
        var i = 0
        val len = sv3Indices.length
        while (i < len) {
          val ind = sv3Indices(i)
          sum += (a * dv1Values(ind) + b * dv2Values(ind)) * sv3Values(i)
          i += 1
        }
        sum
      case _ =>
        throw new IllegalArgumentException("not implemented")
    }
  }

  protected def train(data: RDD[(Double, Vector)],
                      initialWeights: Vector,
                      lossFunc: LossFunc,
                      stepSize: Double,
                      numIterations: Int,
                      regParam: Double,
                      elasticNetParam: Double,
                      factor: Double,
                      numPartitions: Int,
                      fitIntercept: Boolean,
                      restartFrequency: Int,
                      treeAggregateDepth: Int,
                      convergenceTol: Double): (Vector, Double) = {
    // store loss history
    val lossHistory = new ArrayBuffer[Double]()
    // return if empty
    if (data.isEmpty()) {
      println("no data provided")
      return (initialWeights, 0.0)
    }
    // caching and repartitioning if required
    val dataCached =
    if (numPartitions > 0 && numPartitions != data.getNumPartitions)
      data.repartition(numPartitions).cache()
    else
      data.cache()
    val numPartitionsRenew = dataCached.getNumPartitions
    // init weights at driver
    val weights = Vectors.dense(initialWeights.toArray)
    val numWeights = weights.size
    var intercept = 0.0
    // learning loop
    var iter = 1
    while (iter <= numIterations) {
      // broadcast weights
      val bcWeights = dataCached.context.broadcast(weights)

      if (fitIntercept) {
        // treeAggregate gradient && loss
        val (gradientWeightsSum, gradientInterceptSum, lossSum, dataCount) = dataCached.treeAggregate((Vectors.zeros(numWeights), 0.0, 0.0, 0L))(
          seqOp = (c, v) => {
            // c: (gradW, gradI, loss, count), v: (label, features)
            val (gradientFactor, loss) = lossFunc.gradientWithLoss(v._2, v._1, bcWeights.value, intercept)
            BLAS.axpy(gradientFactor, v._2, c._1)
            (c._1, c._2 + gradientFactor, c._3 + loss, c._4 + 1)
          },
          combOp = (c1, c2) => {
            // c: (gradW, gradI, loss, count)
            BLAS.axpy(1.0, c2._1, c1._1)
            (c1._1, c1._2 + c2._2, c1._3 + c2._3, c1._4 + c2._4)
          })
        // loss
        val lossAvg = lossSum / dataCount.toDouble
        lossHistory += lossAvg
        println(s"Iteration ($iter/$numIterations). Loss without Regularization: $lossAvg.")
        // broadcast GradientWeightsSum
        val bcGradientWeightsSum = dataCached.context.broadcast(gradientWeightsSum)
        // Local Stochastic Variance Reduction
        val stepSizeWithDataCount = stepSize / dataCount
        val (weightsUpated, interceptUpdated) = dataCached.mapPartitions(iter => {
          val bcWeightsTarget = bcWeights.value.copy
          var interceptTarget = intercept
          val indexedSeq = iter.toIndexedSeq
          val indexedSeqLength = indexedSeq.length
          var a = 1.0
          var b = 0.0
          val rand = new Random(System.currentTimeMillis())
          for (j <- 1 to indexedSeqLength) {
            // restart is required by regularization
            if (j % restartFrequency == 0) {
              BLAS.scal(a, bcWeightsTarget)
              BLAS.axpy(b, bcGradientWeightsSum.value, bcWeightsTarget)
              interceptTarget = a * interceptTarget + b * gradientInterceptSum
              a = 1.0
              b = 0.0
            }
            val sample = indexedSeq(rand.nextInt(indexedSeqLength))
            val z1 = multiDot(a, bcWeightsTarget, b, bcGradientWeightsSum.value, sample._2) + a * interceptTarget + b * gradientInterceptSum
            val z2 = BLAS.dot(bcWeights.value, sample._2) + intercept
            //            b -= stepSizeWithDataCount
            //            val z = stepSize * (lossFunc.gradientFromDot(z1, sample._1) - lossFunc.gradientFromDot(z2, sample._1))
            b = (1 - regParam * stepSize) * b - stepSizeWithDataCount
            a = (1 - regParam * stepSize) * a
            val z = stepSize * (lossFunc.gradientFromDot(z1, sample._1) - lossFunc.gradientFromDot(z2, sample._1)) / a
            BLAS.axpy(-z, sample._2, bcWeightsTarget)
            interceptTarget -= z
          }
          BLAS.scal(a, bcWeightsTarget)
          BLAS.axpy(b, bcGradientWeightsSum.value, bcWeightsTarget)
          interceptTarget = a * interceptTarget + b * gradientInterceptSum
          Iterator(Some((bcWeightsTarget, interceptTarget)))
        }, true).treeAggregate(Option.empty[(Vector, Double)])(reducerWithIntercept, reducerWithIntercept, treeAggregateDepth).get
        bcGradientWeightsSum.destroy(blocking = false)
        BLAS.scal(1.0 / numPartitionsRenew, weightsUpated)
        (weightsUpated, weights) match {
          case (wo: DenseVector, wt: DenseVector) =>
            Array.copy(wo.values, 0, wt.values, 0, numWeights)
          case _ =>
            throw new IllegalArgumentException("not implemented")
        }
        intercept = interceptUpdated / numPartitionsRenew
      } else {
        // treeAggregate gradient && loss
        val (gradientWeightsSum, lossSum, dataCount) = dataCached.treeAggregate((Vectors.zeros(numWeights), 0.0, 0L))(
          seqOp = (c, v) => {
            // c: (gradW, loss, count), v: (label, features)
            val (gradientFactor, loss) = lossFunc.gradientWithLoss(v._2, v._1, bcWeights.value)
            BLAS.axpy(gradientFactor, v._2, c._1)
            (c._1, c._2 + loss, c._3 + 1)
          },
          combOp = (c1, c2) => {
            // c: (gradW, loss, count)
            BLAS.axpy(1.0, c2._1, c1._1)
            (c1._1, c1._2 + c2._2, c1._3 + c2._3)
          })
        // loss
        val lossAvg = lossSum / dataCount.toDouble
        lossHistory += lossAvg
        println(s"Iteration ($iter/$numIterations). Loss without Regularization: $lossAvg.")
        // broadcast GradientWeightsSum
        val bcGradientWeightsSum = dataCached.context.broadcast(gradientWeightsSum)
        // Local Stochastic Variance Reduction
        val stepSizeWithDataCount = stepSize / dataCount
        val weightsUpated = dataCached.mapPartitions(iter => {
          val bcWeightsTarget = bcWeights.value.copy
          val indexedSeq = iter.toIndexedSeq
          val indexedSeqLength = indexedSeq.length
          var a = 1.0
          var b = 0.0
          val rand = new Random(System.currentTimeMillis())
          for (j <- 1 to indexedSeqLength) {
            // restart is required by regularization
            if (j % restartFrequency == 0) {
              BLAS.scal(a, bcWeightsTarget)
              BLAS.axpy(b, bcGradientWeightsSum.value, bcWeightsTarget)
              a = 1.0
              b = 0.0
            }
            val sample = indexedSeq(rand.nextInt(indexedSeqLength))
            val z1 = multiDot(a, bcWeightsTarget, b, bcGradientWeightsSum.value, sample._2)
            val z2 = BLAS.dot(bcWeights.value, sample._2)
            //            b -= stepSizeWithDataCount
            //            val z = stepSize * (lossFunc.gradientFromDot(z1, sample._1) - lossFunc.gradientFromDot(z2, sample._1))
            b = (1 - regParam * stepSize) * b - stepSizeWithDataCount
            a = (1 - regParam * stepSize) * a
            val z = stepSize * (lossFunc.gradientFromDot(z1, sample._1) - lossFunc.gradientFromDot(z2, sample._1)) / a
            BLAS.axpy(-z, sample._2, bcWeightsTarget)
          }
          BLAS.scal(a, bcWeightsTarget)
          BLAS.axpy(b, bcGradientWeightsSum.value, bcWeightsTarget)
          Iterator(Some(bcWeightsTarget))
        }, true).treeAggregate(Option.empty[Vector])(reducer, reducer, treeAggregateDepth).get
        bcGradientWeightsSum.destroy(blocking = false)
        BLAS.scal(1.0 / numPartitionsRenew, weightsUpated)
        (weightsUpated, weights) match {
          case (wo: DenseVector, wt: DenseVector) =>
            Array.copy(wo.values, 0, wt.values, 0, numWeights)
          case _ =>
            throw new IllegalArgumentException("not implemented")
        }
      }
      bcWeights.destroy(blocking = false)
      iter += 1
    }

    (weights, intercept)

  }

  override def fit(dataset: Dataset[_]): LinearScopeModel = {
    transformSchema(dataset.schema)
    // numFeatures
    val featuresColIndex = dataset.schema.fieldNames.indexOf(${featuresCol})
    val numFeatures = dataset.first().asInstanceOf[Row].get(featuresColIndex).asInstanceOf[newVector].size
    // training data
    val trainData: RDD[(Double, Vector)] = dataset.select(
      col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map { case Row(label: Double, features: newVector) => {
      (label, Vectors.fromML(features))
    }
    }
    // initialWeights
    val initialWeights = Vectors.zeros(numFeatures)
    // restartFrequency
    restartFrequencyHint()
    // just for UPU-LR
    var posRatio: Double = 0.0
    var unlabeledRatio: Double = 0.0
    if ($(loss).equals("UPULR")) {
      val ratio = getUPULRParameter(trainData)
      posRatio = ratio._1
      unlabeledRatio = ratio._2
    }
    // loss func
    val lossFunc = getLossFunction(${loss}, ${posWeight}, posRatio, unlabeledRatio)
    // train
    val (weights, intercept) = train(trainData, initialWeights, lossFunc, ${stepSize}, ${maxIter},
      ${regParam}, ${elasticNetParam}, ${factor}, ${numPartitions}, ${fitIntercept}, ${restartFrequency},
      ${aggregationDepth}, ${tol})
    // model
    val model = new LinearScopeModel(uid, weights, intercept)
    copyValues(model)
  }

  def fit(dataset: Dataset[_], initWeights: newVector): LinearScopeModel = {
    transformSchema(dataset.schema)
    val featuresColIndex=dataset.schema.fieldNames.indexOf(${featuresCol})
    val numFeatures = dataset.first().asInstanceOf[Row].get(featuresColIndex).asInstanceOf[newVector].size
    val trainData: RDD[(Double, Vector)] = dataset.select(
      col($(labelCol)), col($(featuresCol))).rdd.map { case Row(label: Double, features: newVector) => {
      (label, Vectors.fromML(features))
    }
    }
    val initialWeights = Vectors.fromML(initWeights)
    restartFrequencyHint()
    var posRatio: Double = 0.0
    var unlabeledRatio: Double = 0.0
    if ($(loss).equals("UPULR")) {
      val ratio = getUPULRParameter(trainData)
      posRatio = ratio._1
      unlabeledRatio = ratio._2
    }
    val lossFunction = getLossFunction(${loss}, ${posWeight}, posRatio, unlabeledRatio)
    val (weights, intercept) = train(trainData, initialWeights, lossFunction, ${stepSize}, ${maxIter},
      ${regParam}, ${elasticNetParam}, ${factor}, ${numPartitions}, ${fitIntercept}, ${restartFrequency},
      ${aggregationDepth}, ${tol})
    val model = new LinearScopeModel(uid, weights, intercept)
    copyValues(model)
  }

  override def copy(extra: ParamMap): LinearScope = defaultCopy(extra)

}

object LinearScope extends DefaultParamsReadable[LinearScope] {
  override def load(path: String): LinearScope = super.load(path)
}

class LinearScopeModel(override val uid: String, val coefficients: Vector, val intercept: Double)
  extends Model[LinearScopeModel] with LinearScopeParams with HasRawPredictionCol with HasProbabilityCol with MLWritable {

  import LinearScopeModel._

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new newVectorUDT)
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(${rawPredictionCol}),s"prediction column $rawPredictionCol already exists.")
    require(!fieldNames.contains(${probabilityCol}),s"probability column $probabilityCol already exists.")
    val outputFields = new ArrayBuffer[StructField]()
    schema.fields.foreach { structField => outputFields += structField }
    outputFields += new StructField(${rawPredictionCol}, DoubleType, true)
    if($(loss).contains("LR")){
      outputFields += new StructField(${probabilityCol}, DoubleType, true)
    }else{
      //warning:  Probability will be calculated in svm or ssvm model, but this probability is meaningless.
      //This probability is just for convenience of evaluation model.
      outputFields += new StructField(${probabilityCol}, DoubleType, true)
    }
    StructType(outputFields.toArray)
  }

  protected def sigmoid(z: Double): Double = {
     1.0 / (1.0 + math.exp(-z))
  }

  protected def predict(features: Vector): Double = {
     BLAS.dot(coefficients, features) + intercept
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val newSchema = transformSchema(dataset.schema)
    if ($(rawPredictionCol).nonEmpty) {
      transformImpl(dataset, newSchema)
    } else {
      this.logWarning(s"$uid: Predictor.transform() was called as NOOP" +
        " since no output columns were set.")
      dataset.toDF
    }
  }

  protected def transformImpl(dataset: Dataset[_], newSchema: StructType): DataFrame = {
    val featuresColIndex = dataset.schema.fieldNames.indexOf(${featuresCol})
    val predRdd = if (${loss}.contains("LR")) {
      dataset.toDF.rdd.map { row => {
        val features = Vectors.fromML(row.getAs[newVector](featuresColIndex))
        val rawPred = predict(features)
        val prob = sigmoid(rawPred)
        Row.fromSeq(row.toSeq ++ Row(rawPred, prob).toSeq)
      }
      }
    } else {
      //warning:  Probability will be calculated in svm or ssvm model, but this probability is meaningless.
      //This probability is just for convenience of evaluation model.
      dataset.toDF.rdd.map { row => {
        val features = Vectors.fromML(row.getAs[newVector](featuresColIndex))
        val rawPred = predict(features)
        val prob = sigmoid(rawPred)
        Row.fromSeq(row.toSeq ++ Row(rawPred, prob).toSeq)
      }
      }
    }
    dataset.sparkSession.createDataFrame(predRdd, newSchema)
  }

  override def copy(extra: ParamMap): LinearScopeModel = {
    val copied = new LinearScopeModel(uid, coefficients, intercept)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new LinearScopeModelWriter(this)

}

object LinearScopeModel extends MLReadable[LinearScopeModel] {

  private[LinearScopeModel]
  class LinearScopeModelWriter(instance: LinearScopeModel) extends MLWriter {

    private case class Data(coefficients: Vector, intercept: Double)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.coefficients, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private[LinearScopeModel]
  class LinearScopeModelReader extends MLReader[LinearScopeModel] {

    private val className = classOf[LinearScopeModel].getName

    override def load(path: String): LinearScopeModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) = data.select("coefficients", "intercept").head()
      val model = new LinearScopeModel(metadata.uid, coefficients, intercept)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[LinearScopeModel] = new LinearScopeModelReader

  override def load(path: String): LinearScopeModel = super.load(path)

}


