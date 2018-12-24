package org.apache.spark.ml.model.supervised

import net.qihoo.xitong.xdml.model.linalg.BLAS
import net.qihoo.xitong.xdml.model.loss._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Vector => newVector, VectorUDT => newVectorUDT, Vectors => newVectors}
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


trait OVRLinearScopeParams extends Params
  with HasStepSize with HasMaxIter with HasRegParam with HasElasticNetParam with HasAggregationDepth
  with HasLoss with HasTol with HasFitIntercept with HasFeaturesCol with HasLabelCol {

  final val factor: DoubleParam = new DoubleParam(this, "factor", "factor")

  final val numPartitions: IntParam = new IntParam(this, "numPartitions", "number of repartitions")

  final val restartFrequency: IntParam = new IntParam(this, "restartFrequency", "restart frequency")

  final val numClasses: IntParam = new IntParam(this, "numClasses", "number of classes")

}

class OVRLinearScope(override val uid: String) extends Estimator[OVRLinearScopeModel]
  with OVRLinearScopeParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("OVRLinearScope"))

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

  setDefault(loss -> "SVM")

  def setNumClasses(value: Int): this.type = set(numClasses, value)

  setDefault(numClasses -> 2)

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

  protected def getLossFunction(lossType: String, numClasses: Int): MultiLossFunc = {
    // TODO: Add Kernel
    val lossFunc = lossType match {
      case "SVM" =>
        new MultiHingeLossFunc(numClasses)
      case "SSVM" =>
        new MultiSmoothHingeLossFunc(numClasses)
      case _ => throw new IllegalArgumentException("not implemented")
    }
    lossFunc
  }

  protected def reducer(c: Option[Array[Vector]], x: Option[Array[Vector]]): Option[Array[Vector]] = {
    if (c.isDefined && x.isDefined) {
      val len = c.get.length
      for(ind <- 0 until len) {
        BLAS.axpy(1.0, x.get.apply(ind), c.get.apply(ind))
      }
      c
    } else if (c.isDefined) {
      c
    } else if (x.isDefined) {
      x
    } else {
      None
    }
  }

  protected def reducerWithIntercept(c: Option[(Array[Vector], Array[Double])],
                                     x: Option[(Array[Vector], Array[Double])]): Option[(Array[Vector], Array[Double])] = {
    if (c.isDefined && x.isDefined) {
      val len = c.get._2.length
      for(ind <- 0 until len) {
        BLAS.axpy(1.0, x.get._1(ind), c.get._1(ind))
        c.get._2(ind) += x.get._2(ind)
      }
      c
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
                      initialWeightsArr: Array[Vector],
                      lossFunc: MultiLossFunc,
                      numClasses: Int,
                      stepSize: Double,
                      numIterations: Int,
                      regParam: Double,
                      elasticNetParam: Double,
                      factor: Double,
                      numPartitions: Int,
                      fitIntercept: Boolean,
                      restartFrequency: Int,
                      treeAggregateDepth: Int,
                      convergenceTol: Double): (Array[Vector], Array[Double]) = {
    // store loss history
    val lossHistory = new ArrayBuffer[Double]()
    // return if empty
    if (data.isEmpty()) {
      println("no data provided")
      return (initialWeightsArr, Array.fill(numClasses)(0.0))
    }
    // caching and repartitioning if required
    val dataCached =
      if (numPartitions > 0 && numPartitions != data.getNumPartitions)
        data.repartition(numPartitions).cache()
      else
        data.cache()
    val numPartitionsRenew = dataCached.getNumPartitions
    // init weights at driver
    val weightsArr = initialWeightsArr.map(w => Vectors.dense(w.toArray))
    val numWeights = weightsArr(0).size
    val interceptArr = Array.fill(numClasses)(0.0)
    // learning loop
    var iter = 1
    while (iter <= numIterations) {
      // broadcast weights
      val bcWeightsArr = dataCached.context.broadcast(weightsArr)

      if (fitIntercept) {
        // treeAggregate gradient && loss
        val (gradientWeightsSumArr, gradientInterceptSumArr, lossSum, dataCount) = dataCached.treeAggregate(
          (Array.fill(numClasses)(Vectors.zeros(numWeights)), Array.fill(numClasses)(0.0), 0.0, 0L)
        )(
          seqOp = (c, v) => {
            // c: (gradW, gradI, loss, count), v: (label, features)
            val (gradientFactorArr, loss) = lossFunc.gradientWithLoss(v._2, v._1, bcWeightsArr.value, interceptArr)
            for(ind <- 0 until numClasses) {
              BLAS.axpy(gradientFactorArr(ind), v._2, c._1(ind))
              c._2(ind) += gradientFactorArr(ind)
            }
            (c._1, c._2, c._3 + loss, c._4 + 1)
          },
          combOp = (c1, c2) => {
            // c: (gradW, gradI, loss, count)
            for(ind <- 0 until numClasses) {
              BLAS.axpy(1.0, c2._1(ind), c1._1(ind))
              c1._2(ind) += c2._2(ind)
            }
            (c1._1, c1._2, c1._3 + c2._3, c1._4 + c2._4)
          })
        // loss
        val lossAvg = lossSum / dataCount.toDouble
        lossHistory += lossAvg
        println(s"Iteration ($iter/$numIterations). Loss without Regularization: $lossAvg.")
        /****************** SCOPE *********************/
        // broadcast GradientWeightsSumArr
        val bcGradientWeightsSumArr = dataCached.context.broadcast(gradientWeightsSumArr)
        // Local Stochastic Variance Reduction
        val stepSizeWithDataCount = stepSize / dataCount
        val (weightsArrUpdated, interceptArrUpdated) = dataCached.mapPartitions(iter => {
          val bcWeightsArrTarget = bcWeightsArr.value.map(_.copy)
          val interceptArrTarget = interceptArr.clone()
          val indexedSeq = iter.toIndexedSeq
          val indexedSeqLength = indexedSeq.length
          var a = 1.0
          var b = 0.0
          val rand = new Random(System.currentTimeMillis())
          for (j <- 1 to indexedSeqLength) {
            // restart is required by regularization
            if (j % restartFrequency == 0) {
              for(ind <- 0 until numClasses) {
                BLAS.scal(a, bcWeightsArrTarget(ind))
                BLAS.axpy(b, bcGradientWeightsSumArr.value(ind), bcWeightsArrTarget(ind))
                interceptArrTarget(ind) = a * interceptArrTarget(ind) + b * gradientInterceptSumArr(ind)
              }
              a = 1.0
              b = 0.0
            }
            val margins1 = Array.fill(numClasses)(0.0)
            val margins2 = Array.fill(numClasses)(0.0)
            val sample = indexedSeq(rand.nextInt(indexedSeqLength))
            for(ind <- 0 until numClasses) {
              margins1(ind) = multiDot(a, bcWeightsArrTarget(ind), b, bcGradientWeightsSumArr.value(ind), sample._2) + a * interceptArrTarget(ind) + b * gradientInterceptSumArr(ind)
              margins2(ind) = BLAS.dot(bcWeightsArr.value(ind), sample._2) + interceptArr(ind)
            }
            b = (1 - regParam * stepSize) * b - stepSizeWithDataCount
            a = (1 - regParam * stepSize) * a
            val gradFac1 = lossFunc.gradientFromMargins(margins1, sample._1)
            val gradFac2 = lossFunc.gradientFromMargins(margins2, sample._1)
            for(ind <- 0 until numClasses) {
              val z = stepSize * (gradFac1(ind) - gradFac2(ind)) / a
              BLAS.axpy(-z, sample._2, bcWeightsArrTarget(ind))
              interceptArrTarget(ind) -= z
            }
          }
          for(ind <- 0 until numClasses) {
            BLAS.scal(a, bcWeightsArrTarget(ind))
            BLAS.axpy(b, bcGradientWeightsSumArr.value(ind), bcWeightsArrTarget(ind))
            interceptArrTarget(ind) = a * interceptArrTarget(ind) + b * gradientInterceptSumArr(ind)
          }
          Iterator(Some((bcWeightsArrTarget, interceptArrTarget)))
        }, true).treeAggregate(Option.empty[(Array[Vector], Array[Double])])(reducerWithIntercept, reducerWithIntercept, treeAggregateDepth).get
        for(ind <- 0 until numClasses) {
          // old method
          BLAS.scal(1.0 / numPartitionsRenew, weightsArrUpdated(ind))
          (weightsArrUpdated(ind), weightsArr(ind)) match {
            case (wo: DenseVector, wt: DenseVector) =>
              Array.copy(wo.values, 0, wt.values, 0, numWeights)
            case _ =>
              throw new IllegalArgumentException("not implemented")
          }
          interceptArr(ind) = interceptArrUpdated(ind) / numPartitionsRenew
        }
        bcGradientWeightsSumArr.destroy(blocking = false)
      } else {
        // treeAggregate gradient && loss
        val (gradientWeightsSumArr, lossSum, dataCount) = dataCached.treeAggregate(
          (Array.fill(numClasses)(Vectors.zeros(numWeights)), 0.0, 0L)
        )(
          seqOp = (c, v) => {
            // c: (gradW, loss, count), v: (label, features)
            val (gradientFactorArr, loss) = lossFunc.gradientWithLoss(v._2, v._1, bcWeightsArr.value)
            for(ind <- 0 until numClasses) {
              BLAS.axpy(gradientFactorArr(ind), v._2, c._1(ind))
            }
            (c._1, c._2 + loss, c._3 + 1)
          },
          combOp = (c1, c2) => {
            // c: (gradW, gradI, loss, count)
            for(ind <- 0 until numClasses) {
              BLAS.axpy(1.0, c2._1(ind), c1._1(ind))
            }
            (c1._1, c1._2 + c2._2, c1._3 + c2._3)
          })
        // loss
        val lossAvg = lossSum / dataCount.toDouble
        lossHistory += lossAvg
        println(s"Iteration ($iter/$numIterations). Loss without Regularization: $lossAvg.")
        /****************** SCOPE *********************/
        // broadcast GradientWeightsSumArr
        val bcGradientWeightsSumArr = dataCached.context.broadcast(gradientWeightsSumArr)
        // Local Stochastic Variance Reduction
        val stepSizeWithDataCount = stepSize / dataCount
        val weightsArrUpdated = dataCached.mapPartitions(iter => {
          val bcWeightsArrTarget = bcWeightsArr.value.map(_.copy)
          val indexedSeq = iter.toIndexedSeq
          val indexedSeqLength = indexedSeq.length
          var a = 1.0
          var b = 0.0
          val rand = new Random(System.currentTimeMillis())
          for (j <- 1 to indexedSeqLength) {
            // restart is required by regularization
            if (j % restartFrequency == 0) {
              for(ind <- 0 until numClasses) {
                BLAS.scal(a, bcWeightsArrTarget(ind))
                BLAS.axpy(b, bcGradientWeightsSumArr.value(ind), bcWeightsArrTarget(ind))
              }
              a = 1.0
              b = 0.0
            }
            val margins1 = Array.fill(numClasses)(0.0)
            val margins2 = Array.fill(numClasses)(0.0)
            val sample = indexedSeq(rand.nextInt(indexedSeqLength))
            for(ind <- 0 until numClasses) {
              margins1(ind) = multiDot(a, bcWeightsArrTarget(ind), b, bcGradientWeightsSumArr.value(ind), sample._2)
              margins2(ind) = BLAS.dot(bcWeightsArr.value(ind), sample._2)
            }
            //            b -= stepSizeWithDataCount
            //            val z = stepSize * (lossFunc.gradientFromDot(z1, sample._1) - lossFunc.gradientFromDot(z2, sample._1))
            b = (1 - regParam * stepSize) * b - stepSizeWithDataCount
            a = (1 - regParam * stepSize) * a
            val gradFac1 = lossFunc.gradientFromMargins(margins1, sample._1)
            val gradFac2 = lossFunc.gradientFromMargins(margins2, sample._1)
            for(ind <- 0 until numClasses) {
              val z = stepSize * (gradFac1(ind) - gradFac2(ind)) / a
              BLAS.axpy(-z, sample._2, bcWeightsArrTarget(ind))
            }
          }
          for(ind <- 0 until numClasses) {
            BLAS.scal(a, bcWeightsArrTarget(ind))
            BLAS.axpy(b, bcGradientWeightsSumArr.value(ind), bcWeightsArrTarget(ind))
          }
          Iterator(Some(bcWeightsArrTarget))
        }, true).treeAggregate(Option.empty[Array[Vector]])(reducer, reducer, treeAggregateDepth).get

        for(ind <- 0 until numClasses) {
          // old method
          BLAS.scal(1.0 / numPartitionsRenew, weightsArrUpdated(ind))
          (weightsArrUpdated(ind), weightsArr(ind)) match {
            case (wo: DenseVector, wt: DenseVector) =>
              Array.copy(wo.values, 0, wt.values, 0, numWeights)
            case _ =>
              throw new IllegalArgumentException("not implemented")
          }
        }
        bcGradientWeightsSumArr.destroy(blocking = false)

      }
      bcWeightsArr.destroy(blocking = false)
      iter += 1
    }

    (weightsArr, interceptArr)

  }

  override def fit(dataset: Dataset[_]): OVRLinearScopeModel = {
    transformSchema(dataset.schema)
    // numFeatures
    val featuresColIndex = dataset.schema.fieldNames.indexOf(${featuresCol})
    val numFeatures = dataset.first().asInstanceOf[Row].get(featuresColIndex).asInstanceOf[newVector].size
    // training data
    val trainData: RDD[(Double, Vector)] = dataset.select(
      col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map { case Row(label: Double, features: newVector) =>
      (label, Vectors.fromML(features))
    }
    // initialWeightsArr
    val initialWeightsArr = Array.fill(${numClasses})(Vectors.zeros(numFeatures))
    // restartFrequency
    restartFrequencyHint()
    // loss func
    val lossFunc = getLossFunction(${loss}, ${numClasses})
    // train
    val (weightsArr, interceptArr) = train(trainData, initialWeightsArr, lossFunc, ${numClasses}, ${stepSize}, ${maxIter},
      ${regParam}, ${elasticNetParam}, ${factor}, ${numPartitions}, ${fitIntercept}, ${restartFrequency},
      ${aggregationDepth}, ${tol})
    // model
    val model = new OVRLinearScopeModel(uid, weightsArr, interceptArr)
    copyValues(model)
  }

  // TODO: add initial intercept
  def fit(dataset: Dataset[_], initWeightsArr: Array[newVector]): OVRLinearScopeModel = {
    transformSchema(dataset.schema)
    // training data
    val trainData: RDD[(Double, Vector)] = dataset.select(
      col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map { case Row(label: Double, features: newVector) =>
      (label, Vectors.fromML(features))
    }
    // initialWeightsArr
    val initialWeightsArr = initWeightsArr.map(w => Vectors.fromML(w))
    // restartFrequency
    restartFrequencyHint()
    // loss func
    val lossFunc = getLossFunction(${loss}, ${numClasses})
    // train
    val (weightsArr, interceptArr) = train(trainData, initialWeightsArr, lossFunc, ${numClasses}, ${stepSize}, ${maxIter},
      ${regParam}, ${elasticNetParam}, ${factor}, ${numPartitions}, ${fitIntercept}, ${restartFrequency},
      ${aggregationDepth}, ${tol})
    // model
    val model = new OVRLinearScopeModel(uid, weightsArr, interceptArr)
    copyValues(model)
  }

  override def copy(extra: ParamMap): OVRLinearScope = defaultCopy(extra)

}

object OVRLinearScope extends DefaultParamsReadable[OVRLinearScope] {
  override def load(path: String): OVRLinearScope = super.load(path)
}

class OVRLinearScopeModel(override val uid: String, val coefficientsArr: Array[Vector], val interceptArr: Array[Double])
  extends Model[OVRLinearScopeModel] with OVRLinearScopeParams with HasRawPredictionCol with HasProbabilityCol with HasPredictionCol with MLWritable {

  import OVRLinearScopeModel._

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new newVectorUDT)
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(${rawPredictionCol}),s"prediction column $rawPredictionCol already exists.")
    require(!fieldNames.contains(${probabilityCol}),s"probability column $probabilityCol already exists.")
    require(!fieldNames.contains(${predictionCol}),s"prediction column $predictionCol already exists.")
    val outputFields = new ArrayBuffer[StructField]()
    schema.fields.foreach { structField => outputFields += structField }
    outputFields += new StructField(${rawPredictionCol}, new newVectorUDT, true)
    outputFields += new StructField(${probabilityCol}, new newVectorUDT, true)
    outputFields += new StructField(${predictionCol}, DoubleType, true)
    StructType(outputFields.toArray)
  }

  protected def predict(features: Vector): newVector = {
    val probs = Array.fill(${numClasses})(0.0)
    for(ind <- 0 until ${numClasses}) {
      probs(ind) = BLAS.dot(coefficientsArr(ind), features) + interceptArr(ind)
    }
    newVectors.dense(probs)
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
    val dataRDD = if (${loss}.contains("SVM")) {
      dataset.toDF.rdd.map { row =>
        val features = Vectors.fromML(row.getAs[newVector](featuresColIndex))
        val probs = predict(features)
        // TODO: methods from probs to pred
        val pred = probs.argmax.toDouble
        Row.fromSeq(row.toSeq ++ Row(probs, probs, pred).toSeq)
      }
    } else {
      throw new IllegalArgumentException("not implemented")
    }
    dataset.sparkSession.createDataFrame(dataRDD, newSchema)
  }

  override def copy(extra: ParamMap): OVRLinearScopeModel = {
    val copied = new OVRLinearScopeModel(uid, coefficientsArr, interceptArr)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new OVRLinearScopeModelWriter(this)

}

object OVRLinearScopeModel extends MLReadable[OVRLinearScopeModel] {

  private[OVRLinearScopeModel]
  class OVRLinearScopeModelWriter(instance: OVRLinearScopeModel) extends MLWriter {

    private case class Data(coefficientsSeq: Seq[Vector], interceptSeq: Seq[Double])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.coefficientsArr.toSeq, instance.interceptArr.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }

  }

  private[OVRLinearScopeModel]
  class OVRLinearScopeModelReader extends MLReader[OVRLinearScopeModel] {

    private val className = classOf[OVRLinearScopeModel].getName

    override def load(path: String): OVRLinearScopeModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficientsSeq: Seq[Vector], interceptSeq: Seq[Double]) = data.select("coefficientsSeq", "interceptSeq").head()
      val model = new OVRLinearScopeModel(metadata.uid, coefficientsSeq.toArray, interceptSeq.toArray)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

  override def read: MLReader[OVRLinearScopeModel] = new OVRLinearScopeModelReader

  override def load(path: String): OVRLinearScopeModel = super.load(path)

}


