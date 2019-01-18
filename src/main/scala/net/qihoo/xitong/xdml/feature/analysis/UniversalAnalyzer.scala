package net.qihoo.xitong.xdml.feature.analysis

import breeze.numerics.log2
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object UniversalAnalyzer {

  private final val sampleCount = 50000

  case class NumFeatSummary(name: String,
                            countAll: Long,
                            countNotNull: Long,
                            mean: Double,
                            std: Double,
                            skewness: Double,
                            kurtosis: Double,
                            min: Double,
                            max: Double,
                            median: Double,
                            quantileArray: Array[Double],
                            corr: Double,
                            auc: Double)

  case class CatFeatSummary(name: String,
                            countAll: Long,
                            countNotNull: Long,
                            concernedCategories: Array[(String, (Long, Int))],
                            mi: Double,
                            auc: Double)

  // Routine of computing AUC for categorical features
  private def stat2auc(stat: Array[(Long, Int)]): Double = {
    val statSorted = stat.sortBy{ case (n, p) => - 1.0 * p / n }
    val scanNeg = statSorted.reverse.map{ case (n, p) => n - p }.scan(0L)(_+_).reverse
    val countNeg = scanNeg(0)
    var countPos = 0
    var sum = 0.0
    for(i <- statSorted.indices) {
      val p = statSorted(i)._2
      val n = statSorted(i)._1
      countPos += p
      sum += 0.5*(n-p)*p+1.0*p*scanNeg(i+1)
    }
    sum / countNeg / countPos
  }

  private def stat2auc2(stat: Array[(Long, Int)]): Double = {
    val statSorted = stat.sortBy{ case (n, p) => 1.0 * p / n }
    var posCnt = 0L
    var negCnt = 0L
    var rankSum = 0.0
    for(i <- statSorted.indices) {
      val rank = posCnt + negCnt + 0.5 * (statSorted(i)._1 + 1)
      rankSum += statSorted(i)._2 * rank
      posCnt += statSorted(i)._2
      negCnt += statSorted(i)._1 - statSorted(i)._2
    }
    (rankSum - 0.5 * posCnt * (posCnt + 1)) / posCnt / negCnt
  }

  // Routine of computing MI for categorical features
  private def stat2mi(stat: Array[(Long, Int)]): Double = {
    val allCount = stat.map{ case (lc, lpc) => lc }.sum
    val px = stat.map{ case (lc, lpc) => 1.0 * lc / allCount }
    val py1 = 1.0 * stat.map{ case (lc, lpc) => lpc }.sum / allCount
    var sum = 0.0
    for(i <- stat.indices) {
      val pxy1 = 1.0 * stat(i)._2 / allCount
      if (pxy1 > 0) sum += pxy1 * log2(pxy1 / py1 / px(i))
      val pxy0 = 1.0 * (stat(i)._1 - stat(i)._2) / allCount
      if (pxy0 > 0) sum += pxy0 * log2(pxy0 / (1-py1) / px(i))
    }
    sum
  }

  private val CatDefault = "XDMLCatDefault"


  ////////////////////////////////////////   With Label   ///////////////////////////////////////////////////


  // Statistics for numerical features
  class NumericalStatistics extends Serializable {

    // non-null count
    var n: Long = 0L
    // central moments of order 1
    var M1: Double = 0.0
    // central moments of order 2
    var M2: Double = 0.0
    // central moments of order 3
    var M3: Double = 0.0
    // central moments of order 4
    var M4: Double = 0.0
    // central moments of order 1 for label
    var M1Label: Double = 0.0
    // central moments of order 2 for label
    var M2Label: Double = 0.0
    // central moments of order 2 for feat and label
    var M2FeatLabel: Double = 0.0

    def countForNotNull(): Long = n

    def mean(): Double = M1

    def biasedVar(): Double = M2/n

    def unbiasedVar(): Double = M2/(n-1)

    def biasedStd(): Double = math.sqrt(biasedVar())

    def unbiasedStd(): Double = math.sqrt(unbiasedVar())

    def skewness(): Double = math.sqrt(n.toDouble) * M3 / math.pow(M2, 1.5)

    def kurtosis(): Double = n * M4 / (M2*M2) - 3.0

    def corrWithLabel(): Double = M2FeatLabel / math.sqrt(M2 * M2Label)

    def invert(x: Double, label: Double): NumericalStatistics = {
      val n_org = n
      n += 1
      val delta = x - M1
      val delta_n = delta / n
      val delta_n2 = delta_n * delta_n
      val term1 = delta * delta_n * n_org
      M1 += delta_n
      M4 += term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3
      M3 += term1 * delta_n * (n - 2) - 3 * delta_n * M2
      M2 += term1
      // label
      val delta_label = label - M1Label
      val delta_n_label  = delta_label / n
      M1Label += delta_n_label
      M2Label += delta_label * delta_n_label * n_org
      M2FeatLabel += delta * (label - M1Label)
      this
    }

    def merge(ns: NumericalStatistics): NumericalStatistics = {
      val combined = new NumericalStatistics
      combined.n = n + ns.n
      val delta = ns.M1 - M1
      val delta2 = delta * delta
      val delta3 = delta * delta2
      val delta4 = delta2 * delta2
      val na = 1.0 * n / combined.n
      val nb = 1.0 * ns.n / combined.n
      combined.M1 = na * M1 + nb * ns.M1
      combined.M2 = M2 + ns.M2 + delta2 * na * ns.n
      combined.M3 = M3 + ns.M3 + delta3 * na * nb * (n - ns.n)
      combined.M3 += 3.0 * delta * (na * ns.M2 - nb * M2)
      combined.M4 = M4 + ns.M4 + delta4 * na * nb * (na * n - na * ns.n + nb * ns.n)
      combined.M4 += 6.0 * delta2 * (na * na * ns.M2 + nb * nb * M2) + 4.0 * delta * (na * ns.M3 - nb * M3)
      // label
      val delta_label = ns.M1Label - M1Label
      combined.M1Label = na * M1Label + nb * ns.M1Label
      combined.M2FeatLabel = M2FeatLabel + ns.M2FeatLabel + delta * delta_label * na * ns.n
      combined.M2Label = M2Label + ns.M2Label + delta_label * delta_label * na * ns.n
      combined
    }

  }


  // WARNING: requirement: binary indexed label
  private def fitDenseWithLabel(dataDF: DataFrame,
                labelColName: String,
                catFeatColNames: Array[String],
                numFeatColNames: Array[String],
                percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
                relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) = {

    val initValue = (
      Array.fill(catFeatColNames.length)((HashMap[String, Long](), HashMap[String, Int]())),
      Array.fill(numFeatColNames.length)(new NumericalStatistics),
      Array.fill(numFeatColNames.length)(new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)),
      0L
    )

    def seqOp(arrs: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
              row: Row): (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long) = {
      // 0: label
      // 1 .. catFeatColNames.length: catFeat
      // catFeatColNames.length+1 .. catFeatColNames.length+numFeatColNames.length: numFeat
      if (row.isNullAt(0)) {
        arrs
      } else {
        val label = row.getString(0).toDouble
        val countArr = arrs._1
        val nsArr = arrs._2
        val qsArr = arrs._3
        // categorical features
        for (i <- countArr.indices) {
          val elem = countArr(i)
          if (!row.isNullAt(i+1)) {
            val feat = row.getString(i+1)
            if (elem._1.contains(feat)) {
              elem._1(feat) += 1
              if (label > 0.5) elem._2(feat) += 1
            } else {
              elem._1(feat) = 1
              elem._2(feat) = if (label > 0.5) 1 else 0
            }
          }
        }
        val baseInd = 1+countArr.length
        // numerical features
        for (j <- qsArr.indices) {
          if (!row.isNullAt(j+baseInd)) {
            val v = row.getDouble(j+baseInd)
            if (!v.isNaN) {
              nsArr(j) = nsArr(j).invert(v, label) // ns
              qsArr(j) = qsArr(j).insert(v) // qs
            }
          }
        }
        (countArr, nsArr, qsArr, arrs._4 + 1)
      }
    }

    def comOp(arrs1: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
              arrs2: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long))
    : (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long) = {
      val countArr1 = arrs1._1
      val nsArr1 = arrs1._2
      val qsArr1 = arrs1._3
      val countArr2 = arrs2._1
      val nsArr2 = arrs2._2
      val qsArr2 = arrs2._3
      for (i <- countArr1.indices) {
        val elem1 = countArr1(i)
        val elem2 = countArr2(i)
        elem2._1.foreach { case (k, v) => if (elem1._1.contains(k)) elem1._1(k) += v else elem1._1(k) = v }
        elem2._2.foreach { case (k, v) => if (elem1._2.contains(k)) elem1._2(k) += v else elem1._2(k) = v }
      }
      val nsArr3 = nsArr1.zip(nsArr2).map { case (s1, s2) => s1.merge(s2) }
      val qsArr3 = qsArr1.zip(qsArr2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
      (countArr1, nsArr3, qsArr3, arrs1._4 + arrs2._4)
    }

    val res = dataDF.select(labelColName, (catFeatColNames++numFeatColNames):_*).rdd.treeAggregate(initValue)(seqOp, comOp)

    reportSummary(catFeatColNames, numFeatColNames, res, percentages)

  }

  //WARNING: if hasLabel is false, labelColName is not used, labelColName can be empty string.
  def fitDense(dataDF: DataFrame,
               hasLabel: Boolean,
               labelColName: String,
               catFeatColNames: Array[String],
               numFeatColNames: Array[String],
               percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
               relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) ={
    if(hasLabel){
      val isBinary = isBinaryClassification(dataDF, labelColName)
      if(isBinary){
        fitDenseWithLabel(dataDF, labelColName, catFeatColNames, numFeatColNames, percentages, relativeError)
      }else{
        fitDenseNoLabel(dataDF, catFeatColNames, numFeatColNames, percentages, relativeError)
      }
    }else{
      fitDenseNoLabel(dataDF, catFeatColNames, numFeatColNames, percentages, relativeError)
    }
  }

  // WARNING: requirement: binary indexed label
  private def fitSparseWithLabel(dataRDD: RDD[HashMap[String, String]],
                labelColName: String,
                catFeatColNames: Array[String],
                numFeatColNames: Array[String],
                sparseType: String = "default",
                percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
                relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) = {

    val initValue = (
      Array.fill(catFeatColNames.length)((HashMap[String, Long](), HashMap[String, Int]())),
      Array.fill(numFeatColNames.length)(new NumericalStatistics),
      Array.fill(numFeatColNames.length)(new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)),
      0L
    )

    def seqOpForDefault(arrs: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
                        row: HashMap[String, String]): (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long) = {
      val labelWrapper = row.get(labelColName)
      if (labelWrapper.isEmpty) {
        arrs
      } else {
        val label = labelWrapper.get.toDouble
        val countArr = arrs._1
        val nsArr = arrs._2
        val qsArr = arrs._3
        // categorical features
        for (i <- countArr.indices) {
          val elem = countArr(i)
          val featWrapper = row.get(catFeatColNames(i))
          if (featWrapper.isDefined) {
            val feat = featWrapper.get
            if (elem._1.contains(feat)) {
              elem._1(feat) += 1
              if (label > 0.5) elem._2(feat) += 1
            } else {
              elem._1(feat) = 1
              elem._2(feat) = if (label > 0.5) 1 else 0
            }
          } else {
            val feat = CatDefault
            if (elem._1.contains(feat)) {
              elem._1(feat) += 1
              if (label > 0.5) elem._2(feat) += 1
            } else {
              elem._1(feat) = 1
              elem._2(feat) = if (label > 0.5) 1 else 0
            }
          }
        }
        // numerical features
        for (j <- qsArr.indices) {
          val featWrapper = row.get(numFeatColNames(j))
          if (featWrapper.isDefined) {
            val v = featWrapper.get.toDouble
            nsArr(j) = nsArr(j).invert(v, label) // ns
            qsArr(j) = qsArr(j).insert(v) // qs
          }
        }
        (countArr, nsArr, qsArr, arrs._4 + 1)
      }
    }

    def seqOpForMissing(arrs: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
                        row: HashMap[String, String]): (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long) = {
      val labelWrapper = row.get(labelColName)
      if (labelWrapper.isEmpty) {
        arrs
      } else {
        val label = labelWrapper.get.toDouble
        val countArr = arrs._1
        val nsArr = arrs._2
        val qsArr = arrs._3
        // categorical features
        for (i <- countArr.indices) {
          val elem = countArr(i)
          val featWrapper = row.get(catFeatColNames(i))
          if (featWrapper.isDefined) {
            val feat = featWrapper.get
            if (elem._1.contains(feat)) {
              elem._1(feat) += 1
              if (label > 0.5) elem._2(feat) += 1
            } else {
              elem._1(feat) = 1
              elem._2(feat) = if (label > 0.5) 1 else 0
            }
          }
        }
        // numerical features
        for (j <- qsArr.indices) {
          val featWrapper = row.get(numFeatColNames(j))
          if (featWrapper.isDefined) {
            val v = featWrapper.get.toDouble
            nsArr(j) = nsArr(j).invert(v, label) // ns
            qsArr(j) = qsArr(j).insert(v) // qs
          }
        }
        (countArr, nsArr, qsArr, arrs._4 + 1)
      }
    }

    def comOp(arrs1: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
              arrs2: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long))
    : (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long) = {
      val countArr1 = arrs1._1
      val nsArr1 = arrs1._2
      val qsArr1 = arrs1._3
      val countArr2 = arrs2._1
      val nsArr2 = arrs2._2
      val qsArr2 = arrs2._3
      for (i <- countArr1.indices) {
        val elem1 = countArr1(i)
        val elem2 = countArr2(i)
        elem2._1.foreach { case (k, v) => if (elem1._1.contains(k)) elem1._1(k) += v else elem1._1(k) = v }
        elem2._2.foreach { case (k, v) => if (elem1._2.contains(k)) elem1._2(k) += v else elem1._2(k) = v }
      }
      val nsArr3 = nsArr1.zip(nsArr2).map { case (s1, s2) => s1.merge(s2) }
      val qsArr3 = qsArr1.zip(qsArr2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
      (countArr1, nsArr3, qsArr3, arrs1._4 + arrs2._4)
    }

    val res =
      sparseType match {
        case "default" => dataRDD.treeAggregate(initValue)(seqOpForDefault, comOp)
        case "missing" => dataRDD.treeAggregate(initValue)(seqOpForMissing, comOp)
        case _ => throw new IllegalArgumentException("sparse type unknown")
      }

    reportSummary(catFeatColNames, numFeatColNames, res, percentages)

  }

  //WARNING: if hasLabel is false, labelColName is not used, labelColName can be empty string.
  def fitSparse(dataRDD: RDD[HashMap[String, String]],
                hasLabel: Boolean,
                labelColName: String,
                catFeatColNames: Array[String],
                numFeatColNames: Array[String],
                sparseType: String = "default",
                percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
                relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) ={
    if(hasLabel){
      val isBinary = isBinaryClassification(dataRDD, labelColName)
      if(isBinary){
        fitSparseWithLabel(dataRDD, labelColName, catFeatColNames, numFeatColNames, sparseType, percentages, relativeError)
      }else{
        fitSparseNoLabel(dataRDD, catFeatColNames, numFeatColNames, sparseType, percentages, relativeError)
      }
    }else{
      fitSparseNoLabel(dataRDD, catFeatColNames, numFeatColNames, sparseType, percentages, relativeError)
    }
  }


  private def reportSummary(catFeatColNames: Array[String],
                    numFeatColNames: Array[String],
                    res: (Array[(HashMap[String, Long], HashMap[String, Int])], Array[NumericalStatistics], Array[QuantileSummaries], Long),
                    percentages: Array[Double]): (Array[CatFeatSummary], Array[NumFeatSummary]) = {

    val countAll = res._4
    val resForCat = res._1
    val catFeatSummaryArray =
      for(i <- catFeatColNames.indices) yield {
        val name = catFeatColNames(i)
        val levelCount = resForCat(i)._1
        val levelPosHitCount = resForCat(i)._2
        val countForNotNullCat = levelCount.values.sum
        val levelCountWithPosHitCountSorted = levelCount.map { case (k, v) => (k, (v, levelPosHitCount(k))) }
          .toArray.sortBy { tup => - tup._2._2.toDouble / tup._2._1.toDouble }
        val stat = levelCountWithPosHitCountSorted.map(tup => tup._2)
        val mi = stat2mi(stat)
        val auc = stat2auc(stat)
        CatFeatSummary(name, countAll, countForNotNullCat, levelCountWithPosHitCountSorted.take(200), mi, auc)
      }

    val nsArrForNum = res._2
    val qsArrForNum = res._3
    val numFeatSummaryArray =
      for(i <- numFeatColNames.indices) yield {
        val name = numFeatColNames(i)
        val ns = nsArrForNum(i)
        val qs = qsArrForNum(i)
        val quanArr = percentages.flatMap(qs.query)
        val min = qs.query(0.0).get
        val max = qs.query(1.0).get
        val median = qs.query(0.5).get
        val corr = ns.corrWithLabel()
        val auc = -1 // TODO: to be done
        NumFeatSummary(name, countAll, ns.countForNotNull(), ns.mean(), ns.unbiasedStd(), ns.skewness(), ns.kurtosis(),
                        min, max, median, quanArr, corr, auc)
      }

    (catFeatSummaryArray.toArray, numFeatSummaryArray.toArray)

  }

  private def isBinaryClassification(orgDF: DataFrame, labelColName: String): Boolean ={
    val sampledData = orgDF.rdd.takeSample(false, sampleCount)
    val hashSet = mutable.HashSet[Double]()
    val labelIndex = orgDF.schema.fieldIndex(labelColName)
    sampledData.foreach{ row =>
      if(!row.isNullAt(labelIndex)) {
        val label = row.getString(labelIndex).toDouble
        hashSet.add(label)
      }
      if (hashSet.size > 2)
        return false
    }
    if(hashSet.size <= 2)
      true
    else
      false
  }

  private def isBinaryClassification(orgRdd: RDD[HashMap[String, String]], labelColName: String): Boolean={
    val sampledData  = orgRdd.takeSample(false, sampleCount)
    val hashSet = mutable.HashSet[Double]()
    sampledData.foreach{ row =>
      val labelWrapper = row.get(labelColName)
      if(labelWrapper.isDefined) {
        val label = labelWrapper.get.toDouble
        hashSet.add(label)
      }
      if(hashSet.size > 2)
        return false
    }
    if(hashSet.size <= 2)
      true
    else
      false
  }


  ////////////////////////////////////////   Without Label   ///////////////////////////////////////////////////


  class NumericalStatisticsNoLabel extends Serializable {

    // non-null count
    var n: Long = 0L
    // central moments of order 1
    var M1: Double = 0.0
    // central moments of order 2
    var M2: Double = 0.0
    // central moments of order 3
    var M3: Double = 0.0
    // central moments of order 4
    var M4: Double = 0.0

    def countForNotNull(): Long = n

    def mean(): Double = M1

    def biasedVar(): Double = M2/n

    def unbiasedVar(): Double = M2/(n-1)

    def biasedStd(): Double = math.sqrt(biasedVar())

    def unbiasedStd(): Double = math.sqrt(unbiasedVar())

    def skewness(): Double = math.sqrt(n.toDouble) * M3 / math.pow(M2, 1.5)

    def kurtosis(): Double = n * M4 / (M2*M2) - 3.0

    def invert(x: Double): NumericalStatisticsNoLabel = {
      val n_org = n
      n += 1
      val delta = x - M1
      val delta_n = delta / n
      val delta_n2 = delta_n * delta_n
      val term1 = delta * delta_n * n_org
      M1 += delta_n
      M4 += term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3
      M3 += term1 * delta_n * (n - 2) - 3 * delta_n * M2
      M2 += term1
      this
    }

    def merge(ns: NumericalStatisticsNoLabel): NumericalStatisticsNoLabel = {
      val combined = new NumericalStatisticsNoLabel
      combined.n = n + ns.n
      val delta = ns.M1 - M1
      val delta2 = delta * delta
      val delta3 = delta * delta2
      val delta4 = delta2 * delta2
      val na = 1.0 * n / combined.n
      val nb = 1.0 * ns.n / combined.n
      combined.M1 = na * M1 + nb * ns.M1
      combined.M2 = M2 + ns.M2 + delta2 * na * ns.n
      combined.M3 = M3 + ns.M3 + delta3 * na * nb * (n - ns.n)
      combined.M3 += 3.0 * delta * (na * ns.M2 - nb * M2)
      combined.M4 = M4 + ns.M4 + delta4 * na * nb * (na * n - na * ns.n + nb * ns.n)
      combined.M4 += 6.0 * delta2 * (na * na * ns.M2 + nb * nb * M2) + 4.0 * delta * (na * ns.M3 - nb * M3)
      combined
    }

  }


  private def fitDenseNoLabel(dataDF: DataFrame,
                      catFeatColNames: Array[String],
                      numFeatColNames: Array[String],
                      percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
                      relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) = {

    val initValue = (
      Array.fill(catFeatColNames.length)(new HashMap[String, Long]()),
      Array.fill(numFeatColNames.length)(new NumericalStatisticsNoLabel),
      Array.fill(numFeatColNames.length)(new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)),
      0L
    )

    def seqOp(arrs: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
              row: Row): (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long) = {
      // 0 .. catFeatColNames.length-1: catFeat
      // catFeatColNames.length .. catFeatColNames.length+numFeatColNames.length-1: numFeat
      val countArr = arrs._1
      val nsArr = arrs._2
      val qsArr = arrs._3
      // categorical features
      for (i <- countArr.indices) {
        val elem = countArr(i)
        if (!row.isNullAt(i)) {
          val feat = row.getString(i)
          if (elem.contains(feat)) {
            elem(feat) += 1
          } else {
            elem(feat) = 1
          }
        }
      }
      val baseInd = countArr.length
      // numerical features
      for (j <- qsArr.indices) {
        if (!row.isNullAt(j+baseInd)) {
          val v = row.getDouble(j+baseInd)
          if (!v.isNaN) {
            nsArr(j) = nsArr(j).invert(v) // ns
            qsArr(j) = qsArr(j).insert(v) // qs
          }
        }
      }
      (countArr, nsArr, qsArr, arrs._4 + 1)
    }


    def comOp(arrs1: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
              arrs2: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long))
    : (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long) = {
      val countArr1 = arrs1._1
      val nsArr1 = arrs1._2
      val qsArr1 = arrs1._3
      val countArr2 = arrs2._1
      val nsArr2 = arrs2._2
      val qsArr2 = arrs2._3
      for (i <- countArr1.indices) {
        val elem1 = countArr1(i)
        val elem2 = countArr2(i)
        elem2.foreach { case (k, v) => if (elem1.contains(k)) elem1(k) += v else elem1(k) = v }
      }
      val nsArr3 = nsArr1.zip(nsArr2).map { case (s1, s2) => s1.merge(s2) }
      val qsArr3 = qsArr1.zip(qsArr2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
      (countArr1, nsArr3, qsArr3, arrs1._4 + arrs2._4)
    }

    val columns = (catFeatColNames++numFeatColNames).map { colName => col(colName) }
    val res = dataDF.select(columns:_*).rdd.treeAggregate(initValue)(seqOp, comOp)

    reportSummaryNoLabel(catFeatColNames, numFeatColNames, res, percentages)

  }


  private def fitSparseNoLabel(dataRDD: RDD[HashMap[String, String]],
                        catFeatColNames: Array[String],
                        numFeatColNames: Array[String],
                        sparseType: String = "default",
                        percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
                        relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary]) = {

    val initValue = (
      Array.fill(catFeatColNames.length)(new HashMap[String, Long]()),
      Array.fill(numFeatColNames.length)(new NumericalStatisticsNoLabel),
      Array.fill(numFeatColNames.length)(new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)),
      0L
    )

    def seqOpForDefault(arrs: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
                        row: HashMap[String, String]): (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long) = {
      val countArr = arrs._1
      val nsArr = arrs._2
      val qsArr = arrs._3
      // categorical features
      for (i <- countArr.indices) {
        val elem = countArr(i)
        val featWrapper = row.get(catFeatColNames(i))
        if (featWrapper.isDefined) {
          val feat = featWrapper.get
          if (elem.contains(feat)) {
            elem(feat) += 1
          } else {
            elem(feat) = 1
          }
        } else {
          val feat = CatDefault
          if (elem.contains(feat)) {
            elem(feat) += 1
          } else {
            elem(feat) = 1
          }
        }
      }
      // numerical features
      for (j <- qsArr.indices) {
        val featWrapper = row.get(numFeatColNames(j))
        if (featWrapper.isDefined) {
          val v = featWrapper.get.toDouble
          nsArr(j) = nsArr(j).invert(v) // ns
          qsArr(j) = qsArr(j).insert(v) // qs
        }
      }
      (countArr, nsArr, qsArr, arrs._4 + 1)
    }

    def seqOpForMissing(arrs: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
                        row: HashMap[String, String]): (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long) = {
      val countArr = arrs._1
      val nsArr = arrs._2
      val qsArr = arrs._3
      // categorical features
      for (i <- countArr.indices) {
        val elem = countArr(i)
        val featWrapper = row.get(catFeatColNames(i))
        if (featWrapper.isDefined) {
          val feat = featWrapper.get
          if (elem.contains(feat)) {
            elem(feat) += 1
          } else {
            elem(feat) = 1
          }
        }
      }
      // numerical features
      for (j <- qsArr.indices) {
        val featWrapper = row.get(numFeatColNames(j))
        if (featWrapper.isDefined) {
          val v = featWrapper.get.toDouble
          nsArr(j) = nsArr(j).invert(v) // ns
          qsArr(j) = qsArr(j).insert(v) // qs
        }
      }
      (countArr, nsArr, qsArr, arrs._4 + 1)
    }

    def comOp(arrs1: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
              arrs2: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long))
    : (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long) = {
      val countArr1 = arrs1._1
      val nsArr1 = arrs1._2
      val qsArr1 = arrs1._3
      val countArr2 = arrs2._1
      val nsArr2 = arrs2._2
      val qsArr2 = arrs2._3
      for (i <- countArr1.indices) {
        val elem1 = countArr1(i)
        val elem2 = countArr2(i)
        elem2.foreach { case (k, v) => if (elem1.contains(k)) elem1(k) += v else elem1(k) = v }
      }
      val nsArr3 = nsArr1.zip(nsArr2).map { case (s1, s2) => s1.merge(s2) }
      val qsArr3 = qsArr1.zip(qsArr2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
      (countArr1, nsArr3, qsArr3, arrs1._4 + arrs2._4)
    }

    val res =
      sparseType match {
        case "default" => dataRDD.treeAggregate(initValue)(seqOpForDefault, comOp)
        case "missing" => dataRDD.treeAggregate(initValue)(seqOpForMissing, comOp)
        case _ => throw new IllegalArgumentException("sparse type unknown")
      }

    reportSummaryNoLabel(catFeatColNames, numFeatColNames, res, percentages)

  }


  private def reportSummaryNoLabel(catFeatColNames: Array[String],
                           numFeatColNames: Array[String],
                           res: (Array[HashMap[String, Long]], Array[NumericalStatisticsNoLabel], Array[QuantileSummaries], Long),
                           percentages: Array[Double]): (Array[CatFeatSummary], Array[NumFeatSummary]) = {
    val countAll = res._4
    val resForCat = res._1
    val catFeatSummaryArray =
      for(i <- catFeatColNames.indices) yield {
        val name = catFeatColNames(i)
        val map = resForCat(i)
        val countForNotNullCat = map.values.sum
        val top200 = map.toArray.sortBy(-_._2).take(200).map(tup => (tup._1, (tup._2, -1)))
        val mi = -1
        val auc = -1
        CatFeatSummary(name, countAll, countForNotNullCat, top200, mi, auc)
      }

    val nsArrForNum = res._2
    val qsArrForNum = res._3
    val numFeatSummaryArray =
      for(i <- numFeatColNames.indices) yield {
        val name = numFeatColNames(i)
        val ns = nsArrForNum(i)
        val qs = qsArrForNum(i)
        val quanArr = percentages.flatMap(qs.query)
        val min = qs.query(0.0).get
        val max = qs.query(1.0).get
        val median = qs.query(0.5).get
        val corr = -1
        val auc = -1
        NumFeatSummary(name, countAll, ns.countForNotNull(), ns.mean(), ns.unbiasedStd(), ns.skewness(), ns.kurtosis(),
          min, max, median, quanArr, corr, auc)
      }

    (catFeatSummaryArray.toArray, numFeatSummaryArray.toArray)

  }

  ////////////////////////////////////////   Among Features   ///////////////////////////////////////////////////

  private def computeKSParallel(df1: DataFrame, numFeatColNames1: Array[String],
                        df2: DataFrame, numFeatColNames2: Array[String]): Array[Double] = {
    val combinedFeatName = numFeatColNames1.zip(numFeatColNames2).map(tuple => tuple._1 + "_" + tuple._2)
    val bcCombinedFeatName = df1.sparkSession.sparkContext.broadcast(combinedFeatName)
    val firstDataPrefix = "x"
    val secondDataPrefix = "y"
    val delimiter = ":"
    val cols1 = numFeatColNames1.map { featColName =>
      col(featColName)
    }
    val cols2 = numFeatColNames2.map { featColName =>
      col(featColName)
    }
    val rdd1 = df1.select(cols1: _*).rdd.flatMap { row =>
      row.toSeq.zip(bcCombinedFeatName.value).map { case (value, featName) =>
        (featName, firstDataPrefix + delimiter + value)
      }
    }
    val rdd2 = df2.select(cols2: _*).rdd.flatMap { row =>
      row.toSeq.zip(bcCombinedFeatName.value).map { case (value, featName) =>
        (featName, secondDataPrefix + delimiter + value)
      }
    }
    val featNameToIndex = combinedFeatName.zipWithIndex.toMap
    val ksMap = rdd1.union(rdd2)
      .partitionBy(new KSPartitioner(featNameToIndex.size, featNameToIndex))
      .mapPartitions { iter =>
        val x = new ArrayBuffer[Double]()
        val y = new ArrayBuffer[Double]()
        val featNameWithValues = iter.toArray
        val featName = featNameWithValues(0)._1
        featNameWithValues.foreach { tuple =>
          val splitedValue = tuple._2.split(delimiter)
          if (!splitedValue(1).equals("null")) {
            if (splitedValue(0).contains(firstDataPrefix)) {
              x += splitedValue(1).toDouble
            } else if (splitedValue(0).contains(secondDataPrefix)) {
              y += splitedValue(1).toDouble
            }
          }
        }
        val ks = new KolmogorovSmirnovTest
        val ksValue = ks.kolmogorovSmirnovTest(x.toArray, y.toArray)
        Iterator((featName, ksValue))
      }
      .collect().toMap

    combinedFeatName.map { featName =>
      ksMap(featName)
    }
  }

  //WARNING: import net.qihoo.xitong.xdml.feature.analysis.KolmogorovSmirnovTest to avoid version problem when import KolmogorovSmirnovTest.class from jar
  def fitDenseKSForNum(df1: DataFrame, numFeatColNames1: Array[String],
                       df2: DataFrame, numFeatColNames2: Array[String]): Array[Double] = {
    assert(numFeatColNames1.length == numFeatColNames2.length, "Error: Feature Mismatch")
    computeKSParallel(df1, numFeatColNames1, df2, numFeatColNames2)
  }

  ////////////////////////////////////////   Grouped analysis   ///////////////////////////////////////////////////
  case class GroupFeatSummary(name: String,
                              reversePairRate: Double,
                              ndcgMap: HashMap[String, Double])

  def fitDenseGrouped(df: DataFrame,
                      labelColName: String,
                      groupColName: String,
                      featColNames: Array[String],
                      topKs: Array[Int],
                      gainFunc: (Int) => Double,
                      discountFunc: (Int) => Double): Array[GroupFeatSummary] = {
    val selectedColumns = new ArrayBuffer[Column]()
    selectedColumns += col(labelColName).cast(IntegerType)
    selectedColumns += col(groupColName)
    featColNames.foreach { colName =>
      selectedColumns += col(colName)
    }
    val labelIndex = 0
    val groupIndex = 1
    val featStartIndex = 2
    val featSize = featColNames.length
    val (pairs, ndcgPairs, count) =
      df.select(selectedColumns: _*).rdd.map(row => (row.get(groupIndex).toString, row)).groupByKey()
        .map { case (qid, iter) =>
          val rows = iter.toArray
          val filtedRows = rows.filter(row => !row.isNullAt(labelIndex))
          val pairs = computePositiveAndReversePair(filtedRows, featStartIndex, featSize, labelIndex)
          val ndcgPairs = computeNDCG(filtedRows, featStartIndex, featSize, labelIndex, topKs, gainFunc, discountFunc)
          //( [(positionPair,reversePair)],[([ndcgDesc],[ndcgAsc])],count)
          (pairs, ndcgPairs, 1)
        }
        .treeReduce { case ((pairs1, ndcgPairs1, count1), (pairs2, ndcgPairs2, count2)) =>
          val pairs = pairs1.zip(pairs2).map { case (pair1, pair2) =>
            (pair1._1 + pair2._1, pair1._2 + pair2._2)
          }
          val ndcgPairs = ndcgPairs1.zip(ndcgPairs2).map{ case (ndcgPair1, ndcgPair2) =>
            val ndcgDesc = ndcgPair1._1.zip(ndcgPair2._1).map{ case(ndcg1, ndcg2) => ndcg1 + ndcg2}
            val ndcgAsc =  ndcgPair1._2.zip(ndcgPair2._2).map{ case(ndcg1, ndcg2) => ndcg1 + ndcg2}
            (ndcgDesc, ndcgAsc)
          }
          val count = count1 + count2
          (pairs, ndcgPairs, count)
        }

    val metricPair = pairs.map { case (positivePairCount, reversePairCount) =>
      if (reversePairCount == 0) -1 else positivePairCount.toDouble / reversePairCount
    }

    val metricNDCG = ndcgPairs.map{ case (ndcgDesc, ndcgAsc) =>
      ndcgDesc.zip(ndcgAsc).map{ case (ndcg1, ndcg2) =>
        (ndcg1/count, ndcg2/count)
      }
    }

    reportGroupFeatSummary(featColNames, metricPair, metricNDCG, topKs)

  }

  private def computePositiveAndReversePair(rows: Array[Row],
                                            featStartIndex: Int,
                                            featSize: Int,
                                            labelIndex: Int): Array[(Int, Int)] = {
    val groupSize = rows.length
    (0 until featSize).map { offset =>
      val featIndex = featStartIndex + offset
      var positivePairCount = 0
      var reversePairCount = 0
      for (i <- 0 until groupSize - 1) {
        val row1 = rows(i)
        if (!row1.isNullAt(featIndex)) {
          val rating1 = row1.getInt(labelIndex)
          val feat1 = row1.getDouble(featIndex)
          for (j <- i + 1 until groupSize) {
            val row2 = rows(j)
            if (!row2.isNullAt(featIndex)) {
              val rating2 = row2.getInt(labelIndex)
              val feat2 = row2.getDouble(featIndex)
              val pairValue = (rating1 - rating2) * (feat1 - feat2)
              if (pairValue > 0) {
                positivePairCount += 1
              } else if (pairValue < 0) {
                reversePairCount += 1
              }
            }
          }
        }
      }
      (positivePairCount, reversePairCount)
    }.toArray
  }

  private def computeNDCG(rows: Array[Row],
                          featStartIndex: Int,
                          featSize: Int,
                          labelIndex: Int,
                          topKs: Array[Int],
                          gainFunc: (Int) => Double,
                          discountFunc: (Int) => Double): Array[(Array[Double], Array[Double])] = {
    val groupSize = rows.length
    val numTopK = topKs.length
    val minTopK = topKs.min
    //if number of query result less than minPos, direct return ndcg = 0.0
    if (groupSize < minTopK) {
      Array.fill[(Array[Double], Array[Double])](featSize)((Array.fill[Double](numTopK)(0.0), Array.fill[Double](numTopK)(0.0)))
    } else {
      val ratingIdeal = rows.map(row => row.getInt(labelIndex)).sortWith(_ > _) // TODO: memory issue
      val idcgs = computeDCG(ratingIdeal, gainFunc, discountFunc, topKs)
      //compute ndcg with feature descending order and feature ascending order
      val ndcgPairs = (0 until featSize).map { offset =>
        val featIndex = featStartIndex + offset
        val filtedRows = rows.filter(row => !row.isNullAt(featIndex)).map(row => (row.getDouble(featIndex), row.getInt(labelIndex)))
        val ratingOrderedDesc = filtedRows.sortBy(tuple => (-tuple._1, -tuple._2)) // TODO: memory issue
          .map(tuple => tuple._2)
        val dcgsDesc = computeDCG(ratingOrderedDesc, gainFunc, discountFunc, topKs)
        val ndcgDesc = normalizeDCG(dcgsDesc, idcgs)
        val ratingOrderedAsc = filtedRows.sortBy(tuple => (tuple._1, -tuple._2)) // TODO: memory issue
          .map(tuple => tuple._2)
        val dcgsAsc = computeDCG(ratingOrderedAsc, gainFunc, discountFunc, topKs)
        val ndcgAsc = normalizeDCG(dcgsAsc, idcgs)
        (ndcgDesc, ndcgAsc)
      }.toArray
      ndcgPairs
    }
  }

  private def computeDCG(ratingOrdered: Array[Int],
                         gainFunc: (Int) => Double,
                         discountFunc: (Int) => Double,
                         topKs: Array[Int]): Array[Double] = {
    val groupSize = ratingOrdered.length
    val maxTopK = topKs.max
    val p = math.min(groupSize, maxTopK)
    //compute gain in top p postion, p is min(docsize, max(rankPosition))
    val discountGainList = (0 until p).map { index =>
      if(ratingOrdered(index) >= 0) gainFunc(ratingOrdered(index)) / discountFunc(index) else 0.0
    }
    topKs.map { topK =>
      //if position more than query size, dcg is zero
      if(topK > groupSize) 0.0 else discountGainList.slice(0, topK).sum
    }
  }

  private def normalizeDCG(dcgs: Array[Double], idcgs: Array[Double]): Array[Double] = {
    dcgs.zip(idcgs).map{ case (dcg, idcg) => if (idcg == 0) 0.0 else dcg / idcg }
  }

  private def reportGroupFeatSummary(featColNames: Array[String], metricPair: Array[Double],
                                             metricNDCG: Array[Array[(Double, Double)]], topKs: Array[Int]): Array[GroupFeatSummary]={
    val groupFeatSummaryArray =
      for (i <- featColNames.indices) yield {
        val name = featColNames(i)
        val reversePairRate = metricPair(i)
        val ndcgs = metricNDCG(i)
        val ndcgMap = new HashMap[String, Double]()
        topKs.zip(ndcgs).foreach { case (topK, ndcg) =>
          val key = "ndcg" + topK
          val ndcgValue = math.max(ndcg._1, ndcg._2)
          ndcgMap.put(key, ndcgValue)
        }
        GroupFeatSummary(name, reversePairRate, ndcgMap)
      }
    groupFeatSummaryArray.toArray
  }

  private class KSPartitioner(parts: Int, keyToPartition: Map[String,Int]) extends Partitioner{
    override def numPartitions: Int = parts
    override def getPartition(key: Any):Int = {
      keyToPartition.getOrElse(key.toString, 0)
    }
  }

}
