package net.qihoo.xitong.xdml.feature.process

import net.qihoo.xitong.xdml.model.data.SchemaHandler
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelinePatch, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object FeatureProcessor {

  val transformed = "TransformedByNumericalFeatureProcessor"
  val encoded = "EncodedByCategoricalFeatureProcessor"

  val featsProcessedColName = "featuresProcessedByFeatureProcessor"
  val labelProcessedColName = "labelProcessedByFeatureProcessor"

  val pipelineStages = new ArrayBuffer[PipelineStage]()

  def appendPipelineStagesForOtherFeatures(schemaHandler: SchemaHandler): Unit = {
    if (schemaHandler.otherColNames.length > 0) {
      val columnEliminatorForOtherColNames = new ColumnEliminator()
        .setInputCols(schemaHandler.otherColNames)
      pipelineStages += columnEliminatorForOtherColNames
    }
  }

  def appendPipelineStagesForNumericalFeatures(schemaHandler: SchemaHandler,
                                               method: String,
                                               ifOnehot: Boolean,
                                               numBuckets: Int): Unit = {
    if (schemaHandler.numFeatColNames.length > 0) {
      if (method.equalsIgnoreCase("normalize")) {
        val numericStandardizer = new NumericStandardizer()
          .setInputCols(schemaHandler.numFeatColNames)
          .setOutputCols(schemaHandler.numFeatColNames.map(name => name + FeatureProcessor.transformed))
          .setDropInputCols(true)
        pipelineStages += numericStandardizer
      } else if (method.equalsIgnoreCase("bucketize")) {
        val numericBucketer = new NumericBucketer()
          .setInputCols(schemaHandler.numFeatColNames)
          .setOutputCols(schemaHandler.numFeatColNames.map(name => name + FeatureProcessor.transformed))
          .setDropInputCols(true)
          .setIndexOnly(!ifOnehot)
          .setNumBucketsArray(Array.fill(schemaHandler.numFeatColNames.length)(numBuckets))
        pipelineStages += numericBucketer
      } else if (method.equalsIgnoreCase("remain")) {
        val columnRenamer = new ColumnRenamer()
          .setInputCols(schemaHandler.numFeatColNames)
          .setOutputCols(schemaHandler.numFeatColNames.map(name => name + FeatureProcessor.transformed))
        pipelineStages += columnRenamer
      }
    }
  }

  def appendPipelineStagesForCategoricalFeatures(schemaHandler: SchemaHandler,
                                                 ifOnehot: Boolean,
                                                 categoriesReserved: Int): Unit = {
    if(schemaHandler.catFeatColNames.length > 0) {
      val catFeatColNamesEncoded = schemaHandler.catFeatColNames.map(name => name + FeatureProcessor.encoded)
      // TODO: remove useless features
      // TODO: coalesce tailed categories
      val categoryEncoder = new CategoryEncoder()
        .setInputCols(schemaHandler.catFeatColNames)
        .setOutputCols(catFeatColNamesEncoded)
        .setIndexOnly(!ifOnehot)
        .setDropInputCols(true)
        .setCategoriesReserved(categoriesReserved)
      pipelineStages += categoryEncoder
    }
  }

  def appendPipelineStagesForMultiCategoricalFeatures(schemaHandler: SchemaHandler,
                                                      ifOnehot: Boolean,
                                                      categoriesReserved: Int,
                                                      multiCatDelimiter: String): Unit = {
    if(schemaHandler.multiCatFeatColNames.length > 0) {
      val multiCatFeatColNamesEncoded = schemaHandler.multiCatFeatColNames.map(name => name + FeatureProcessor.encoded)
      // TODO: remove useless features
      // TODO: coalesce tailed categories
      val multiCategoryEncoder = new MultiCategoryEncoder()
        .setInputCols(schemaHandler.multiCatFeatColNames)
        .setOutputCols(multiCatFeatColNamesEncoded)
        .setDelimiter(multiCatDelimiter)
        .setIndexOnly(!ifOnehot)
        .setDropInputCols(true)
        .setCategoriesReserved(categoriesReserved)
      pipelineStages += multiCategoryEncoder
    }
  }

  def appendPipelineStagesForMergingFeatures(schemaHandler: SchemaHandler, ifMerge: Boolean): Unit = {
    if (ifMerge) {
      val numFeatColNamesTransformed = schemaHandler.numFeatColNames.map(name => name + FeatureProcessor.transformed)
      val catFeatColNamesEncoded = schemaHandler.catFeatColNames.map(name => name + FeatureProcessor.encoded)
      val multiCatFeatColNamesEncoded = schemaHandler.multiCatFeatColNames.map(name => name + FeatureProcessor.encoded)
      // merge
      val vectorAssembler = new VectorAssembler()
        .setInputCols(numFeatColNamesTransformed ++ catFeatColNamesEncoded ++ multiCatFeatColNamesEncoded)
        .setOutputCol(FeatureProcessor.featsProcessedColName)
      pipelineStages += vectorAssembler
      // drop for saving memory
      val columnEliminatorForLast = new ColumnEliminator()
        .setInputCols(numFeatColNamesTransformed ++ catFeatColNamesEncoded ++ multiCatFeatColNamesEncoded)
      pipelineStages += columnEliminatorForLast
    }
  }

  // TODO: regression
  def appendPipelineStagesForLabel(schemaHandler: SchemaHandler): Unit = {
    if(schemaHandler.labelColName.length > 0) {
      val categoryEncoder = new CategoryEncoder()
        .setInputCols(schemaHandler.labelColName)
        .setOutputCols(Array(FeatureProcessor.labelProcessedColName))
        .setIndexOnly(true)
        .setDropInputCols(true)
      pipelineStages += categoryEncoder
    }
  }

  def buildPipeline(schemaHandler: SchemaHandler,
                    methodForNum: String,
                    ifOnehotForNum: Boolean,
                    ifOnehotForCat: Boolean,
                    ifOnehotForMultiCat: Boolean,
                    ifMerge: Boolean,
                    numBuckets: Int = 40,
                    categoriesReservedForCat: Int = 0,
                    categoriesReservedForMultiCat: Int = 0,
                    multiCatDelimiter: String = ","): Unit = {
    appendPipelineStagesForOtherFeatures(schemaHandler)
    appendPipelineStagesForNumericalFeatures(schemaHandler,
                                              methodForNum, ifOnehotForNum, numBuckets)
    appendPipelineStagesForCategoricalFeatures(schemaHandler,
                                              ifOnehotForCat, categoriesReservedForCat)
    appendPipelineStagesForMultiCategoricalFeatures(schemaHandler,
                                              ifOnehotForMultiCat, categoriesReservedForMultiCat, multiCatDelimiter)
    appendPipelineStagesForMergingFeatures(schemaHandler, ifMerge)
    appendPipelineStagesForLabel(schemaHandler)
  }

  def pipelineFitTransform(df: DataFrame,
                           schemaHandler: SchemaHandler,
                           methodForNum: String,
                           ifOnehotForNum: Boolean,
                           ifOnehotForCat: Boolean,
                           ifOnehotForMultiCat: Boolean,
                           ifMerge: Boolean,
                           numBuckets: Int = 40,
                           categoriesReservedForCat: Int = 0,
                           categoriesReservedForMultiCat: Int = 0,
                           multiCatDelimiter: String = ","): (DataFrame, PipelineModel) = {
    buildPipeline(schemaHandler, methodForNum, ifOnehotForNum, ifOnehotForCat, ifOnehotForMultiCat, ifMerge,
        numBuckets, categoriesReservedForCat, categoriesReservedForMultiCat, multiCatDelimiter)
    val pipeline = new Pipeline().setStages(pipelineStages.toArray)
    PipelinePatch.pipelineFitTransform(df, pipeline)
  }
}



