package org.apache.spark.ml

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object PipelinePatch {

  def pipelineFitTransform(df: DataFrame, pipeline: Pipeline): (DataFrame, PipelineModel) = {
    // check validity of schema
    pipeline.transformSchema(df.schema)
    val stagesClone = pipeline.getStages
    var dfTarget = df
    val transformers = new ArrayBuffer[Transformer]()
    stagesClone.foreach{ stage => {
      stage match {
        case estimator: Estimator[_] => {
          val e2t = estimator.fit(dfTarget)
          dfTarget = e2t.transform(dfTarget)
          transformers += e2t
        }
        case transformer: Transformer => {
          dfTarget = transformer.transform(dfTarget)
          transformers += transformer
        }
        case _ =>
          throw new IllegalArgumentException(
            s"Does not support stage $stage of type ${stage.getClass}")
      }
    }}
    (dfTarget, new PipelineModel(pipeline.uid, transformers.toArray).setParent(pipeline))
  }

}
