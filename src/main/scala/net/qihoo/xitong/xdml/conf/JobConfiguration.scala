package net.qihoo.xitong.xdml.conf

import org.apache.spark.SparkContext

import scala.collection.mutable


object JobConfiguration {

	val DATA_SPLIT = "spark.xdml.data.split"
	val KUDU_MASTER = "spark.xdml.kudu.master"
	val LEARNING_RATE = "spark.xdml.learningRate"
	val TABLE_NAME = "spark.xdml.table.name"
	val MODEL_PATH = "spark.xdml.model.path"
	val DATA_PATH = "spark.xdml.data.path"
	val TRAIN_DATA_PARTITION = "spark.xdml.train.data.partitionNum"
	val TRAIN_ITER_NUM = "spark.xdml.train.iter"
	val BATCH_SIZE = "spark.xdml.train.batchsize"
	val SUBSAMPLE_RATE = "spark.xdml.subsample.rate"
	val SUBSAMPLE_LABEL = "spark.xdml.train.subsample.label"
	val HZ_CLUSTER_NUM = "spark.xdml.hz.clusterNum"
	val HZ_PARTITION_NUM = "spark.xdml.hz.partitionNum"
	val HZ_MAXCACHESIZE_PER_PARTITION = "spark.xdml.hz.maxcachesize"
	val SAVE_ALL_WEIGHTS = "spark.xdml.model.save.allweights"
	val SAVE_FEATURE = "spark.xdml.model.save.feature"
	val FEATURE_FILE = "spark.xdml.model.feature.path"
	val PREDICT_RESULT_PATH = "spark.xdml.predict.result.path"
	val JOB_TYPE = "spark.xdml.job.type"

	val DC_ASGD_COFF = "spark.xdml.dcasgdCoff"

	val MOMENTUM_COFF = "spark.xdml.momentumCoff"

	val FTRL_ALPHA = "spark.xdml.train.alpha"
	val FTRL_BETA = "spark.xdml.train.beta"
	val FTRL_LAMBDA1 = "spark.xdml.train.lambda1"
	val FTRL_LAMBDA2 = "spark.xdml.train.lambda2"
	val FTRL_FROCESPARSE = "spark.xdml.train.forcesparse"

	val FM_RANK = "spark.xdml.model.fm.rank"

	val FFM_RANK = "spark.xdml.model.ffm.rank"
	val FFM_FIELD = "spark.xdml.model.ffm.field"

	private val jobConfSet = Set(
		/**
		  * Common configuration
		  */
		DATA_SPLIT,
		KUDU_MASTER,
		LEARNING_RATE,
		TABLE_NAME,
		MODEL_PATH,
		DATA_PATH,
		TRAIN_DATA_PARTITION,
		TRAIN_ITER_NUM,
		BATCH_SIZE,
		SUBSAMPLE_RATE,
		SUBSAMPLE_LABEL,
		HZ_CLUSTER_NUM,
		HZ_PARTITION_NUM,
		HZ_MAXCACHESIZE_PER_PARTITION,
		SAVE_ALL_WEIGHTS,
		SAVE_FEATURE,
		FEATURE_FILE,
		PREDICT_RESULT_PATH,
		JOB_TYPE,

		/**
		  * Momentum
		  */
		MOMENTUM_COFF,

		/**
		  * DC-ASGD
		  */
		DC_ASGD_COFF,
		/**
		  * FTRL configuration
		  */
		FTRL_ALPHA,
		FTRL_BETA,
		FTRL_LAMBDA1,
		FTRL_LAMBDA2,
		FTRL_FROCESPARSE,

		/**
		  * FM configuration
		  */
		FM_RANK,

		/**
		  * FFM configuration
		  */
		FFM_RANK,
		FFM_FIELD
	)

}

class JobConfiguration() {
	//read all job configuration
	def readJobConfig(sc: SparkContext): Map[String, String] = {
		val confMap = new mutable.HashMap[String, String]()
		for (confName <- JobConfiguration.jobConfSet) {
			val value = sc.getConf.get(confName, null)
			if (value != null)
				confMap.put(confName, value)
		}
		confMap.toMap
	}
}
