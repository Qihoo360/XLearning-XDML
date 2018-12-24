package net.qihoo.xitong.xdml.example.ml

import net.qihoo.xitong.xdml.conf.{JobConfiguration, JobType, PSConfiguration, PSDataType}
import net.qihoo.xitong.xdml.dataProcess.LibSVMProcessor
import net.qihoo.xitong.xdml.ml.LogisticRegressionWithDCASGD
import net.qihoo.xitong.xdml.ps.PS
import net.qihoo.xitong.xdml.utils.XDMLException
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object DCASGDTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Momentum-Test")
		val sc = new SparkContext(conf)
		//read spark config
		val jobConf = new JobConfiguration().readJobConfig(sc)
		val dataPath = jobConf.getOrElse(JobConfiguration.DATA_PATH, "")
		val dataPartitionNum = jobConf.getOrElse(JobConfiguration.TRAIN_DATA_PARTITION, "50").toInt
		val iterNum = jobConf.getOrElse(JobConfiguration.TRAIN_ITER_NUM, "1").toInt
		val batchSize = jobConf.getOrElse(JobConfiguration.BATCH_SIZE, "5000").toInt
		val jobType = jobConf.getOrElse(JobConfiguration.JOB_TYPE, "train")
		val modelPath = jobConf.getOrElse(JobConfiguration.MODEL_PATH, "")
		val learningRate = jobConf.getOrElse(JobConfiguration.LEARNING_RATE, "0.01").toFloat
		val coff = jobConf.getOrElse(JobConfiguration.DC_ASGD_COFF, "0.1").toFloat
		val hzClusterNum = jobConf.getOrElse(JobConfiguration.HZ_CLUSTER_NUM, "50").toInt
		val hzPartitionNum = jobConf.getOrElse(JobConfiguration.HZ_PARTITION_NUM, "271").toInt
		val tableName = System.getProperty("user.name") + "_" + jobConf.getOrElse(JobConfiguration.TABLE_NAME, "DC-ASGD")
		val kuduMaster = jobConf.getOrElse(JobConfiguration.KUDU_MASTER, "")
		val resultPath = jobConf.getOrElse(JobConfiguration.PREDICT_RESULT_PATH, "")
		val split = jobConf.getOrElse(JobConfiguration.DATA_SPLIT, " ")
		if (kuduMaster.equals("")) {
			throw new XDMLException("kudu master must be set!")
		}
		//read data
		val rawData = sc.textFile(dataPath)
		val data = LibSVMProcessor.processData(rawData, split).coalesce(dataPartitionNum)
		val psConf = new PSConfiguration()
			.setPsTableName(tableName)
			.setHzClusterNum(hzClusterNum)
			.setHzPartitionNum(hzPartitionNum)
			.setPsDataType(PSDataType.FLOAT_ARRAY)
			.setPsDataLength(2)
			.setKuduMaster(kuduMaster)
		if (jobType.toUpperCase.equals("TRAIN")) {
			psConf.setJobType(JobType.TRAIN)
		} else if (jobType.toUpperCase.equals("PREDICT")) {
			if (modelPath.equals(""))
				throw new XDMLException("Predict job must have a model path")
			else
				psConf.setPredictModelPath(modelPath)
			psConf.setJobType(JobType.PREDICT)
		} else if (jobType.toUpperCase.equals("INCREMENT_TRAIN")) {
			if (modelPath.equals(""))
				throw new XDMLException("INCREMENT_TRAIN job must have a model path")
			else
				psConf.setPredictModelPath(modelPath)
			psConf.setJobType(JobType.INCREMENT_TRAIN)
		} else {
			throw new XDMLException("Wrong job type")
		}
		val ps = PS.getInstance(sc, psConf)
		val model = new LogisticRegressionWithDCASGD(ps)
			.setIterNum(iterNum)
			.setBatchSize(batchSize)
			.setLearningRate(learningRate)
			.setDcAsgdCoff(coff)
		psConf.getJobType match {
			case JobType.TRAIN => {
				//start train
				println("Start Train...")
				val info = model.fit(data)
				println(s"Save the model to path :" + modelPath)
				ps.saveModel(sc, modelPath)
			}
			case JobType.PREDICT => {
				//start predict
				println("Start Predict...")
				val result = model.predict(data)
				val path = new Path(resultPath)
				val fs = path.getFileSystem(sc.hadoopConfiguration)
				if (fs.exists(path)) {
					println(s"Result Path ${resultPath} existed, delete.")
					fs.delete(path, true)
				}
				result.saveAsTextFile(resultPath)
				val rightRate = result.filter(x => (if (x._1 > 0.5) 1 else 0) == x._2).count().toDouble / result.count().toDouble
				println("right rate is : " + rightRate)
			}
			case JobType.INCREMENT_TRAIN => {
				//start train
				println("Start Increment Train...")
				val info = model.fit(data)
				println(s"Save the new model to path :" + modelPath)
				ps.saveModel(sc, modelPath)
			}
		}
		sc.stop()
	}
}
