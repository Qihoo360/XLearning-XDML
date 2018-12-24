package net.qihoo.xitong.xdml.ps

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy
import com.hazelcast.config._
import com.hazelcast.core.Hazelcast
import com.hazelcast.map.eviction.LRUEvictionPolicy
import net.qihoo.xitong.xdml.conf.{JobType, PSConfiguration, PSDataType}
import net.qihoo.xitong.xdml.mapstore.{KuduTableOp, PSMapStore}
import net.qihoo.xitong.xdml.utils.XDMLException
import org.apache.hadoop.fs.Path
import org.apache.kudu.ColumnSchema.Encoding
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._


object PS {
	var psInstance: PS = _
	var kuduContext:KuduContext = _
	//get a singleton PS instance and start it
	def getInstance(sc: SparkContext, psConf: PSConfiguration): PS = {
		if (psInstance == null) {
			this.synchronized {
				psInstance = new PS(psConf)
				kuduContext = new KuduContext(psConf.getKuduMaster, sc)
				psInstance.start(sc)
			}
		}
		psInstance
	}
}

class PS(val psConf: PSConfiguration) extends Serializable {
	//hazelcast cluster's host IPs
	private var hzClusterIP: Array[String] = _
	def getIPList: Array[String] = {
		this.hzClusterIP
	}

	//start ps
	def start(sc: SparkContext): Unit = {
		println("Job type: " + psConf.getJobType)
		psConf.getJobType match {
			case JobType.TRAIN => startTrainJob(sc)
			case JobType.PREDICT => startPredictJob(sc)
			case JobType.INCREMENT_TRAIN => startIncrementTrain(sc)
			case _ => throw new XDMLException("PS has a wrong job type!")
		}
	}
	//start a train job
	def startTrainJob(sc: SparkContext): Unit ={
		println("Train hazelcast and kudu is starting...")
		createKuduTable()
		startHzCluster(sc)
		println("PS has started!")
	}
	//start a predict job
	def startPredictJob(sc: SparkContext): Unit = {
		println("Preparing to predict...")
		loadModel(sc, psConf.getPredictModelPath, PS.kuduContext)
		startHzCluster(sc)
		val paraNum = getAllParametersReady(sc, psConf.getPsTableName(), PS.kuduContext)
		println("All parameters get ready, total count: " + paraNum)
	}
	//start a increment job
	def startIncrementTrain(sc: SparkContext):Unit = {
		println("Increment train hz and kudu is starting...")
		loadModel(sc, psConf.getPredictModelPath, PS.kuduContext)
		startHzCluster(sc)
		val paraNum = getAllParametersReady(sc, psConf.getPsTableName(), PS.kuduContext)
		println("All parameters get ready, total count: " + paraNum)
	}

	//	create kudu table by config of a PSConfiguration instance
	def createKuduTable(): Unit = {
		val kuduOp = new KuduTableOp(psConf.getKuduMaster)
			.setReplicasNum(psConf.getKuduReplicaNum)
			.setPartitionNum(psConf.getKuduPartitionNum)
		for (tableIndex <- 0 until psConf.getPsTableNumber) {
			val tableName = psConf.getPsTableName(tableIndex)
			val kuduCols = new util.ArrayList[ColumnSchema]()
			kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(psConf.getKuduKeyColName, Type.INT64).encoding(Encoding.PLAIN_ENCODING).key(true).build())
			kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(psConf.getKuduValueColName, Type.BINARY).encoding(Encoding.PLAIN_ENCODING).nullable(psConf.getKuduValueNullable).build())
			if (psConf.getKuduTableForceOverride && kuduOp.isTableExist(tableName)) {
				kuduOp.deleteTable(tableName)
				println(tableName + " already have! Deleted table: " + tableName)
			}
			kuduOp.createTable(tableName, kuduCols, psConf.getKuduHashPartition)
		}
		kuduOp.closeClient()
	}

	//start hz cluster
	def startHzCluster(sc: SparkContext): Unit = {
		val initialClusterIP = sc.parallelize(0 until psConf.getHzClusterNum, psConf.getHzClusterNum).mapPartitions { partition =>
			val partitionHost = InetAddress.getLocalHost
			val hostIP = partitionHost.getHostAddress
			Iterator(hostIP)
		}.take(psConf.getHzInitialHzClusterNum)
		psConf.setHzInitialClusterIP(initialClusterIP.mkString(","))
		val cluster = sc.parallelize(0 until psConf.getHzClusterNum, psConf.getHzClusterNum).map(index => {
			val cfg = new Config()
			cfg.setProperty("hazelcast.partition.count", psConf.getHzPartitionNum.toString)
			cfg.getGroupConfig.setName(psConf.getHzGroupName).setPassword(psConf.getHzGroupPassword)
			cfg.getSecurityConfig.setEnabled(psConf.getHzSecurityEnable)
			cfg.setProperty("hazelcast.logging.type", psConf.getHzLoggingType)
			cfg.setProperty("hazelcast.operation.thread.count", psConf.getHzOperationThreadCount.toString)
			cfg.setProperty("hazelcast.io.thread.count", psConf.getHzIOThreadCount.toString)
			cfg.setProperty("hazelcast.clientengine.thread.count", psConf.getHzClientThreadCount.toString)
			cfg.setProperty("hazelcast.max.no.heartbeat.seconds", psConf.getHzHeartbeatTime.toString)
			cfg.setProperty("hazelcast.client.heartbeat.timeout", psConf.getHzClientHeartbeatTime.toString)
			cfg.getExecutorConfig(psConf.getPsTableName()).setPoolSize(psConf.getHzClusterNum).setStatisticsEnabled(false)
			for (tableIndex <- 0 until psConf.getPsTableNumber) {
				val tableName = psConf.getPsTableName(tableIndex)
				cfg.addMapConfig(createHzMapConfig(tableName))
			}
			val join = cfg.getNetworkConfig.getJoin
			join.getMulticastConfig.setEnabled(psConf.getHzMulticastEnable)
			join.getTcpIpConfig.addMember(psConf.getHzInitialClusterIP).setEnabled(psConf.getHzTcpIpConfigEnable)
			val hzNode = Hazelcast.newHazelcastInstance(cfg)
			val clusterIPs = hzNode.getCluster.getMembers.map(member => member.getAddress.getHost).toArray
			clusterIPs
		}).cache()
		hzClusterIP = cluster.collect().last
		println("hz cluster: " + hzClusterIP.mkString(","))
	}

	//create hz map config instance by config of PSConfiguration
	def createHzMapConfig(tableName: String): MapConfig = {
		val kuduStore = new PSMapStore(psConf, tableName)
		val mapStoreConfig = new MapStoreConfig()
			.setImplementation(kuduStore)
			.setWriteDelaySeconds(psConf.getHzWriteDelaySeconds)
			.setWriteBatchSize(psConf.getHzWriteBatchSize)
		val mapConfig = new MapConfig()
			.setName(tableName)
			.setInMemoryFormat(InMemoryFormat.BINARY)
			.setBackupCount(psConf.getHzBackupCount)
			.setStatisticsEnabled(psConf.getHzStatisticsEnabled)
			.setMapStoreConfig(mapStoreConfig)
			.setMapEvictionPolicy(new LRUEvictionPolicy)
			.setCacheDeserializedValues(CacheDeserializedValues.NEVER)
		mapConfig.getMaxSizeConfig.setMaxSizePolicy(MaxSizePolicy.PER_PARTITION).setSize(psConf.getHzMaxCacheSizePerPartition)
		mapConfig
	}

	//pull all parameters from kudu to hz cluster
	def getAllParametersReady(sc: SparkContext, tableName: String = psConf.getPsTableName(), kuduContext: KuduContext): Long = {
		println("Load parameters from kudu to hz cluster...")
		val columnProjection = Seq(psConf.getKuduKeyColName, psConf.getKuduValueColName)
		val rdd = kuduContext.kuduRDD(sc, tableName, columnProjection)
		val parametersRdd = rdd.map {
			case Row(id: Long, weight: Array[Byte]) => (id, weight)
		}
		parametersRdd.mapPartitions { iter =>
			val clientConfig = new ClientConfig()
			clientConfig.getGroupConfig.setName(psConf.getHzGroupName).setPassword(psConf.getHzGroupPassword)
			clientConfig.setProperty("hazelcast.io.thread.count", psConf.getHzIOThreadCount.toString)
			clientConfig.setProperty("hazelcast.logging.type", psConf.getHzLoggingType)
			clientConfig.getNetworkConfig.setAddresses(hzClusterIP.toList)
			val client = HazelcastClient.newHazelcastClient(clientConfig)
			val weightMap = client.getMap[Long, Array[Byte]](tableName)
			var partCount = 0L
			while (iter.hasNext) {
				val (id, weight) = iter.next()
				weightMap.setAsync(id, weight)
				partCount += 1
			}
			client.shutdown()
			Iterator(partCount)
		}.reduce(_ + _)
	}
	//get range array from bytes
	def getRangeModelWeightFromBytes(bytes: Array[Byte], startRange: Int, endRange: Int): Any = {
		val valueBuff = ByteBuffer.wrap(bytes)
		val allWeights = if (startRange == 0 && endRange == psConf.getPsDataLength) true else false
		psConf.getPsDataType match {
			case PSDataType.FLOAT => valueBuff.getFloat()
			case PSDataType.DOUBLE => valueBuff.getDouble()
			case PSDataType.FLOAT_ARRAY => {
				val arr = new Array[Float](psConf.getPsDataLength)
				for (index <- arr.indices) {
					arr(index) = valueBuff.getFloat(index << 2)
				}
				if (allWeights)
					arr
				else
					arr.slice(startRange, endRange)
			}
			case PSDataType.DOUBLE_ARRAY => {
				val arr = new Array[Double](psConf.getPsDataLength)
				for (index <- arr.indices) {
					arr(index) = valueBuff.getDouble(index << 3)
				}
				if (allWeights)
					arr
				else
					arr.slice(startRange, endRange)
			}
			case _ => throw new IllegalArgumentException("data type error!")
		}
	}
	//get range bytes from array
	def getRangeBytesFromWeight(values: Array[String], startRange: Int, endRange: Int): Array[Byte] = {
		val byteSize = PSDataType.sizeOf(psConf.getPsDataType) * psConf.getPsDataLength
		val start = startRange * PSDataType.sizeOf(psConf.getPsDataType)
		val end = endRange * PSDataType.sizeOf(psConf.getPsDataType)
		val byteBuff = ByteBuffer.allocate(byteSize)
		val allWeight = if (start == 0 && end == byteSize) true else false
		psConf.getPsDataType match {
			case PSDataType.FLOAT => {
				byteBuff.putFloat(values.head.toFloat)
			}
			case PSDataType.DOUBLE => {
				byteBuff.putDouble(values.head.toDouble)
			}
			case PSDataType.FLOAT_ARRAY => {
				val arr = values.asInstanceOf[Array[String]]
				arr.indices.map { index =>
					byteBuff.putFloat(index << 2, arr(index).toFloat)
				}
			}
			case PSDataType.DOUBLE_ARRAY => {
				val arr = values.asInstanceOf[Array[String]]
				arr.indices.map { index =>
					byteBuff.putDouble(index << 3, arr(index).toDouble)
				}
			}
			case _ => throw new IllegalArgumentException("data type error!")
		}
		if (allWeight)
			byteBuff.array()
		else
			byteBuff.array().slice(start, end)
	}
	//save model to HDFS
	def saveModel(sc: SparkContext,
				  outputPath: String,
				  tableName: String = psConf.getPsTableName(),
				  split: String = "\t",
				  weightStartPos: Int = psConf.getWeightIndexRangeStart,
				  weightEndPos: Int = psConf.getWeightIndexRangeEnd,
				  kuduContext: KuduContext = PS.kuduContext): Unit = {
		try {
			val path = new Path(outputPath)
			val fs = path.getFileSystem(sc.hadoopConfiguration)
			if (fs.exists(path)) {
				println(s"outputPath ${outputPath} existed, delete.")
				fs.delete(path, true)
			}
		} catch {
			case e: Exception =>
				println(s"Check and delete the model path ${outputPath} failed. More details: ${e}")
				e.printStackTrace()
		}
		val columnProjection = Seq(psConf.getKuduKeyColName, psConf.getKuduValueColName)
		val parametersRdd = kuduContext.kuduRDD(sc, tableName, columnProjection)
		val featureNum = parametersRdd.count()
		val weightData = parametersRdd.map { case Row(id: Long, weight: Array[Byte]) => (id, weight) }
		weightData.map { case (id, weight) =>
			val modelLine = new StringBuilder()
			modelLine.append(id)
			val modelWeight = getRangeModelWeightFromBytes(weight, weightStartPos, weightEndPos)
			psConf.getPsDataType match {
				case PSDataType.FLOAT => modelLine.append(split).append(modelWeight.asInstanceOf[Float])
				case PSDataType.DOUBLE => modelLine.append(split).append(modelWeight.asInstanceOf[Double])
				case PSDataType.FLOAT_ARRAY => {
					modelLine.append(split).append(modelWeight.asInstanceOf[Array[Float]].mkString(split))
				}
				case PSDataType.DOUBLE_ARRAY => {
					modelLine.append(split).append(modelWeight.asInstanceOf[Array[Double]].mkString(split))
				}
			}
			modelLine
		}.saveAsTextFile(outputPath)
		println("Save model succeed! The effective features size is " + featureNum)
		if (featureNum != 0)
			kuduContext.deleteTable(tableName)
		println("Delete table: " + tableName)
	}
	//load model from HDFS to kudu
	def loadModel(sc: SparkContext,
				  inputPath: String,
				  kuduContext: KuduContext,
				  tableName: String = psConf.getPsTableName(),
				  split: String = "\t",
				  weightStartPos: Int = psConf.getWeightIndexRangeStart,
				  weightEndPos: Int = psConf.getWeightIndexRangeEnd
				 ): Unit = {
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		val weightData = sc.textFile(inputPath).map { line =>
			val modelLine = line.split(split)
			Weight(modelLine(0).toLong, getRangeBytesFromWeight(modelLine.slice(1, modelLine.length), weightStartPos, weightEndPos))
		}.toDF(psConf.getKuduKeyColName, psConf.getKuduValueColName)
		println("Loading model to kudu...")
		if (kuduContext.tableExists(tableName)) {
			kuduContext.deleteTable(tableName)
		}
		val kuduTableOptions = new CreateTableOptions()
			.setNumReplicas(psConf.getKuduReplicaNum)
			.addHashPartitions(List(psConf.getKuduKeyColName), psConf.getKuduPartitionNum)
		kuduContext.createTable(tableName, weightData.schema, Seq(psConf.getKuduKeyColName), kuduTableOptions)
		kuduContext.insertRows(weightData, tableName)
	}
}

case class Weight(id: Long, value: Array[Byte])