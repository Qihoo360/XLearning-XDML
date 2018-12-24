package net.qihoo.xitong.xdml.conf

import net.qihoo.xitong.xdml.conf.PSDataType.PSDataType
import net.qihoo.xitong.xdml.conf.JobType.PSJobType

class PSConfiguration extends Serializable {
	/**
	  * Kudu configuration
	  */
	private var kuduMaster = ""
	private var kuduReplicaNum = 3
	private var kuduPartitionNum = 50
	private var kuduTableForceOverride = true
	private var kuduHashPartitionEnable = true
	private var kuduPrimaryKeyCol = true
	private var kuduKeyColName: String = "id"
	private var kuduValueColName: String = "value"
	private var kuduValueNullable = false
//	private var kuduByteArraySize: Int = 4
	/**
	  * Hazelcast configuration
	  */
	private var hzClusterNum = 50
	private var hzPartitionNum = 271
	private var hzInitialHzClusterNum = hzClusterNum / 2 + 1
	private var hzClientThreadCount = 20
	private var hzOperationThreadCount = 20
	private var hzMaxCacheSizePerPartition = 50000000
	private var hzInitialClusterIP: String = _
	private var hzGroupName = "HazelcastGroup"
	private var hzGroupPassword = "Password"
	private var hzLoggingType = "none"
	private var hzSecurityEnable = false
	private var hzMulticastEnable = false
	private var hzTcpIpConfigEnable = true
	private var hzBackupCount = 0
	private var hzStatisticsEnabled = false
	private var hzWriteDelaySeconds = 20
	private var hzWriteBatchSize = 5000
	private var hzHeartbeatTime = 3000
	private var hzClientHeartbeatTime = 6000
	private var hzIOThreadCount = 20
	/**
	  * PS config
	  */
	private var psTableName: Array[String] = Array("table")
	private var psTableNumber: Int = 1
	private var psDataType = PSDataType.FLOAT
	private var psDataLength = 1
	private var randomInit = false
	private var randomMin = 0
	private var randomMax = 1
	private var forceSparse = false
	private var isIncrementTrain = false
	private var jobType:PSJobType = JobType.TRAIN
	private var predictModelPath = ""


	private var weightIndexRangeStart:Int = 0
	private var weightIndexRangeEnd:Int = this.psDataLength

	def getJobType:PSJobType = this.jobType
	def setJobType(jobType:PSJobType) :this.type = {
		this.jobType = jobType
		this
	}
	def getKuduMaster: String = this.kuduMaster

	def setKuduMaster(master: String): this.type = {
		this.kuduMaster = master
		this
	}

	def getKuduReplicaNum: Int = this.kuduReplicaNum

	def setKuduReplicaNum(kuduReplicaNum: Int): this.type = {
		this.kuduReplicaNum = kuduReplicaNum
		this
	}

	def getKuduPartitionNum: Int = this.kuduPartitionNum

	def setKuduPartitionNum(kuduPartitionNum: Int): this.type = {
		this.kuduPartitionNum = kuduPartitionNum
		this
	}

	def getKuduTableForceOverride: Boolean = this.kuduTableForceOverride

	def setKuduTableForceOverride(kuduTableForceOverride: Boolean): this.type = {
		this.kuduTableForceOverride = kuduTableForceOverride
		this
	}

	def getKuduHashPartition: Boolean = this.kuduHashPartitionEnable

	def setKuduHashPartition(kuduHashPartitionEnable: Boolean): this.type = {
		this.kuduHashPartitionEnable = kuduHashPartitionEnable
		this
	}

	def getKuduPrimaryKeyCol: Boolean = this.kuduPrimaryKeyCol

	def setKuduPrimaryKeyCol(kuduPrimaryKeyCol: Boolean): this.type = {
		this.kuduPrimaryKeyCol = kuduPrimaryKeyCol
		this
	}

	def getKuduKeyColName: String = this.kuduKeyColName

	def setKuduKeyColName(kuduKeyColName: String): this.type = {
		this.kuduKeyColName = kuduKeyColName
		this
	}

	def getKuduValueColName: String = this.kuduValueColName

	def setKuduValueColName(kuduValueColName: String): this.type = {
		this.kuduValueColName = kuduValueColName
		this
	}

	def getKuduValueNullable: Boolean = this.kuduValueNullable

	def setKuduValueNullable(kuduValueNullable: Boolean): this.type = {
		this.kuduHashPartitionEnable = kuduValueNullable
		this
	}

	def getKuduRandomInit: Boolean = this.randomInit

	def setKuduRandomInit(kuduRandomInit: Boolean): this.type = {
		this.randomInit = kuduRandomInit
		this
	}

	def getKuduRandomMin: Int = randomMin

	def getKuduRandomMax: Int = randomMax

	def setKuduRandomInit(min: Int, max: Int): this.type = {
		this.randomInit = true
		randomMin = min
		randomMax = max
		this
	}

	def getHzClusterNum:Int = this.hzClusterNum
	def setHzClusterNum(hzClusterNum:Int):this.type ={
		this.hzClusterNum = hzClusterNum
		this.hzInitialHzClusterNum = hzClusterNum / 2 + 1
		this
	}
	def getHzPartitionNum: Int = this.hzPartitionNum

	def setHzPartitionNum(hzPartitionNum: Int): this.type = {
		this.hzPartitionNum = hzPartitionNum
		this
	}

	def getHzInitialHzClusterNum: Int = this.hzInitialHzClusterNum

	def setHzInitialHzClusterNum(hzInitialHzClusterNum: Int): this.type = {
		this.hzInitialHzClusterNum = hzInitialHzClusterNum
		this
	}

	def getHzClientThreadCount: Int = this.hzClientThreadCount

	def setHzClientThreadCount(hzClientThreadCount: Int): this.type = {
		this.hzClientThreadCount = hzClientThreadCount
		this
	}

	def getHzOperationThreadCount:Int = this.hzOperationThreadCount
	def setHzOperationThreadCount(hzOperationThreadCount:Int) :this.type ={
		this.hzOperationThreadCount = hzOperationThreadCount
		this
	}

	def getHzMaxCacheSizePerPartition: Int = this.hzMaxCacheSizePerPartition

	def setHzMaxCacheSizePerPartition(hzMaxCacheSizePerPartition: Int): this.type = {
		this.hzMaxCacheSizePerPartition = hzMaxCacheSizePerPartition
		this
	}

	def getHzInitialClusterIP: String = this.hzInitialClusterIP

	def setHzInitialClusterIP(hzInitialClusterIP: String): this.type = {
		this.hzInitialClusterIP = hzInitialClusterIP
		this
	}

	def getHzGroupName: String = this.hzGroupName

	def setHzGroupName(hzGroupName: String): this.type = {
		this.hzGroupName = hzGroupName
		this
	}

	def getHzGroupPassword: String = this.hzGroupPassword

	def setHzGroupPassword(hzGroupPassword: String): this.type = {
		this.hzGroupPassword = hzGroupPassword
		this
	}

	def getHzLoggingType: String = this.hzLoggingType

	def setHzLoggingType(hzLoggingType: String): this.type = {
		this.hzLoggingType = hzLoggingType
		this
	}

	def getHzSecurityEnable: Boolean = this.hzSecurityEnable

	def setHzSecurityEnable(hzSecurityEnable: Boolean): this.type = {
		this.hzSecurityEnable = hzSecurityEnable
		this
	}

	def getHzMulticastEnable: Boolean = this.hzMulticastEnable

	def setHzMulticastEnable(hzMulticastEnable: Boolean): this.type = {
		this.hzMulticastEnable = hzMulticastEnable
		this
	}

	def getHzTcpIpConfigEnable: Boolean = this.hzTcpIpConfigEnable

	def setHzTcpIpConfigEnable(hzTcpIpConfigEnable: Boolean): this.type = {
		this.hzTcpIpConfigEnable = hzTcpIpConfigEnable
		this
	}

	def getHzBackupCount: Int = this.hzBackupCount

	def setHzBackupCount(hzBackupCount: Int): this.type = {
		this.hzBackupCount = hzBackupCount
		this
	}

	def getHzStatisticsEnabled: Boolean = this.hzStatisticsEnabled

	def setHzStatisticsEnabled(hzStatisticsEnabled: Boolean): this.type = {
		this.hzStatisticsEnabled = hzStatisticsEnabled
		this
	}

	def getHzWriteDelaySeconds: Int = this.hzWriteDelaySeconds

	def setHzWriteDelaySeconds(hzWriteDelaySeconds: Int): this.type = {
		this.hzWriteDelaySeconds = hzWriteDelaySeconds
		this
	}

	def getHzWriteBatchSize: Int = this.hzWriteBatchSize

	def setHzWriteBatchSize(hzWriteBatchSize: Int): this.type = {
		this.hzWriteBatchSize = hzWriteBatchSize
		this
	}

	def getHzHeartbeatTime: Int = this.hzHeartbeatTime

	def setHzHeartbeatTime(hzHeartbeatTime: Int): this.type = {
		this.hzHeartbeatTime = hzHeartbeatTime
		this
	}

	def getHzClientHeartbeatTime: Int = this.hzClientHeartbeatTime

	def setHzClientHeartbeatTime(hzClientHeartbeatTime: Int): this.type = {
		this.hzClientHeartbeatTime = hzClientHeartbeatTime
		this
	}

	def getHzIOThreadCount:Int = hzIOThreadCount
	def setHzIOThreadCount(hzIOThreadCount:Int):this.type ={
		this.hzIOThreadCount = hzIOThreadCount
		this
	}

	def getPsTableName(tableIndex: Int = 0): String = {
		if (tableIndex >= psTableName.length)
			throw new IllegalArgumentException("Kudu doesn't have so many tables")
		psTableName(tableIndex)
	}

	def setPsTableName(psTableName: String*): this.type = {
		this.psTableName = Seq(psTableName).head.toArray
		psTableNumber = psTableName.length
		this
	}

	def getPsTableNumber: Int = this.psTableNumber

	def getPsDataType: PSDataType = this.psDataType

	def setPsDataType(psDataType: PSDataType): this.type = {
		this.psDataType = psDataType
		this
	}

	def getPsDataLength: Int = this.psDataLength

	def setPsDataLength(psDataLength: Int): this.type = {
		this.psDataLength = psDataLength
		this.weightIndexRangeEnd = psDataLength
		this
	}

	def getForceSparse:Boolean = this.forceSparse
	def setForceSparse(forceSparse:Boolean):this.type ={
		this.forceSparse = forceSparse
		this
	}

	def getIsIncrementTrain:Boolean = this.isIncrementTrain
	def setIsIncrementTrain(isIncrementTrain:Boolean):this.type ={
		this.isIncrementTrain = isIncrementTrain
		this
	}

	def getPredictModelPath:String = this.predictModelPath
	def setPredictModelPath(predictModelPath:String):this.type ={
		this.predictModelPath = predictModelPath
		this
	}

	def getWeightIndexRangeStart:Int = this.weightIndexRangeStart
	def setWeightIndexRangeStart(weightIndexRangeStart:Int) :this.type = {
		this.weightIndexRangeStart = weightIndexRangeStart
		this
	}

	def getWeightIndexRangeEnd:Int = this.weightIndexRangeEnd
	def setWeightIndexRangeEnd(weightIndexRangeEnd:Int) :this.type = {
		this.weightIndexRangeEnd = weightIndexRangeEnd
		this
	}
}
