# PSConfiguration

---


> PSConfiguration封装了所有XDML中参数服务器中使用的两个组件Kudu和Hazelcast所有的配置参数和PS相关的参数。

## 功能

* 使用get和set方法获取和配置XDML中参数服务器的相关参数。


## 核心接口

1. **setJobType**
	- 定义：```setJobType(jobType:PSJobType) :this.type ```
	- 功能描述：设置作业参数服务器的作业类型，目前有train,predict和increment_train三种作业类型。
	- 参数：
		- jobType: 枚举类JobType的实例
	- 返回值：this

2. **setKuduMaster**
	- 定义：```setKuduMaster(master: String): this.type ```
	- 功能描述：设置kudu集群的master地址
	- 参数：
		- master: Kudu集群的mater节点地址
	- 返回值：this

3. **setKuduReplicaNum**
	- 定义：```setKuduReplicaNum(kuduReplicaNum: Int) ```
	- 功能描述：设置kudu数据备份数，默认为3
	- 参数：
		- kuduReplicaNum:kudu数据备份数
	- 返回值：this

4. **setKuduPartitionNum**
	- 定义：```setKuduPartitionNum(kuduPartitionNum: Int) ```
	- 功能描述：设置kudu的分区数，默认为50
	- 参数：
		- kuduPartitionNum：kudu存储的分区数
	- 返回值：this

5. **setKuduTableForceOverride**
	- 定义：```setKuduTableForceOverride(kuduTableForceOverride: Boolean): this.type ```
	- 功能描述：设置当kudu中新建表时有重名的表出现是否强制覆盖旧表，默认为true
	- 参数：
		- kuduTableForceOverride：是否强制覆盖，true为覆盖，false为使用旧表
	- 返回值：this

6. **setKuduKeyColName**
	- 定义：```setKuduKeyColName(kuduKeyColName: String): this.type ```
	- 功能描述：设置kudu表的键所在列的名字
	- 参数：
		- kuduKeyColName：键列的名字
	- 返回值：this

7. **setKuduValueColName**
	- 定义：```setKuduValueColName(kuduValueColName: String): this.type ```
	- 功能描述：设置kudu表的值所在列的名字
	- 参数：
		- kuduValueColName：值列的名字
	- 返回值：this

8. **setKuduValueNullable**
	- 定义：```setKuduValueNullable(kuduValueNullable: Boolean) ```
	- 功能描述：设置kudu表的值列是否可以为null，默认为false
	- 参数：
		- kuduValueNullable：值是否可以为null
	- 返回值：this

9. **setKuduRandomInit**
	- 定义：```setKuduRandomInit(kuduRandomInit: Boolean) ```
	- 功能描述：设置是否需要参数的随机初始化，默认为false
	- 参数：
		- kuduRandomInit：布尔类型是否需要随机初始化值，默认范围是0-1之间随机值，可以通过setKuduRandomInit方法设置随机范围。
	- 返回值：this

10. **setHzClusterNum**
	- 定义：```setHzClusterNum(hzClusterNum:Int):this.type ```
	- 功能描述：配置在参数服务器启动的时候需要启动的hz的节点数，默认为50
	- 参数：
		- hzClusterNum：参数服务器中需要启动的hz节点数
	- 返回值：this

11. **setHzPartitionNum**
	- 定义：```setHzPartitionNum(hzPartitionNum: Int): this.type ```
	- 功能描述：设置hz的分区数，默认为271
	- 参数：
		- hzPartitionNum:Hazelcast的分区数
	- 返回值：this

12. **setHzClientThreadCount**
	- 定义：```setHzClientThreadCount(hzClientThreadCount: Int): this.type ```
	- 功能描述：设置hazelcast的client最大处理线程数，默认为20
	- 参数：
		- hzClientThreadCount：hzclient的线程数
	- 返回值：this

13. **setHzOperationThreadCount**
	- 定义：```setHzOperationThreadCount(hzOperationThreadCount:Int) :this.type ```
	- 功能描述：设置hz的server最大操作线程数，默认是20
	- 参数：
		- hzOperationThreadCount：hz的server最大操作线程数
	- 返回值：this

14. **setHzMaxCacheSizePerPartition**
	- 定义：```setHzMaxCacheSizePerPartition(hzMaxCacheSizePerPartition: Int): this.type ```
	- 功能描述：设置Hz的单个分区最大缓存数据量，默认是50000000
	- 参数：
		- hzMaxCacheSizePerPartition：Hz的单个分区最大缓存数据量
	- 返回值：this

15. **setHzWriteDelaySeconds**
	- 定义：```setHzWriteDelaySeconds(hzWriteDelaySeconds: Int): this.type ```
	- 功能描述：设置hz异步向kudu中持久化数据的时间间隔，单位为s，默认为20
	- 参数：
		- hzWriteDelaySeconds：久化数据的时间间隔
	- 返回值：this

16. **setHzWriteBatchSize**
	- 定义：```setHzWriteBatchSize(hzWriteBatchSize: Int): this.type ```
	- 功能描述：设置hz写入的batchsize
	- 参数：
		- hzWriteBatchSize：hz写入的batchsize
	- 返回值：this

17. **setHzHeartbeatTime**
	- 定义：```setHzHeartbeatTime(hzHeartbeatTime: Int): this.type ```
	- 功能描述：设置hz server的心跳时间，单位为ms，默认为3000
	- 参数：
		- hzHeartbeatTime：hz心跳时间
	- 返回值：this

18. **setHzClientHeartbeatTime**
	- 定义：```setHzClientHeartbeatTime(hzClientHeartbeatTime: Int): this.type ```
	- 功能描述：设置hz client的心跳时间，
	- 参数：
		- hzClientHeartbeatTime：hz client的心跳时间
	- 返回值：this

19. **setHzIOThreadCount**
	- 定义：```setHzIOThreadCount(hzIOThreadCount:Int):this.type ```
	- 功能描述：设置hazelcast的IO操作的线程数
	- 参数：
		- hzIOThreadCount：IO操作线程数
	- 返回值：this

20. **setPsTableName**
	- 定义：```setPsTableName(psTableName: String*): this.type ```
	- 功能描述：设置本次作业使用的PS中的表名
	- 参数：
		- psTableName：为可变参数，参数服务器中使用的表名，如果使用多张表，可以设置多个。
	- 返回值：this

21. **setPsDataType**
	- 定义：```setPsDataType(psDataType: PSDataType): this.type ```
	- 功能描述：设置参数服务器中使用的value的数据类型
	- 参数：
		- psDataType：PSDataType的枚举类型，目前包括Float，Double，Float_array和Double_array
	- 返回值：this

22. **setPsDataLength**
	- 定义：```setPsDataLength(psDataLength: Int): this.type ```
	- 功能描述：设置参数服务器中使用的value的数据长度
	- 参数：
		- psDataLength：参数服务器中value的数据长度
	- 返回值：this

23. **setPredictModelPath**
	- 定义：```setPredictModelPath(predictModelPath:String):this.type ```
	- 功能描述：设置预测作业中需要使用的模型路径
	- 参数：
		- predictModelPath：需要载入模型的HDFS路径
	- 返回值：this
