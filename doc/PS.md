# PS

---


> PS是XDML中抽象出来的参数服务器类。

## 功能

* 包括获取参数服务器单例，保存和载入模型，load参数，启动和销毁参数服务器等功能


## 核心接口

1. **getInstance**
	- 定义：```PS.getInstance(sc: SparkContext, psConf: PSConfiguration): PS ```
	- 功能描述：设置作业参数服务器的作业类型，目前有train,predict和increment_train三种作业类型。
	- 参数：
		- sc：Spark作业的SparkContext实例
		- psConf：PSConfiguration的实例，包含了启动参数服务器的所有参数。
	- 返回值：
		- PS：参数服务器的单例

2. **getIPList**
	- 定义：```getIPList: Array[String] ```
	- 功能描述：获取hazelcast server集群启动后的节点IP列表
	- 参数：无
	- 返回值：
		- Array[String]：hazelcast集群节点的ip组成的数组

3. **start**
	- 定义：```start(sc: SparkContext): Unit ```
	- 功能描述：根据已配置的参数和作业类型启动参数服务器
	- 参数：
		- sc：Spark作业的SparkContext实例
	- 返回值：无

4. **createKuduTable**
	- 定义：```createKuduTable(): Unit ```
	- 功能描述：在kudu集群中创建一张新的表
	- 参数：无
	- 返回值：无

5. **startHzCluster**
	- 定义：```startHzCluster(sc: SparkContext) ```
	- 功能描述：根据传入的PSConfiguration的实例参数启动hazelcast集群
	- 参数：
		- sc：Spark作业的SparkContext实例
	- 返回值：无

6. **createHzMapConfig**
	- 定义：```createHzMapConfig(tableName: String): MapConfig ```
	- 功能描述：创建启动hz节点需要的一个MapConfig的实例
	- 参数：
		- tableName：需要创建的表名
	- 返回值：MapConfig：启动hz所需的MapConfig的实例

7. **getAllParametersReady**
	- 定义：
	```getAllParametersReady(sc: SparkContext, tableName: String = psConf.getPsTableName(), kuduContext: KuduContext): Long ```
	- 功能描述：将kudu制定表中所有的参数载入到hz中
	- 参数：
		- sc：Spark作业的SparkContext实例
		- tableName: kudu中指定的表名，默认是PsConf中配置的第一个表名
		- kuduContext：KuduContext实例，类属性
	- 返回值：Long：载入的参数的总个数

8. **getRangeModelWeightFromBytes**
	- 定义：```getRangeModelWeightFromBytes(bytes: Array[Byte], startRange: Int, endRange: Int): Any ```
	- 功能描述：从Byte数组中获取指定范围的泛型数据类型的数据
	- 参数：
		- bytes: 源byte数组数据
		- startRange: 起始索引（包含）
		- endRange: 结束索引（不包含）
	- 返回值：Any：泛型指定的数据类型数据

9. **getRangeBytesFromWeight**
	- 定义：```getRangeBytesFromWeight(values: Array[String], startRange: Int, endRange: Int): Array[Byte] ```
	- 功能描述：从指定的字符串数组中按照ps设置的数据类型将指定范围的数据转换成Byte数组
	- 参数：
		- values：需要处理的字符串数组
		- startRange: 起始索引（包含）
		- endRange: 结束索引（不包含）
	- 返回值：Array[Byte]：转化后的Byte类型的数组

10. **saveModel**
	- 定义：	

			saveModel(sc: SparkContext,
					  outputPath: String,
					  tableName: String = psConf.getPsTableName(),
					  split: String = "\t",
					  weightStartPos: Int = psConf.getWeightIndexRangeStart,
					  weightEndPos: Int = psConf.getWeightIndexRangeEnd,
					  kuduContext: KuduContext = PS.kuduContext): Unit 
			
	- 功能描述：保存训练的模型到HDFS
	- 参数：
		- sc：Spark作业的SparkContext实例
		- outputPath: 模型输入的HDFS路径
		- tableName: 指定模型参数所在kudu指定的表名，
		- split: 模型参数文本的分隔符，默认为\t
		- weightStartPos： 指定需要保存模型参数的起始索引（包含），默认为0，
		- weightEndPos：指定需要保存模型参数的起始索引（不包含），默认为ps中设置的数据长度
		- kuduContext：KuduContext实例，类属性
	- 返回值：无

11. **loadModel**
	- 定义：

			loadModel(sc: SparkContext,
					  inputPath: String,
					  kuduContext: KuduContext,
					  tableName: String = psConf.getPsTableName(),
					  split: String = "\t",
					  weightStartPos: Int = psConf.getWeightIndexRangeStart,
					  weightEndPos: Int = psConf.getWeightIndexRangeEnd
					 ): Unit

	- 功能描述：从HDFS中load模型到kudu集群的指定表中
	- 参数：
		- sc：Spark作业的SparkContext实例
		- inputPath: 需要载入的模型的HDFS路径
		- kuduContext：KuduContext实例，类属性
		- tableName:模型load到kudu指定的表名中
		- split: 模型参数文本的分隔符，默认为\t
		- weightStartPos： 指定需要载入模型参数的起始索引（包含），默认为0，
		- weightEndPos：指定需要载入模型参数的起始索引（不包含），默认为ps中设置的数据长度
	- 返回值：无
