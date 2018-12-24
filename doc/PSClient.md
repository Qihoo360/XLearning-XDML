# PSClient

---


> PSClient封装了用户模型与参数服务器之间参数交互关键的pull和push方法。

## 功能

* 在参数服务器启动后，PSClient相当于是个代理，用户模型与参数服务器打交道在每一个RDD的partition中都需要一个PSClient的实例与参数服务器进行参数的拉取和推送。其中pull和push的过程中数据类型对用户是无感的，会根据ps启动时设置的数据类型自动匹配，用户只需要向PSClient进行pull和push操作即可，无需关心数据类型的转化。


## 核心接口

1. **pull**
	- 定义：```pull(featureId: util.HashSet[Long], tableName: String = ps.psConf.getPsTableName()): util.Map[Long, V] ```
	- 功能描述：用户从参数服务器中拉去所需要的参数。
	- 参数：
		- featureId: 用户需要拉取的feature的id
		- tableName： 指定需要从哪个表中拉去参数，默认是ps启动时配置的第一张表名
	- 返回值：
		- - util.Map[Long, V]: 用户拉取的参数Map

2. **push**
	- 定义：```push(weightMap: util.Map[Long, V], tableName: String = ps.psConf.getPsTableName()): Unit ```
	- 功能描述：用户向参数服务器推送保存参数。
	- 参数：
		- weightMap： 需要推送的参数id和值
		- tableName： 指定需要向哪张表推送参数，默认是ps启动时配置的第一张表名
	- 返回值：无

3. **init**
	- 定义：```init(): Unit ```
	- 功能描述：初始化本实例的相关参数
	- 参数：无
	- 返回值：无

4. **validDataType**
	- 定义：```validDataType(): Unit ```
	- 功能描述：验证PS中配置的PSDataType和PSClient实例中的泛型是否一致，若不一致则抛出异常
	- 参数：无
	- 返回值：无
	- 抛出异常：XDMLException

5. **setUpdater**
	- 定义：```setUpdater(updater: Updater[Long, V]): this.type ```
	- 功能描述：设置本实例需要使用的Updater，在进行push之前会先调用updater实例中的update方法处理参数后再push到参数服务器。
	- 参数：
		- updater：该PSClient实例设定的updater
	- 返回值：this

6. **setPullRange**
	- 定义：```setPullRange(start: Int, end: Int): this.type ```
	- 功能描述：设置拉去参数的索引范围
	- 参数：
		- start：索引起始值（包含）
		- end：索引结束值（不包含）
	- 返回值：this

7. **setPullRange**
	- 定义：```setPullRange(index: Int): this.type ```
	- 功能描述：设置只拉取某个索引的参数
	- 参数：
		- index：参数索引值
	- 返回值：this

8. **getRangeVFromBytes**
	- 定义：```getRangeVFromBytes(bytes: Array[Byte]): V ```
	- 功能描述：从byte数组中获取指定范围的泛型V指定类型的数据
	- 参数：
		- bytes：数据源Byte数组
	- 返回值：V：泛型数据

9. **shutDown**
	- 定义：```shutDown(): Unit ```
	- 功能描述：关闭本PSClient实例
	- 参数：无
	- 返回值：无

