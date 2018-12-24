# PSMapStore

---


> PSMapStore实现了hazelcast中的MapStore接口。用于实现hazelcast缓存中数据向Kudu中的持久化，参数服务器会优先从hazelcast中拉取参数，如果没有命中则调用该接口从kudu中拉取或者初始化。

## 功能

* 实现了hazelcast和kudu交互的接口规范，是实现两级式参数服务器的关键核心接口。数据在kudu中的存储统一为Array[Byte]字节数组的格式


## 核心接口

1. **init**
	- 定义：```init(): Unit ```
	- 功能描述：用于本实例属性的初始化函数，在构造实例是自动调用
	- 参数：无
	- 返回值：无

2. **store**
	- 定义：```store(key: Long, value: Array[Byte]): Unit ```
	- 功能描述：将hazelcast中的k-v数据存储到kudu中
	- 参数：
		- key: 存储数据的键
		- value: 存储数据的值
	- 返回值：无

3. **storeAll**
	- 定义：```def storeAll(map: util.Map[Long, Array[Byte]]): Unit ```
	- 功能描述：将一个键值对集合Map存入kudu中
	- 参数：
		- map：键值对集合Map
	- 返回值：无

4. **load**
	- 定义：```load(key: Long): Array[Byte] ```
	- 功能描述：从kudu中载入一个键值对
	- 参数：
		- key：需要载入的键
	- 返回值：Array[Byte]：返回该键对应的Byte数组的值

5. **loadAll**
	- 定义：```loadAll(keys: util.Collection[Long]): util.Map[Long, Array[Byte]] ```
	- 功能描述：从kudu中拉取一个集合的键值对
	- 参数：
		- keys：需要从kudu中拉取的键值集合
	- 返回值：util.Map[Long, Array[Byte]]：返回拉取得到的键值对集合Map
