# FFMPreocessor

---


> 处理标准libFFM格式数据的工具类

## 功能

* 处理标准的文本libFFM数据，得到样本的RDD

## 核心接口

1. **processData**
	- 定义：```processData(data:RDD[String],separator:String = " "):RDD[(Double,Array[Int],Array[Long],Array[Float])] ```
	- 功能描述：处理原始的文本类型的RDD数据，返回新的数值化的RDD
	- 参数：
		- data：原始的文本数据RDD
		- separator：特征数据之间的分隔符，默认为单个空格
	- 返回值：
		- RDD[(Double,Array[Int],Array[Long],Array[Float])] ：返回数值化的RDD，
		* 第一列为样本标签，
		* 第二列为field数组
		* 第三列为featureId数组
		* 第四列为feature对应的value数组