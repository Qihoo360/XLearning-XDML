# BinaryClassify

---


> 二分类模型中计算梯度和损失的工具类

## 功能

* 计算二分类模型中训练数据的梯度和损失值，并且可以用predict函数进行预测。

## 核心接口

1. **train**
	- 定义：
			```
			train(label: Double,
	              feature: (Array[Long], Array[Float]),
	              localW: Map[Long, Float],
	              subsampling_rate: Double = 1.0,
	              subsampling_label: Double = 0.0
	             ): (Map[Long, Float], Double)
			 ```
	- 功能描述：处理原始的文本类型的RDD数据，返回新的数值化的RDD
	- 参数：
		- label：样本的标签label
		- feature：样本的特征和对应的value值
		- localW：本地的特征权重值Map
		- subsampling_rate：负采样率（未启用）
		- subsampling_label：负采样标签（未启用）
	- 返回值：
		- (Map[Long, Float], Double) ：计算得到的样本的梯度和损失值
		* 第一列为样本特征对应的梯度，
		* 第二列为样本的损失

1. **predict**
	- 定义：```predict(feature: (Array[Long], Array[Float]),weight: Map[Long, Float]): Double ```
	- 功能描述：对二分类模型进行预测
	- 参数：
		- feature：样本的特征id和value值
		- weight：特征weight权重Map
	- 返回值：Double：该条样本预测的标签