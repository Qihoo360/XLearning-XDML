# FieldawareFactorizationMachine

---


> FFM模型

## 功能

* 实现了FFM，使用SGD的优化方法。提供训练的fit方法和预测的predict方法。


## 核心接口

1. **fit**
	- 定义：```fit(data: RDD[(Double, Array[Int], Array[Long], Array[Float])]): (Map[Int, Double], (Long, Long)) ```
	- 功能描述：模型的训练方法
	- 参数：
		- data：训练的输入数据RDD，1为label标签，2为特征的filed域，3为特征id数组，4为相应特征值数组
	- 返回值：
		- (Map[Int, Double], (Long, Long))：
		- 第一列为训练信息Map，key为迭代轮次，value为每轮的loss损失
		- 第二列为训练数据的正负样本数

2. **predict**
	- 定义：```def predict(data: RDD[(Double, Array[Long], Array[Float])]): RDD[(Double, Double)] ```
	- 功能描述：模型的预测方法
	- 参数：
		- data：预测的输入数据RDD，1为label标签，2为特征的filed域，3为特征id数组，4为相应特征值数组
	- 返回值：
		- RDD[(Double, Double)]：样本label和预测的label

3. **setIterNum**
	- 定义：```setIterNum(iterNum: Int): this.type ```
	- 功能描述：设置数据迭代次数
	- 参数：
		- iterNum: 迭代次数
	- 返回值：this

4. **setBatchSize**
	- 定义：```setBatchSize(batchSize: Int): this.type ```
	- 功能描述：设置训练batch的大小
	- 参数：
		- batchSize：一组batch的大小
	- 返回值：this

5. **setLearningRate**
	- 定义：```setLearningRate(lr: Float): this.type ```
	- 功能描述：设置模型的学习率
	- 参数：
		- lr：学习率
	- 返回值：this

6. **setRank**
	- 定义：```setRank(rank:Int):this.type ```
	- 功能描述：设置FFM的单个域的向量长度
	- 参数：
		- rank：域向量长度
	- 返回值：this

6. **setField**
	- 定义：```setField(field:Int):this.type ```
	- 功能描述：设置FFM的单个域的向量长度
	- 参数：
		- field：设置field域的个数
	- 返回值：this