# LogisticRegressionWithDCASGD

---


> 带有冲量的逻辑回归模型

## 功能

* 实现了带有冲量的逻辑回归模型，使用SGD的优化方法。提供训练的fit方法和预测的predict方法。


## 核心接口

1. **fit**
	- 定义：```fit(data: RDD[(Double, Array[Long], Array[Float])]): (Map[Int, Double], (Long, Long)) ```
	- 功能描述：模型的训练方法
	- 参数：
		- data：训练的输入数据RDD，1为label标签，2为特征id数组，3为相应特征值数组
	- 返回值：
		- (Map[Int, Double], (Long, Long))：
		- 第一列为训练信息Map，key为迭代轮次，value为每轮的loss损失
		- 第二列为训练数据的正负样本数

2. **predict**
	- 定义：```def predict(data: RDD[(Double, Array[Long], Array[Float])]): RDD[(Double, Double)] ```
	- 功能描述：模型的预测方法
	- 参数：
		- data：预测的输入数据RDD，1为label标签，2为特征id数组，3为相应特征值数组
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

6. **setMomemtumCoff**
	- 定义：```setMomemtumCoff(coff:Float):this.type ```
	- 功能描述：设置冲量系数
	- 参数：
		- coff：冲量系数
	- 返回值：this
