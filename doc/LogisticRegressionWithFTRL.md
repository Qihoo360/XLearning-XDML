# LogisticRegressionWithFTRL

---


> 使用FTRL优化方法的逻辑回归模型

## 功能

* 实现了带有FTRL优化的逻辑回归模型。提供训练的fit方法和预测的predict方法。


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

5. **setAlpha**
	- 定义：```setAlpha(alpha: Float): this.type ```
	- 功能描述：设置FTRL中的参数alpha
	- 参数：
		- alpha：FTRL中的参数alpha
	- 返回值：this

6. **setBeta**
	- 定义：```setBeta(beta: Float): this.type ```
	- 功能描述：设置FTRL中的参数beta
	- 参数：
		- beta:FTRL中的参数beta
	- 返回值：this

7. **setLambda1**
	- 定义：```setLambda1(lambda1: Float): this.type ```
	- 功能描述：设置FTRL中的参数lambda1
	- 参数：
		- lambda1：FTRL中的参数lambda1
	- 返回值：this

8. **setLambda2**
	- 定义：```setLambda2(lambda2: Float): this.type ```
	- 功能描述：设置FTRL中的参数lambda1
	- 参数：
		- lambda2：FTRL中的参数lambda1
	- 返回值：this
