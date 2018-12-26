# 模型
XDML模型部分主要包括线性模型和树模型，分为GLM和H2O两个模块。[GLM(Generalized linear model)](https://en.wikipedia.org/wiki/Generalized_linear_model)支持常见的线性模型，包括LR，SVM，[Softmax Regression](https://en.wikipedia.org/wiki/Multinomial_logistic_regression)等模型。H2O模块实现了对[H2O](https://www.h2o.ai/)中[DRF(Distributed Random Forest)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/drf.html)、[GBM(Gradient Boosting Machine)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/gbm.html)、[GLM(Generalized Linear Model )](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/glm.html)和[MLP(Multilayer Perceptron)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/deep-learning.html)四个模型的pipeline封装。


## 输入/输出说明
模型主要涉及模型训练，模型验证和数据预测三个阶段，每个阶段的输入输出有所不同，详细说明如下：

### 训练阶段
训练阶段，学习算法在数据集上进行训练产生一个模型。此阶段，学习算法是一个[estimator](https://spark.apache.org/docs/latest/ml-pipeline.html#estimators)，因此输入为spark 标准的[dataframe](https://spark.apache.org/docs/latest/ml-pipeline.html#dataframe)，输出为训练好的模型。输入的dataframe要求包含label和feature两列数据，且feature列只支持向量形式。而产生的模型是一个[transformer](https://spark.apache.org/docs/latest/ml-pipeline.html#transformers)，此模型用于后续验证和预测两个阶段，也可以调用save()接口保存到磁盘。

### 验证阶段
验证阶段，对训练阶段产生的模型进行验证。此阶段，模型是一个transformer，因此输入和输出都为dataframe。由于需要对模型进行评估，因此输入的dataframe与训练阶段相同，需要包含label和feature数据。输出的dataframe会产生预测的label列。后续可以利用实际label和预测label进行模型评估。

### 预测阶段
预测阶段，利用训练阶段产生的模型对新的数据进行预测。此阶段，模型的输入输出与验证阶段类似，只是输入dataframe只需要提供feature数据。输出dataframe会相应增加预测的label数据。

## GLM

### <span id="LinearScope">LinearScope</span>
该算子利用[Scope](https://arxiv.org/pdf/1602.00133.pdf)算法对广义线性模型进行训练，支持LR，SVM，[UPULR](https://arxiv.org/abs/1703.00593)等模型，能在较少的迭代次数取得非常好的模型效果。

	val linearScope = new LinearScope()
			        .setFeaturesCol(String)
			        .setLabelCol(String)
			        .setMaxIter(Int)
			        .setStepSize(Double)
			        .setLossFunc(String)
			        .setRegParam(Double)
			        .setElasticNetParam(Double)
			        .setFactor(Double)
			        .setFitIntercept(Boolean)
			        .setPosWeight(Double)
			        .setConvergenceTol(Double)
			        .setNumPartitions(Int)
			        .setTreeAggregateDepth(Int)
			        .setRestartFrequency(Int)

- 接口说明
	- setFeaturesCol(String)：指定数据的特征列名称，默认为"features"
	- setLabelCol(String)：指定数据的标签列名称，默认为"label"
	- setMaxIter(Int)：设置算法最大迭代次数，默认值为5
	- setStepSize(Double)：设置算法学习率，默认值为1.0
	- setLossFunc(String)：设置损失函数，默认值为“LR”
		- "LR"：逻辑回归模型
		- "SVM"：SVM模型
		- "SSVM"：smooth SVM模型
			- "SSVM"其实是[smooth hinge loss](https://en.wikipedia.org/wiki/Hinge_loss)作为损失函数的svm模型
		- "UPULR"：unbiased Positive-unlabeled LR模型
	- setRegParam(Double)：设置正则化参数，默认为0.0
	- setElasticNetParam(Double)：设置弹性网络参数，默认为0.0（目前没有生效）
	- setFactor(Double)：设置调整label分布参数，默认为0.0（目前没有生效）
	- setFitIntercept(Boolean)：设置是否拟合截距，默认为true，拟合截距
	- setPosWeight(Double)：设置正样本权重，默认为1.0
	- setConvergenceTol(Double)：设置优化算法收敛的阈值，默认为0.001（目前没有生效）
	- setNumPartitions(Int)：设置训练数据分区数目，默认为0，不重新分区
	- setTreeAggregateDepth(Int)：设置treeaggregate的深度，默认为2
	- setRestartFrequency(Int)：为避免浮点数计算误差，重置浮点参数的累积计算次数（内部已经优化，用户可以不用设置该值）
- 调用示例
	
		//-----train----------
		val linearScopeModel = linearScope.fit(trainDF)
  		//val linearScopeModel = linearScope.fit(trainDF,initialWeights)//fit支持自定义初始权重,initialWeights: Vector，默认初始化权重为零向量
        
        //------validate--------
        linearScopeModel.setPredictionCol("prediction")            //设置输出df原使预测列名称,String,默认为"prediction"
						.setProbabilityCol("probability")          //设置输出df概率预测列名称,String,默认为"probability"                                                       	
  		val dfProcessed = linearScopeModel.transform(validDF)
  
    参考net.qihoo.xitong.xdml.example.analysis.model.runFromLibSVMDataToXDMLLinearScopeModel运行示例

### <span id="MultiLinearScope">MultiLinearScope</span>
顾名思义，MultiLinearScope算子利用scope算法对多分类LR模型进行优化，解决多分类问题。


	val mls = new MultiLinearScope()
            .setFeaturesCol(String)
            .setLabelCol(String)
            .setMaxIter(Int)
            .setStepSize(Double)
            .setLossFunc(String)
            .setRegParam(Double)
            .setElasticNetParam(Double)
            .setFactor(Double)
            .setFitIntercept(Boolean)
            .setConvergenceTol(Double)
            .setNumPartitions(Int)
            .setTreeAggregateDepth(Int)
            .setRestartFrequency(Int)
	      	.setNumClasses(Int)

- 接口说明
	- setLossFunc(String)：设置损失函数，默认值为“SR”
		- "SR"：Softmax Regression
	- setNumClasses(Int)：设置多分类的类别数目，默认值为2，指定为二分类问题
	
	除了上述两个接口，MultiLinearScope其他接口含义和默认值均和LinearScope保持一致，具体参考[LinearScope](#LinearScope)模型接口说明。

- 调用示例
        
        //-----train----------
		val mlsModel = mls.fit(trainDF)
        
        //-----validate-------
		val dfProcessed = mlsModel.transform(validDF)
        
	参考net.qihoo.xitong.xdml.example.analysis.model.runFromLibSVMDataToXDMLSR运行示例

### OVRLinearScope
OVRLinearScope为多分类SVM模型。该模型使用One-VS-Rest策略解决多分类问题，采用scope算法进行模型优化，支持hinge loss损失函数。

	val ovrls = new OVRLinearScope()
              .setFeaturesCol(String)
              .setLabelCol(String)
              .setMaxIter(Int)
              .setStepSize(Double)
              .setLossFunc(String)
              .setRegParam(Double)
              .setElasticNetParam(Double)
              .setFactor(Double)
              .setFitIntercept(Boolean)
              .setConvergenceTol(Double)
              .setNumPartitions(Int)
              .setTreeAggregateDepth(Int)
              .setRestartFrequency(Int)
	      	  .setNumClasses(Int)
- 接口说明
	- setLossFunc(String)：设置损失函数，默认值为“SVM”
		- “SVM”：SVM模型
		- “SSVM”：SSVM模型
	
	除了setLossFunc()支持的损失函数不同，OVRLinearScope其他接口含义和默认值均和MultiLinearScope保持一致，具体参考[MultiLinearScope](#MultiLinearScope)模型接口说明。
- 调用示例
	
		//-----train----------
		val ovrlsModel = ovrls.fit(trainDF)

		//-----validate-------
		val dfProcessed = ovrlsModel.transform(validDF)

	参考net.qihoo.xitong.xdml.example.analysis.model.runFromLibSVMDataToXDMLOVR运行示例
	
## H2O
该部分模型主要实现对[H2O](https://www.h2o.ai/)中DRF、GBM、GLM和MLP四个模型进行Spark pipeline封装。

### <span id="H2ODRF">H2ODRF</span>
[DRF(Distributed Random Forest)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/drf.html)，是H2O中随机森林模型。

	val h2oDRF = new H2ODRF()
		      .setLabelCol(String)
		      .setCatFeatColNames(Array[String])
		      .setIgnoreFeatColNames(Array[String])
		      .setMaxDepth(Int)
		      .setNumTrees(Int)
		      .setMaxBinsForCat(Int)
		      .setMaxBinsForNum(Int)
		      .setMinInstancesPerNode(Int)
		      .setCategoricalEncodingScheme(String)
		      .setHistogramType(String)
		      .setDistribution(String)
		      .setScoreTreeInterval(Int)

- 接口说明
	- setLabelCol(String)：指定数据的标签列名称，用户需要指定标签列名称，否则报错
	- setCatFeatColNames(Array[String])：指定数据集中类别型特征列名称，对类别型特征进行Enum编码能取得更好的模型效果
	- setIgnoreFeatColNames(Array[String])：指定需要忽略的特征列名称
	- setMaxDepth(Int)：设置树最大深度，默认值为20
	- setNumTrees(Int)：设置树的颗数，默认值为50
	- setMaxBinsForCat(Int)：设置类别型特征的最大分箱数目，默认值为256
	- setMaxBinsForNum(Int)：设置数值型特征的最大分箱数目，默认值为20
	- setMinInstancesPerNode(Int)：设置节点分裂过程中子节点至少包含的样本数量，默认值为1
	- setCategoricalEncodingScheme(String)：设置类别型特征编码的方式，默认为"Enum"
		- 支持"AUTO"，"Enum"，"LabelEncoder"，"OneHotExplicit"，"SortByResponse"
	- setHistogramType(String): 设置直方图统计方式，默认为"QuantilesGlobal"
		- 支持"AUTO", "UniformAdaptive", "QuantilesGlobal"
	- setDistribution(String): 设置label服从的分布，默认为"bernoulli"分布
		- 支持"bernoulli", "multinomial", "gaussian"三种分布
	- setScoreTreeInterval(Int): 设置每构建多少颗树，就对模型进行评估，默认为0(构建过程不进行模型评估)
- 调用示例
	    
        //-----train----------
		val drfModel = h2oDRF.fit(trainDF)
		
		//-----validate-------
 		val dfProcessed = drfModel.transform(validDF)

	参考net.qihoo.xitong.xdml.example.analysis.model.runFromDenseDataToXDMLH2ODRF运行示例


### H2OGBM
[GBM(Gradient Boosting Machine)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/gbm.html)，是H2O中梯度提升模型。

	val h2oGBM = new H2OGBM()
		      .setLabelCol(String)
		      .setCatFeatColNames(Array[String])
		      .setIgnoreFeatColNames(Array[String])
		      .setMaxDepth(Int)
		      .setNumTrees(Int)
		      .setMaxBinsForCat(Int)
		      .setMaxBinsForNum(Int)
		      .setMinInstancesPerNode(Int)
		      .setCategoricalEncodingScheme(String)
		      .setHistogramType(String)
		      .setLearnRate(Double)
		      .setLearnRateAnnealing(Double)
		      .setDistribution(String)
		      .setScoreTreeInterval(Int)
- 接口说明
	- setMaxDepth(Int)：设置树深度，默认为5
	- setMinInstancesPerNode(Int)：设置节点分裂过程中子节点至少包含的样本数量，默认值为10
	- setLearnRate(Double)：设置学习率，默认为0.1
	- setLearnRateAnnealing(Double)：设置学习率的衰减因子，默认为1（不衰减）
		- 每构造一棵树，学习率都会乘上该衰减因子
	
除了上述接口，H2OGBM其他接口含义和默认值都和H2ODRF保持一致，具体参考[H2ODRF](#DRF)模型接口说明。

- 调用示例
	    
        //-----train----------
		val gbmModel = h2oGBM.fit(trainDF)

        //-----validate-------
		val dfProcessed = gbmModel.transform(validDF)

	参考net.qihoo.xitong.xdml.example.analysis.model.runFromDenseDataToXDMLH2OGBM运行示例


### H2OGLM
[GLM(Generalized Linear Model )](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/glm.html)，是H2O中广义线性模型。

	val h2oGLM = new H2OGLM()
		      .setLabelCol(String)
		      .setCatFeatColNames(Array[String])
		      .setIgnoreFeatColNames(Array[String])
		      .setFamily(String)
		      .setMaxIter(Int)
		      .setAlpha(Double)
		      .setLambda(Double)
		      .setFitIntercept(Boolean)
		      .setStandardization(Boolean)
		      .setMissingValueHandling(String)

- 接口说明
	- setFamily(String)：设置label的分布类型，默认为"binomial"
		- 支持"binomial", "multinomial", "gaussian"
	- setMaxIter(Int)：设置最大迭代次数，默认为100
	- setAlpha(Double)：设置弹性网络(elasticnet)参数，默认为0，
		- alpha取值范围[0,1]，alpha=0时，为L2正则；alpha=1时，为L1正则
	- setLambda(Double)：设置正则化参数，默认为0.0
	- setFitIntercept(Boolean)：设置是否拟合截距，默认为true
	- setStandardization(Boolean)：设置是否对数值型特征进行标准化，默认为true
	- setMissingValueHandling(String)：设置缺失值的处理方式，默认为"MeanImputation"，均值替代
		- 支持"Skip", "MeanImputation"两种处理方式
- 调用示例

        //-----train----------
		val glmModel = h2oGLM.fit(trainDF)
    	
		//-----validate-------
    	val dfProcessed = glmModel2.transform(validDF)

	参考net.qihoo.xitong.xdml.example.analysis.model.runFromDenseDataToXDMLH2OGLM运行示例

### H2OMLP
[MLP(Multilayer Perceptron)](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/deep-learning.html)， 是H2O中多层感知器模型。

	val h2oMLP = new H2OMLP()
		      .setLabelCol(String)
		      .setCatFeatColNames(Array[String])
		      .setIgnoreFeatColNames(Array[String])
		      .setMissingValueHandling(String)
		      .setCategoricalEncodingScheme(String)
		      .setDistribution(String)
		      .setHidden(Array[Int])
		      .setActivation(String)
		      .setEpochs(Double)
		      .setLearnRate(Double)
		      .setMomentumStart(Double)
		      .setMomentumStable(Double)
		      .setL1(Double)
		      .setL2(Double)
		      .setHiddenDropoutRatios(Array[Double])
		      .setElasticAveraging(Boolean)
		      .setStandardization(Boolean)
- 接口说明
	- setDistribution(String)：设置分布类型，默认为"bernoulli"
		- 支持"bernoulli", "multinomial"
	- setHidden(Array[Int])：设置隐含层节点数目，默认为[200,200]
	- setActivation(String)：设置激活函数，默认为"Rectifier"
		- 支持"Rectifier", "RectifierWithDropout", "Maxout", "MaxoutWithDropout", "ExpRectifier", "ExpRectifierWithDropout", "Tanh", "TanhWithDropout"
	- setEpochs(Double)：设置迭代次数，默认为10
	- setLearnRate(Double)：设置学习率，默认为0.1
	- setMomentumStart(Double)：设置动量初始值，默认为0.5
	- setMomentumStable(Double)：设置动量稳定值，默认为0.99
	- setL1(Double)：设置l1正则化参数，默认为0
	- setL2(Double)：设置l2正则化参数，默认为0
	- setHiddenDropoutRatios(Array[Double])：设置droupout率，默认为[0.5]
	- setStandardization(Boolean)：设置是否进行标准化，默认为true
- 调用示例
	
		 //-----train----------
		 val mlpModel = h2oMLP.fit(trainDF)
	
         //-----validate-------		
		 val processedDF = mlpModel.transform(validDF)

	参考net.qihoo.xitong.xdml.example.analysis.model.runFromDenseDataToXDMLH2OMLP运行示例