# XDML运行示例说明
Example目录下是XDML项目特征分析、特征处理和模型的运行示例。下面分别对上述三个模块的运行示例进行说明。


## 特征分析
特征分析以稠密型数据特征分析（runUniversalAnalyzerDense）为例，对XDML特征分析运行示例进行说明，该运行示例包含在net.qihoo.xitong.xdml.example.analysis.feature.analysis中。对于稀疏数据和分组特征数据分析，用户可以参考该目录下其他两个调用示例。

特征分析调用可以总结为以下步骤：

- 读取数据
- 调用UniversalAnalyzer.fitDense()接口，对读取的数据进行特征分析

下面详细说明：
### 1.数据读取
由于数据分析接口支持数据格式为spark dataframe，所以用户需要将数据读入为dataframe格式。数据处理主要涉及SchemaHandler和DataHandler两个对象。

	 val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter) //读取数据schema
     val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)  //读取数据

#### SchemaHandler
SchemaHandler是针对dataframe schema处理的封装类。SchemaHandler中readSchema主要从文件中读取schema，返回SchemaHandler对象，方便对dataframe格式schema进行处理。

	 def readSchema(sc: SparkContext,
					schemaPath: String, 
					delimiter: String): SchemaHandler
	
- 接口说明
	- sc：sparkcontext对象
	- schemaPath：schema文件所在路径
	- delimiter：列名与类型之间的分隔符
- schema文本格式

  	readSchema从文本文件中读取并解析schema，schema文本必须符合列名(columnName)+分隔符+数据类型(DataType)格式，例如`columnName;DataType`形式。其中columnName和DataType之间的分隔符需要与readSchema中delimiter保持一致，否则解析schema失败。
- schema支持的数据类型
	- Num：数值型特征
	- Cat：类别型特征
	- Label：标签
	- Text：文本型特征
	- MultiCat：多类别型特征
	- Key：id类特征
	- Other：其他特征（无意义数据）

#### DataHandler
DataHandler是XDML中数据读写的封装类，主要支持csv和libsvm格式数据读写。

- 读csv数据

		def readData(spark: SparkSession,
               		dataPath: String,
               		dataDelimiter: String,
               		structType: StructType,
               		nullValue: String,
               		numPartitions: Int = -1,
               		header: Boolean = false): DataFrame
	- 接口说明
		- spark：SparkSession对象
		- dataPath：数据路径
		- dataDelimiter：csv数据分隔符
		- structType：dataFrame的schema
		- nullValue：指定csv数据中作为null的标识
		- numPartitions：指定数据重新分区数，默认为-1，不对数据重新分区
		- header：指定csv数据第一行是否为字段名称，默认为false，不带标题数据
	- 返回结果
		- DataFrame数据
- 写csv数据

		def writeData(df: DataFrame,
                	  path: String,
                	  delimiter: String,
                	  numPartitions: Int = -1,
                	  header: Boolean = false): Unit
	- 接口说明
		- df：需要写文件的DataFrame数据
		- path：文件写入路径
		- delimiter：csv数据分隔符
		- numPartitions：写文件的分区数目，默认为-1，不重新分区
		- header：文件第一行是否为字段名称，默认为false，不写入字段名称
		
- 读libsvm数据
	
		def readLibSVMData(spark: SparkSession,
                           dataPath: String,
                           numFeatures: Int = -1,
                           numPartitions: Int = -1,
                           labelColName: String = "labelLibSVM",
                           featuresColName: String = "featuresLibSVM"): DataFrame
	- 接口说明
		- numFeatures：指定特征个数，默认为-1
		- labelColName：指定标签列名称，默认为labelLibSVM
		- featuresColName：指定特征类名称，默认为featuresLibSVM
	- 返回结果
		- DataFrame数据
- 写libsvm数据

		def writeLibSVMData(df: DataFrame,
                      		labelColName: String,
                      		featuresColName: String,
                      		path: String,
                      		numPartitions: Int = -1): Unit
	- 接口说明
		- 字段名称含义参考读取libsvm数据介绍
		- dataframe中数据features列数据要求是vector类型


### 2.特征分析
用户获取数据之后，可以直接调用UniversalAnalyzer.fitDense()对数据进行分析。

	 val (catFeatSummaryArray, numFeatSummaryArray) = UniversalAnalyzer.fitDense(orgDataDF, hasLabel, labelColName,
                                                      schemaHandler.catFeatColNames, schemaHandler.numFeatColNames,
                                                      Range(0, fineness+1).toArray.map{ d => 1.0*d/fineness })

	
接口字段含义和返回结果介绍，可以参考FeatureAnalysis详细的接口介绍。

## 特征处理
特征处理涉及类别型、数值型特征预处理常见算子，主要包括CategoryEncoder，MultiCategoryEncoder，NumericBucketer，NumericStandardizer四个算子。运行示例包含在net.qihoo.xitong.xdml.example.analysis.feature.process中。该部分以CategoryEncoder（runCategoryEncoder）为例，对XDML特征处理算子调用进行说明。

特征处理调用可以总结为以下步骤：

- 读取数据
- 创建算子对象
- 算子对象fit数据，生成模型
- 模型transform数据，得到变换后的数据


上述步骤中涉及spark [Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html)的Transformer和Estimator接口的概念。下面详细介绍各个步骤：

- 读取数据
   
  	读取数据参考特征分析数据读取部分介绍。

	 		val schemaHandler = SchemaHandler.readSchema(spark.sparkContext, schemaPath, schemaDelimiter)
     		val orgDataDF = DataHandler.readData(spark, dataPath, dataDelimiter, schemaHandler.schema, nullValue, numPartition)
- 创建算子

	算子创建接口可以参考FeatureProcess部分接口说明。
		
		val indexer = new CategoryEncoder()
      				.setInputCols(schemaHandler.catFeatColNames)
      				.setOutputCols(schemaHandler.catFeatColNames.map(name => name + "xdmlIndexed"))
      				.setIndexOnly(true)

- fit数据

	该步骤主要调用算子对象fit接口，生成算子模型。

		val indexerModel = indexer.fit(orgDataDF)
- transform数据

	该步骤主要调用生成模型的transform接口对数据进行变换。
	
		val processedDataDF = indexerModel.transform(orgDataDF)

## 模型
XDML模型主要涉及用scope算优化的线性模型和对H2O中部分模型的spark pipeline封装。运行示例包含在net.qihoo.xitong.xdml.example.analysis.model中。该部分以LinearScope（runFromLibSVMDataToXDMLLinearScopeModel）模型作为示例，对XDML中模型调用进行说明。

模型调用可以总结为以下步骤：

- 读取数据
- 创建模型算子对象
- 模型训练
- 模型验证/预测

下面详细介绍各个步骤：

- 读取数据
	
		val trainData = DataProcessor.readLibSVMDataAsDF(spark, trainPath, numFeatures, numPartitions,
      													oneBased, needSort, rescaleBinaryLabel)

- 创建模型算子对象

	详细参考Model说明文件中api说明
		
		val linearScope = new LinearScope()
        				.setFeaturesCol("features")
        				.setLabelCol("label")
        				.setMaxIter(numIterations)
        				.setStepSize(stepSize)
        				.setLossFunc(lossType)
        				.setRegParam(regParam)
        				.setElasticNetParam(elasticNetParam)
        				.setFactor(factor)
        				.setFitIntercept(fitIntercept)
        				.setPosWeight(posWeight)
        				.setConvergenceTol(convergenceTol)
        				.setNumPartitions(numPartitions)

- 模型训练
	
		val model = linearScope.fit(trainData)

- 模型验证/预测
		
		val df = model.transform(validData)



## 运行任务说明

可以通过spark-submit将作业提交到spark集群。

		spark-submit \
        --class net.qihoo.xitong.xdml.example.analysis.model.runFromLibSVMDataToXDMLLinearScopeModel \
        --name runFromLibSVMDataToXDMLLinearScopeModel \
        --driver-memory 10G \
        --driver-cores 2 \
        --num-executors 8 \
        --executor-cores 2 \
        --executor-memory 10G \
        --queue default \
        ./XDML-1.0-SNAPSHOT.jar \
        ${trainPath} ${validPath} ${numFeatures} ${numPartitions} \
        ${oneBased} ${needSort} ${rescaleBinaryLabel} ${stepSize} \
        ${maxIter} ${regParam} ${elasticNetParam} \



