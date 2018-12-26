# 特征处理
XDML特征处理模块包含类别型特征oneHot，multiHot，数值型特征分箱和标准化等常见的数据预处理算子。特征处理的所有算子都支持多个特征并行处理，相比spark mllib原生算子处理效率更高。特征处理算子封装遵循spark [Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html)的[Transformer](https://spark.apache.org/docs/latest/ml-pipeline.html#transformers)和[Estimator](https://spark.apache.org/docs/latest/ml-pipeline.html#estimators)标准形式。

## 输入输出说明
特征处理模块除了ColumnRenamer和ColumnEliminator是transformer算子，其余都是estimator算子。两种类型的算子输入输出有所不同，下面分别说明。

### Estimator算子
Estimator算子接受spark标准的[dataframe](https://spark.apache.org/docs/latest/ml-pipeline.html#dataframe)格式输入，产生一个模型（transformer类型）。生成的模型才真正用于特征处理。与此同时，生成模型可以通过save()接口保存到硬盘。

### Transformer算子
Transformer算子或者Estimator产生的model，输入输出都是spark dataframe格式。输入的每一列特征都会生成新的一列特征，新特征的数据类型与具体算子相关，可以参考接下来的算子说明。


## 算子说明


### CategoryEncoder
该算子用于类别型特征进行oneHot或indexing处理。

	val encoder = new CategoryEncoder()
                .setInputCols(Array[String])                
                .setOutputCols(Array[String])               
                .setIndexOnly(Boolean)                      
                .setDropInputCols(Boolean)                  
                .setCategoriesReserved(Int)
				
- 接口说明
	- setInputCols(Array[String])：指定进行类别型编码的特征列名称
		- 支持多列特征同时处理，提升处理速度
	- setOutputCols(Array[String])：指定编码后的特征列名称，与输入列一一对应
	- setIndexOnly(Boolean)：指定算子是否只对特征进行索引化(indexing)，而不进行oneHot；默认只做索引化处理
		- true：只做indexing；输出为类别编码
		- false：oneHot处理；输出为oneHot稀疏向量
	- setDropInputCols(Boolean)：指定算子处理后，是否删除原始特征列；默认不删除原始列
		- true：删除原始特征列
		- false：不删除原始特征列
	- setCategoriesReserved(Int)：特征值类别过多时，设置保留top k个类别；默认为0，保留所有类别
- 调用示例

		val encoderModel = encoder.fit(dataframe)
		val dfProcessed = encoderModel.transform(dataframe)
  参考net.qihoo.xitong.xdml.example.analysis.feature.process.runCategoryEncoder.scala 运行示例

### MultiCategoryEncoder
该算子用于类别型特征进行multiHot处理。
	
	val encoder = new MultiCategoryEncoder() 
                .setInputCols(Array[String])                      
                .setOutputCols(Array[String])                  
                .setIndexOnly(Boolean)                            
                .setDropInputCols(Boolean)                        
                .setCategoriesReserved(Int)
                .setDelimiter(String)
                .setOutputSparse(Boolean)  

- 接口说明
	- setInputCols(Array[String])：指定需要进行multiHot的特征列名称
	- setOutputCols(Array[String])：指定编码后的特征列名称，与输入列一一对应
	- setIndexOnly(Boolean)：指定算子是否只对特征进行索引化（indexing），而不进行multiHot；默认只做索引化处理
		- true：只会indexing；输出为类别索引的稠密向量
		- false：multiHot处理；输出为multiHot的稠密或稀疏向量
	- setDropInputCols(Boolean)：指定算子处理后，是否删除原始特征列；默认不删除原始列
		- true：删除原始特征列
		- false：不删除原始特征列
	- setCategoriesReserved(Int)：特征类别过多时，设置保留top k个类别；默认为0，保留所有类别
	- setDelimiter(String)：设置输入列各个类别之间的分隔符，默认为“,”分隔
	- setOutputSparse(Boolean)：设置multiHot处理输出为稠密向量或者稀疏向量；默认输出为稀疏向量
		- true：输出为稀疏向量
		- false：输出为稠密向量
- 调用示例

		val encoderModel = encoder.fit(dataframe)
		val dfProcessed = encoderModel.transform(dataframe)
  参考net.qihoo.xitong.xdml.example.analysis.feature.process.runMultiCategoryEncoder.scala 运行示例

### NumericBucketer
该算子用于数值型特征(等频)分箱处理。

	 val numericBucketer = new NumericBucketer()
                         .setInputCols(Array[String])                      
                         .setOutputCols(Array[String])           
                         .setDropInputCols(Boolean)
                         .setNumBucketsArray(Array[Int])
                         .setRelativeError(Double)
                         .setIndexOnly(Boolean)
                         .setOutputSparse(Boolean)	

- 接口说明
	- setInputCols(Array[String])：指定需要进行分箱处理的特征列名称
	- setOutputCols(Array[String])：指定分箱后的特征列名称，与输入列一一对应
	- setDropInputCols(Boolean)：指定算子处理后，是否删除原始特征列；默认不删除原始列
		- true：删除原始特征列
		- false：不删除原始特征列
	- setNumBucketsArray(Array[Int])：为各个输入特征列设置分箱数目
	- setRelativeError(Double)：设置分箱误差，默认为0.0005
	- setIndexOnly(Boolean)：指定特征分箱之后，进行indexing处理或oneHot处理；默认为true，只做索引化处理
		- true：只会indexing；输出为类别编码
		- false：oneHot处理；输出为oneHot之后的稀疏或稠密向量
	- setOutputSparse(Boolean)：设置oneHot处理输出为稠密向量或者稀疏向量；默认输出为稀疏向量
		- true：输出为稀疏向量
		- false：输出为稠密向量
- 调用示例

		val numericBucketerModel = numericBucketer.fit(dataframe)
		val dfProcessed = numericBucketerModel.transform(dataframe)
参考net.qihoo.xitong.xdml.example.analysis.feature.process.runNumericBucketer.scala 运行示例

### NumericStandardizer
该算子用于数值型特征标准化处理。
	
	val numericStandardizer = new NumericStandardizer()
                            .setInputCols(Array[String])             
                            .setOutputCols(Array[String])
                            .setDropInputCols(Boolean)
- 接口说明
	- setInputCols(Array[String])：指定需要进行标准化处理的特征列名称
	- setOutputCols(Array[String])：指定标准化后的特征列名称，与输入列一一对应
	- setDropInputCols(Boolean)：指定算子处理后，是否删除原始特征列；默认不删除原始列
		- true：删除原始特征列
		- false：不删除原始特征列
- 调用示例
	
		val numericStandardizerModel = numericStandardizer.fit(dataframe)
		val dfProcessed = numericStandardizerModel.transform(dataframe)
参考net.qihoo.xitong.xdml.example.analysis.feature.process.runNumericStandardizer.scala 运行示例

### ColumnRenamer
该算子用于dataframe列重命名，实现对dataframe.withColumnRenamed()接口的transformer封装。

	val columnRenamer = new ColumnRenamer()
                      .setInputCols(Array[String])             
                      .setOutputCols(Array[String])

- 接口说明
	- setInputCols(Array[String])：指定需要重命名的特征列名称
	- setOutputCols(Array[String])：指定新的列名称，与输入列一一对应	
- 调用示例

		val dfProcessed = columnRenamer.transform(dataframe)

### ColumnEliminator
该算子用于dataframe列删除，实现对dataframe.drop()接口的transformer封装。
	
	val columnEliminator = new ColumnEliminator()
                         .setInputCols(Array[String])
- 接口说明
	- setInputCols(Array[String])：指定需要删除的特征列名称
- 调用示例

		val dfProcessed = columnEliminator.transform(dataframe)

## 特征处理pipeline
### FeatureProcessor

featureProcessor是利用上述特征处理算子实现对特征进行处理的[Pipeline stage](https://spark.apache.org/docs/latest/ml-pipeline.html#properties-of-pipeline-components)串联。

	def pipelineFitTransform(df: DataFrame,
                           schemaHandler: SchemaHandler,
                           methodForNum: String,
                           ifOnehotForNum: Boolean,
                           ifOnehotForCat: Boolean,
                           ifOnehotForMultiCat: Boolean,
                           ifMerge: Boolean,
                           numBuckets: Int = 40,
                           categoriesReservedForCat: Int = 0,
                           categoriesReservedForMultiCat: Int = 0,
                           multiCatDelimiter: String = ","): (DataFrame, PipelineModel)
- 输入参数说明
	- df：输入数据
	- schemaHandler：Schema处理对象
	- methodForNum：数值型特征处理方法，支持三种处理方式
		- "normalize"：标准化，调用NumericStandardizer算子处理
		- "bucketize"：分箱，调用NumericBucketer算子处理
		- "remain"：保持，不做处理
	- ifOnehotForNum：指定是否对分箱后的数值型特征进行oneHot处理
	- ifOnehotForCat：指定是否对类别型特征进行oneHot处理
	- ifOnehotForMultiCat：指定是否对多类别特征进行multiHot处理
	- ifMerge：是否对处理之后的数值型，类别型和多类别型特征进行整合
		- true：对处理后的特征整合成特征向量，可以作为后续模型输入
		- 如果将特征整合成向量形式，输出列名称为"featuresProcessedByFeatureProcessor"
	- numBuckets：指定数值型特征的分箱数目，对于所有的特征分箱数目相同，默认值为40
	- categoriesReservedForCat：设定类别型特征编码过程中保留top k类别数量；默认为0，保留所有类别
	- categoriesReservedForMultiCat：设定多类别型特征编码过程中保留top k类别数量；默认为0，保留所有类别
	- multiCatDelimiter：设定多类别特征的类别分隔符，默认为","
- 输出结果说明
	- (DataFrame， PipelineModel)：处理后的dataframe和pipeline处理过程产生的PipelineModel
		- PipelineModel可以进行持久化保存
- 调用示例

	参考net.qihoo.xitong.xdml.example.analysis.feature.process.runFeatureProcess.scala 运行示例