# 特征分析
XDML特征分析模块支持多特征并行、数据one-pass的特征分析，大大提升了大规模数据集下特征分析速度。分析指标覆盖特征自身统计维度和auc、互信息、ndcg等与label相关的维度特征分析。

## 分析维度

### 特征自身统计维度
- **类别型特征**
	- 缺失度
	- 特征类别频率统计
- **数值型特征**
	- 缺失度
	- 均值
	- 标准差
	- 偏度
	- 峰度
	- 最小值
	- 最大值
	- 中位数
	- 分位数

### 与label相关分析
- **类别型特征**
	- auc
	- 互信息
- **数值型特征**
	- 相关系数
	- ndcg
	- 逆序对

## 输入数据格式
特征分析支持dense和sparse两种数据格式，分别对应fitDense()和fitSparse()两个接口。

- Dense
  
	fitDense()接口输入数据类型为spark标准[dataframe](https://spark.apache.org/docs/latest/ml-pipeline.html#dataframe)格式。

- Sparse
	
	fitSparse()接口输入数据为RDD[HashMap[String, String]]形式，其中HashMap中的key-value分别对应为特征名称和特征值。


## 输出结果格式
特征分析结果的各个指标封装在case class里面，下面详细介绍不同类型特征的封装形式。

### <span id="CatFeatSummary">类别型特征</span>

	case class CatFeatSummary(name: String,
                            countAll: Long,
                            countNotNull: Long,
                            concernedCategories: Array[(String, (Long, Int))],
                            mi: Double,
                            auc: Double)
- 字段说明
	- name：特征名称
	- countAll：样本总数量
	- countNotNull：该特征非空值样本数量
	- concernedCategories：该特征不同类别值对应的样本数量和正样本数量，形式为（featureName, (sampleCount, positiveSampleCount))
		- 数组中默认只包含该特征的top 200个类别的统计数据，特征类别的排序方式如下：
			- 有标签的二分类数据按照该类别正样本占比降序排列，其他类型数据（无标签，回归，多分类）按照该类别样本数量降序排列。
		- 有标签的二分类数据集才会统计类别的正样本数量(positiveSampleCount)，否则置为-1
	- mi：特征与label的互信息
		- 有标签的二分类数据集才会计算此指标，否则置为-1
	- auc：特征与label的auc
		- 有标签的二分类数据集才会计算此指标，否则置为-1


### <span id="NumFeatSummary">数值型特征</span>

	case class NumFeatSummary(name: String,
                            countAll: Long,
                            countNotNull: Long,
                            mean: Double,
                            std: Double,
                            skewness: Double,
                            kurtosis: Double,
                            min: Double,
                            max: Double,
                            median: Double,
                            quantileArray: Array[Double],
                            corr: Double,
                            auc: Double)
- 字段说明
	- name：特征名称
	- countAll：样本总数量
	- countNotNull：该特征非空值样本数量
	- mean：均值
	- std：标准差
	- skewness：偏度
	- kurtosis：峰度
	- min：最小值
	- max：最大值
	- median：中位数
	- quantileArray：分位数统计值
	- corr：特征与label相关系数
	- auc：特征与label的auc值（目前未实现，置为-1）

### <span id="GroupFeatSummary">分组(grouped)分析的特征</span>
	case class GroupFeatSummary(name: String,
                              reversePairRate: Double,
                              ndcgMap: HashMap[String, Double])
- 字段说明
	- name：特征名称
	- reversePairRate：该特征正序对与逆序对比值
	- ndcgMap：该特征ndcg计算值
		- ndcg以key-value存入map中，分别表示ndcg指标名称和计算值
		- 用户可以统计ndcg@3,ndcg@5等不同top位置的ndcg值
	

## 接口说明

### UniversalAnalyzer.fitDense()

	fitDense(dataDF: DataFrame,
             hasLabel: Boolean,
             labelColName: String,
             catFeatColNames: Array[String],
             numFeatColNames: Array[String],
             percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
             relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary])
- 输入参数说明
	- dataDF：输入数据
	- hasLabel：数据是否有标签
	- labelColName：标签列名称
	- catFeatColNames：需要进行分析的类别型特征列名称
	- numFeatColNames：需要进行分析的数值型特征列名称
	- percentages：指定数值型特征需要统计的分位点（默认统计0-100全部的分位点数据）
	- relativeError：分位点统计允许误差
- 输出结果说明
	- Array[CatFeatSummary]：按照输入catFeatColNames顺序的类别型特征分析结果，详细参考[CatFeatSummary](#CatFeatSummary)
	- Array[NumFeatSummary]：按照输入numFeatColNames顺序的数值型特征分析结果，详细参考[NumFeatSummary](#NumFeatSummary)
- 调用示例

	参考net.qihoo.xitong.xdml.example.analysis.feature.analysis.runUniversalAnalyzerDense.scala 运行示例
- **特别说明**
	- 与label相关的分析指标，往往和label的类型相关。例如，auc只能在二分类数据集上进行计算。因此fitDense()接口内部会对数据集的label进行采样，如果label的值个数超过2个，auc，互信息，相关系数都会置为-1。
	- 如果数据集没有标签，在hasLabel置为false的情况下，labelColNames可以为任意字符串。


### UniversalAnalyzer.fitSparse()

	fitSparse(dataRDD: RDD[HashMap[String, String]],
              hasLabel: Boolean,
              labelColName: String,
              catFeatColNames: Array[String],
              numFeatColNames: Array[String],
              sparseType: String = "default",
              percentages: Array[Double] = Range(0,101,1).toArray.map(_/100.0),
              relativeError: Double = 5.0e-4): (Array[CatFeatSummary], Array[NumFeatSummary])
- 输入参数说明
	- dataRDD：输入数据
	- hasLabel：数据是否有标签
	- labelColName：标签列名称
	- catFeatColNames：需要进行分析的类别型特征列名称
	- numFeatColNames：需要进行分析的数值型特征列名称
	- percentages：指定数值型特征需要统计的分位点（默认统计0-100全部的分位点数据）
	- relativeError：分位点统计允许误差
- 输出结果说明
	- Array[CatFeatSummary]：按照输入catFeatColNames顺序的类别型特征分析结果，详细参考[CatFeatSummary](#CatFeatSummary)
	- Array[NumFeatSummary]：按照输入numFeatColNames顺序的数值型特征分析结果，详细参考[NumFeatSummary](#NumFeatSummary)
- 调用示例

	参考net.qihoo.xitong.xdml.example.analysis.feature.analysis.runUniversalAnalyzerSparse.scala 运行示例
- **特别说明**
	- 与label相关的分析指标，往往和label的类型相关。例如，auc只能在二分类数据集上进行计算。因此fitSparse()接口内部会对数据集的label进行采样，如果label的值个数超过2个，auc，互信息，相关系数都会置为-1。
	- 如果数据集没有标签，在hasLabel置为false的情况下，labelColNames可以为任意字符串。

### UniversalAnalyzer.fitDenseKSForNum()

该接口主要实现双样本KS检验（[Kolmogorov–Smirnov test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)）功能，用户可以通过该接口检验两份样本数据是否服从同一分布。

	def fitDenseKSForNum(df1: DataFrame, numFeatColNames1: Array[String],
                         df2: DataFrame, numFeatColNames2: Array[String]): Array[Double]

- 输入参数说明
	- df1: 输入dataframe1
	- numFeatColNames1: 计算ks统计的列名称
	- df2: 输入dataframe2
	- numFeatColNames2: 计算ks统计的列名称
- 输出结果说明
	- Array[Double]: 返回零假设的P值（零假设表示样本1和样本2服从同一个分布）
- 调用示例
  
    参考net.qihoo.xitong.xdml.example.analysis.feature.analysis.runUniversalAnalyzerDenseKS 运行示例

- **特别说明**
	- 此接口只能对数值型特征进行ks检验
	- numFeatColNames1与numFeatColNames2中的列一一对应进行KS检验

### UniversalAnalyzer.fitDenseGrouped()
	fitDenseGrouped(df: DataFrame,
                    labelColName: String,
                    groupColName: String,
                    featColNames: Array[String],
                    topKs: Array[Int],
                    gainFunc: (Int) => Double,
                    discountFunc: (Int) => Double): Array[GroupFeatSummary] 
- 输入参数说明
	- df：输入数据
	- labelColName：标签列名称
	- groupColName：group列名称
	- featColNames：特征列名称
	- topKs：需要计算的ndcg的top位置数
	- gainFunc：计算dcg的收益函数
	- discountFunc：计算dcg的折扣函数
- 输出结果说明
	- Array[GroupFeatSummary]：按照输入的featColNames顺序的特征的分析结果，包括逆序对和ndcg统计，详细参考[GroupFeatSummary](#GroupFeatSummary)
- 调用示例

    参考net.qihoo.xitong.xdml.example.analysis.feature.analysis.runUniversalAnalyzerDenseGrouped 运行示例
- **特别说明**
	- 此接口只针对需要进行分组的特征计算逆序对和ndcg指标
	- 由于逆序对和ndcg指标特性，此接口要求数据必须有label，并且label是可排序值，特征只能是数值类型
	- dcg计算需要用户依据自身业务需求定义每个位置的收益和每个位置的折扣
