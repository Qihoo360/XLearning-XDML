## 作业相关配置
    
配置项名称 | 默认值 | 配置项含义  
---------------- | --------------- | ---------------  
spark.xdml.job.type | "train" | 设置作业类型：train,predict,increment_train   
spark.xdml.table.name | "table" | 设置kudu存储的表名称   
spark.xdml.train.data.path | 无 | 训练数据路径  
spark.xdml.train.data.partitionNum |  | 训练数据分区数目   
spark.xdml.data.split | " " | 数据分隔符  
spark.xdml.model.path | 无 | 模型存储路径   
spark.xdml.predict.result.path | 无 | 预测结果输出路径   
spark.xdml.kudu.master | 无 | kudu集群master地址   
spark.xdml.hz.clusterNum | 50 | hazelcast集群节点数目  
spark.xdml.hz.partitionNum | 127 | hazelcast分区数目  
spark.xdml.hz.maxcachesize | 50000000 | hazelcast分区最大缓存大小   
spark.xdml.train.iter |1 | 训练迭代次数  
spark.xdml.train.batchsize | 50 | 训练数据batch大小  
spark.xdml.learningRate | 0.01f| 设置学习率  
spark.xdml.momentumCoff | 0.1f | 动量阻力参数 
spark.xdml.train.alpha | 1f | 设置ftrl等算法训练所需参数alpha  
spark.xdml.train.beta | 1f | 设置ftrl等算法训练所需参数beta  
spark.xdml.train.lambda1 | 1f | 设置ftrl算法训练所需参数lambda1  
spark.xdml.train.lambda2 | 1f | 设置ftrl算法训练所需参数lambda1  
spark.xdml.train.forcesparse | false | 设置ftrl算法是否保持强稀疏性，即w为0时，z、n对应值也为0  
spark.xdml.model.ffm.rank | 1 | 设置ffm算法中的k值  
spark.xdml.model.ffm.field | 1 | 设置ffm算法中的  

