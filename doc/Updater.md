# Updater

---


> Updater是PSClient中用到的一个接口，用户可以自定义实现其中的Updater方法，PSClient会在push数据到参数服务器之前先用updater方法处理传入的参数和梯度数据。

## 功能

* 定义参数服务器中更新参数的方法，在执行push操作时自动调用。


## 核心接口

1. **setJobType**
	- 定义：```update(originalWeightMap: java.util.Map[K, V], gradientMap: java.util.Map[K, V]):java.util.Map[K, V] ```
	- 功能描述：定义用户参数服务器更新参数方式的接口规范
	- 参数：
		- originalWeightMap：参数服务器上的原始参数
		- gradientMap：一般是梯度列表参数
	- 返回值：java.util.Map[K, V] ：用户自定义更新后的Map数据，也就是真正更新到参数服务器的Map数据