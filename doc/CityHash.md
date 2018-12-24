# CityHash

---


> 计算hash值的工具类，可用于将字符串特征进行hash化得到数值型id，是google开源的cityhash的java版本，具有效率高超低碰撞的特点

## 功能

* google的cityhash算法的java版本，提供32位，64位和128位哈希值运算。


## 核心接口

1. **stringCityHash32**
	- 定义：```int stringCityHash64(String str) ```
	- 功能描述：计算指定字符串的32位的hash值
	- 参数：
		- str：需要计算的字符串
	- 返回值：int：32位hash值

1. **stringCityHash64**
	- 定义：```long stringCityHash64(String str) ```
	- 功能描述：计算指定字符串的64位的hash值
	- 参数：
		- str：需要计算的字符串
	- 返回值：long：64位hash值

1. **stringCityHash128**
	- 定义：```long[] stringCityHash128(String str) ```
	- 功能描述：计算指定字符串的128位的hash值
	- 参数：
		- str：需要计算的字符串
	- 返回值：long[]：128位hash值
