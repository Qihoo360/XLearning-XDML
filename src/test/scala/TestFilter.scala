import net.qihoo.xitong.xdml.utils.CityHash
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TestFilter {
	val rowFilterConfigFile = "ftrl_filter_demo.txt"
	val colFilterConfigFile = "ftrl_filter_demo.txt"
	val splitA = "\01"
	val splitB = "\02"
	val splitC = "\03"


	def main(args: Array[String]): Unit = {
		//配置spark
		val rowF = new DataRowFilter()
		rowF.readFilterConfig(rowFilterConfigFile)
		val colF = new DataColFilter()
		colF.readFilterConfig(colFilterConfigFile)
		val sc = new SparkContext(new SparkConf().setAppName("TestFilter"))
		val trainInputPath = sc.getConf.get("spark.ndml.data.path", "").trim
		if (trainInputPath == "") {
			println(s"\'spark.ndml.data.path\' must be set of input data.")
			System.exit(-1)
		}
		val rawData = sc.textFile(trainInputPath).repartition(1000)

		//使用ArrayBuffer只过一遍数据的新思路
		val s = System.nanoTime()
		val number = rawData.mapPartitions(iter =>{
			val res = new ArrayBuffer[(Double,Array[Long],Array[Float])]
			while(iter.hasNext){
				val line = iter.next()
				if(rowF.rowFilter(line)) {
					val splits = line.split(splitA)
					val list = splits.slice(1, splits.length)
					val featureList = colF.colFilter(list)
					res.append((splits(0).toDouble, featureList.map(x=>CityHash.stringCityHash64(x.split(splitB)(0))), featureList.map(x=>x.split(splitB)(1).toFloat)))
				}
			}
			res.iterator
		})
		val e = System.nanoTime()

		//传统思路
//		val s = System.nanoTime()
//		val number = rawData.filter(line => rowF.rowFilter(line)).map(line => {
//			val splits = line.split(splitA)
//			val list = splits.slice(1, splits.length)
//			val featureList = colF.colFilter(list)
//			(splits(0).toDouble, featureList.map(x=>CityHash.stringCityHash64(x.split(splitB)(0))), featureList.map(x=>x.split(splitB)(1).toFloat))
//		}).count()
//		val e = System.nanoTime()

		println("time:" + (e - s)/1e9)
		println("number:" + number)
		sc.stop()
	}
}
