import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks._

/**
  * @author wangxingda
  *			2018/08/16
  */

class DataColFilter extends DataFilter {
	val featureFilterSet = new mutable.HashSet[String]
	def readFilterConfig(filePath: String): Unit = {
		val file = Source.fromFile(filePath)
		var isStarted:Boolean = false
		for (line <- file.getLines() if !line.trim.equals("")) {
			if(line.contains("[col]")){
				isStarted = true
			} else if(isStarted){
				breakable{
					if(line.trim.contains("[row]")){
						isStarted = false
						break
					}
					val kv = getKeyAndValue(line.trim)
					if(kv._2 == 1)
						featureFilterSet.add(kv._1)
				}
			}
		}
		println("colFilter:" + featureFilterSet.mkString(","))
	}

	def colFilter(feature: Array[String]): Array[String] = {
		feature.filter(x => !featureFilterSet.contains(x.split(splitC)(0)))
	}

	//get key value from k = v
	def getKeyAndValue(str: String): (String, Long) = {
		if (str.equals(""))
			throw new IllegalArgumentException
		val splits = str.split("=")
		if(splits.length != 2){
			throw new IllegalArgumentException("Col filter getKeyAndValue Error:  cause by splits's length is not equals 2:  " + str)
		}
		var value = -1
		try{
			value = splits(1).trim.toInt
		}catch{
			case ex:Exception => println("Col filter getKeyAndValue Error:  cause by value is not a number value:  " + str)
				ex.printStackTrace()
		}
		(splits(0).trim, value)
	}
}
