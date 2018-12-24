import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * @author wangxingda
  *	 		2018/08/16
  */

class DataRowFilter extends DataFilter {
	val classifyFilterSet = new mutable.HashSet[String]
	val classifyOnlyKeepMap = new mutable.HashMap[String, mutable.HashSet[String]]
	val andClassifyFilterSet = new mutable.HashSet[ArrayBuffer[(String,mutable.HashSet[String])]]
//	val andClassifyOnlyKeepMap = new mutable.HashSet[String, ArrayBuffer[String]]

	val valueFilterMap = new mutable.HashMap[String, (Double, Double)]
	val valueOnlyKeepMap = new mutable.HashMap[String, (Double, Double)]
	val andValueFilterMap = new mutable.HashSet[ArrayBuffer[(String, Double, Double)]]()
//	val andValueOnlyKeepSet = new mutable.HashSet[ArrayBuffer[(String, Double, Double)]]()

	def rowFilter(line: String): Boolean = {
		!(rowClassifyFilter(line, classifyFilterSet, classifyOnlyKeepMap,andClassifyFilterSet) || rowValueFilter(line, valueFilterMap, valueOnlyKeepMap,andValueFilterMap))
	}

	//read filter config
	@Override
	def readFilterConfig(filePath: String): Unit = {
		val file = Source.fromFile(filePath)
		var needFilter = -1 //-1 : invalid value，1 : which should be filtered，0 : keep these only and filter others
		for (line <- file.getLines() if !line.trim.equals("")) {
			if (line.startsWith("[") && line.endsWith("]") && line.contains("filter=")) {
				val str = line.trim.substring(1, line.trim.length - 1)
				val kv = getKeyAndValue(str)
				needFilter = kv._2.toInt
				//println("needFilter:" + needFilter)
			} else if (needFilter == 1) {
				//such like operator "&&", a line contains ";" represent "&&"
				if (line.contains(";")) {
					val strs = line.trim.split(";")
					if (line.contains("{") && line.contains("}")) {
						val array = new ArrayBuffer[(String,mutable.HashSet[String])]()
						var key:String = null
						strs.foreach(condition => {
							val localSet = new mutable.HashSet[String]()
							val tempKV = getKeyAndValue(condition.trim)
							val k = tempKV._1
							key = k
							val v = tempKV._2
							if (v.startsWith("{") && v.endsWith("}")) {
								val classes = v.substring(1, v.length - 1).split(",")
								classes.foreach(x => {
									localSet.add(x.trim)
								})
							}
							array.append((key,localSet))
						})
						andClassifyFilterSet.add(array)
					} else if (line.contains("[") && line.contains("]")) {
						val local = new ArrayBuffer[(String, Double, Double)]()
						strs.foreach(condition => {
							val tempKV = getKeyAndValue(condition.trim)
							val k = tempKV._1
							val v = tempKV._2
							if (v.startsWith("[") && v.endsWith("]")) {
								val value = v.substring(1, v.length - 1).split(",")
								val min = if (value(0).equals("#")) Double.MinValue else value(0).toDouble
								val max = if (value(1).equals("#")) Double.MaxValue else value(1).toDouble
								local.append((k, min, max))
							}
						})
						andValueFilterMap.add(local)
					}
					//filter list
				} else {
					val tempKV = getKeyAndValue(line.trim)
					val k = tempKV._1
					val v = tempKV._2
					if (v.startsWith("{") && v.endsWith("}")) {
						val classes = v.substring(1, v.length - 1).split(",")
						classes.foreach(x => {
							classifyFilterSet.add(k + splitC + x.trim)
						})
					} else if (v.startsWith("[") && v.endsWith("]")) {
						val value = v.substring(1, v.length - 1).split(",")
						val min = if (value(0).equals("#")) Double.MinValue else value(0).toDouble
						val max = if (value(1).equals("#")) Double.MaxValue else value(1).toDouble
						valueFilterMap.put(k, (min, max))
					}
				}
				//only keep these list
			} else if (needFilter == 0) {
				val tempKV = getKeyAndValue(line.trim)
				val k = tempKV._1
				val v = tempKV._2
				if (v.startsWith("{") && v.endsWith("}")) {
					val classes = v.substring(1, v.length - 1).split(",")
					val set = new mutable.HashSet[String]()
					classes.foreach(x => {
						set.add(x)
					})
					classifyOnlyKeepMap.put(k, set)
				} else if (v.startsWith("[") && v.endsWith("]")) {
					val value = v.substring(1, v.length - 1).split(",")
					val min = if (value(0).equals("#")) Double.MinValue else value(0).toDouble
					val max = if (value(1).equals("#")) Double.MaxValue else value(1).toDouble
					valueOnlyKeepMap.put(k, (min, max))
				}

			}
		}
		println("----------------Need Filter-----------------")
		println("classifyFilterSet: " + classifyFilterSet.mkString(","))
		println("andClassifyFilterSet: " + andClassifyFilterSet.mkString(","))
		println("valueFilterMap: " + valueFilterMap.mkString(","))
		println("andValueFilterMap: " + andValueFilterMap.mkString(","))
		println()
		println("----------------Only keep-------------------")
		println("classifyOnlyKeepSet: " + classifyOnlyKeepMap.mkString(","))
//		println("andClassifyOnlyKeepSet: " + andClassifyOnlyKeepSet.mkString(","))
		println("valueOnlyKeepMap: " + valueOnlyKeepMap.mkString(","))
//		println("andValueOnlyKeepSet: " + andValueOnlyKeepSet.mkString(","))
	}


	//return true means this line need to be filtered
	def rowClassifyFilter(line: String, filterFeatureSet: mutable.HashSet[String], filterOthersExceptThese: mutable.HashMap[String,mutable.HashSet[String]],andFilterMap: mutable.HashSet[ArrayBuffer[(String,mutable.HashSet[String])]]): Boolean = {
		//filter list
		for (f <- filterFeatureSet) {
			if (line.contains(f))
				return true
		}
		for (onlyKeepFeature <- filterOthersExceptThese) {
			val fClass = getFeatureClass(line,onlyKeepFeature._1)
			val t = if(fClass == null) false else !onlyKeepFeature._2.contains(fClass)
			if(line.contains(onlyKeepFeature._1) && t){
				return true
			}
		}
		for(arr <- andFilterMap){
			var flag = true
			for(oneFeature <- arr){
				val featureName = oneFeature._1
				val fClass = getFeatureClass(line,featureName)
				val t = if(fClass == null) false else oneFeature._2.contains(fClass)
				flag = flag & t
			}
			if(flag) return true
		}
		false
	}
	//get class by feature name between splitC and splitB
	def getFeatureClass(line: String,featureName:String): String ={
		val index = line.indexOf(featureName + splitC)
		if(index == -1)
			return null
		val start = index + featureName.length + splitC.length
		val end = line.indexOf(splitB,start)
		var res: String = null
		try{
			res = line.substring(start,end)
		}catch{
			case ex:Exception =>
				print("GetFeatureValueError:  ")
				println(featureName + ":[" + start + ", " + end + "]")
				ex.printStackTrace()
		}
		res
	}

	//get values between splitB and splitA
	def getFeatureValue(line: String,featureName: String): Double = {
		val valueIndex = line.indexOf(featureName + splitC)
		if(valueIndex == -1)
			return Double.NaN
		val valueIndexStart = line.indexOf(splitB, valueIndex)
		var valueIndexEnd = line.indexOf(splitA,valueIndexStart)
		if(valueIndexEnd == -1)
			valueIndexEnd = line.indexOf(splitT,valueIndexStart)
		var res = Double.NaN
		try {
			res =  line.substring(valueIndexStart, valueIndexEnd).toDouble
		}catch {
			case ex:Exception =>
				print("GetFeatureValueError:  ")
				println(featureName + ":[" + valueIndexStart + ", " + valueIndexEnd + "]")
				ex.printStackTrace()
		}
		res
	}

	//return true means this line need to be filtered
	def rowValueFilter(line: String, filterMap: mutable.HashMap[String, (Double, Double)], filterOthersExceptThese: mutable.HashMap[String, (Double, Double)],andFilterMap: mutable.HashSet[ArrayBuffer[(String, Double, Double)]]): Boolean = {
		for ((k: String, v: (Double, Double)) <- filterMap) {
			val min = v._1
			val max = v._2
			val value = getFeatureValue(line,k)
			if (value <= max && value >= min)
				return true
		}
		for(onlyKeepValue <- filterOthersExceptThese){
			val min = onlyKeepValue._2._1
			val max = onlyKeepValue._2._2
			val value = getFeatureValue(line,onlyKeepValue._1)
			val t = if(value.equals(Double.NaN)) false else value > max || value < min
			if(line.contains(onlyKeepValue._1) && t)
				return true
		}
		for(arr <- andFilterMap){
			var flag = true
			for(oneLimit <- arr){
				val value = getFeatureValue(line,oneLimit._1)
				val min = oneLimit._2
				val max = oneLimit._3
				val t = if(value.equals(Double.NaN)) false else value >= min && value <= max
				flag = flag & t
			}
			if(flag) return true
		}
		false
	}
	//get key and value from string such as "key=value"
	def getKeyAndValue(str: String): (String, String) = {
		if (str.equals(""))
			throw new IllegalArgumentException
		val splits = str.split("=")
		(splits(0).trim, splits(1).trim)
	}
}
