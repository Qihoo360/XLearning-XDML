import java.io.Serializable

/**
  * @author wangxingda
  *			2018/08/16
  */

trait DataFilter extends Serializable{
	val splitC: String = "\03"
	val splitB: String = "\02"
	val splitA: String = "\01"
	val splitT: String = "\t"
	//read config file, include row and col filter
	def readFilterConfig(filePath: String)
}
