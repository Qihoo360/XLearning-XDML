package net.qihoo.xitong.xdml.conf

object PSDataType extends Enumeration {
	type PSDataType = Value

	val BYTE = Value(0)
	val SHORT = Value(1)
	val INT = Value(2)
	val LONG = Value(3)
	val FLOAT = Value(4)
	val DOUBLE = Value(5)
	val FLOAT_ARRAY = Value(6)
	val DOUBLE_ARRAY = Value(7)

	def sizeOf(dataType: PSDataType): Int ={
		dataType match {
			case BYTE => 1
			case SHORT => 2
			case INT => 4
			case LONG => 8
			case FLOAT => 4
			case DOUBLE => 8
			case FLOAT_ARRAY => 4
			case DOUBLE_ARRAY => 8
		}
	}

	def isArrayType(tpe:PSDataType):Boolean = {
		tpe == FLOAT_ARRAY || tpe == DOUBLE_ARRAY
	}
}
