package net.qihoo.xitong.xdml.conf

object JobType extends Enumeration {
	type PSJobType = Value

	val TRAIN = Value(0)
	val PREDICT = Value(1)
	val INCREMENT_TRAIN = Value(2)

}
