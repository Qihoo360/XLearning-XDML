package net.qihoo.xitong.xdml.task

import java.util.concurrent.Callable

class PullTask[K, V] extends Task with Callable[java.util.Map[Long, Array[Byte]]]{
	var pullSet: java.util.Set[Long] = _
	//pull set must be set before pull parameters from ps
	def setPullSet(pullSet: java.util.Set[Long]): Unit = {
		this.pullSet = pullSet
	}
	//pull task
	override def call(): java.util.Map[Long, Array[Byte]] = {
		val psMap = hazelcastIns.getMap[Long, Array[Byte]](tableName)
		psMap.getAll(pullSet)
	}
}
