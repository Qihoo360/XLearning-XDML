package net.qihoo.xitong.xdml.task

import java.io.Serializable

import com.hazelcast.core.{HazelcastInstance, HazelcastInstanceAware}

abstract class Task extends Serializable with HazelcastInstanceAware {
	@transient
	var hazelcastIns: HazelcastInstance = _

	//pull and push task table name
	protected var tableName:String = _

	def setTableName(name: String): Unit = {
		this.tableName = name
	}
	//set hz server instance
	override def setHazelcastInstance(hz: HazelcastInstance): Unit = {
		this.hazelcastIns = hz
	}

}
