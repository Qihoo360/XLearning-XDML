package net.qihoo.xitong.xdml.updater

trait Updater[K, V] extends Serializable{
	//update function
	def update(originalWeightMap: java.util.Map[K, V], gradientMap: java.util.Map[K, V]):java.util.Map[K, V]
}
