package net.qihoo.xitong.xdml.model.data

import org.apache.log4j.{Level, Logger}

object LogHandler extends Serializable {

  @transient lazy val logger = Logger.getLogger("XDML")

  def avoidLog(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

}