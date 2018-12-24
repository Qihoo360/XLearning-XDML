package net.qihoo.xitong.xdml.mapstore

import java.io.Serializable
import java.util

import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Schema}

import scala.collection.JavaConverters._

class KuduTableOp(master: String) extends Serializable {
  private val client = new KuduClient.KuduClientBuilder(master).build()
  private var step = 1
  private var partitionNum = 1
  private var replicasNum = 3

  def setStep(step: Int): this.type = {
    this.step = step
    this
  }

  def setPartitionNum(partitionNum: Int): this.type = {
    this.partitionNum = partitionNum
    this
  }

  def setReplicasNum(replicasNum: Int): this.type = {
    this.replicasNum = replicasNum
    this
  }

  def getReplicasNum(): Int = {
    this.replicasNum
  }

  def getPartitionNum(): Int = {
    this.partitionNum
  }

  def getClient(): KuduClient = {
    this.client
  }

  def getStep(): Int = {
    this.step
  }

  def isTableExist(tableName: String): Boolean = {
    client.tableExists(tableName)
  }

  def createTable(tableName: String, columnList: util.List[ColumnSchema], hashPartition: Boolean = true): Unit = {
    if (hashPartition)
      createHashTable(tableName, this.partitionNum, columnList)
    else
      createRangeTable(tableName, this.partitionNum, this.step, columnList)
  }

  def createHashTable(tableName: String, partitionNum: Int, columnList: util.List[ColumnSchema], partitionRowKey: String = "id"): Unit = {
    println(s"Create the table ${tableName} with hashPartition.")
    val schema = new Schema(columnList)
    if (client.tableExists(tableName)) {
      client.deleteTable(tableName)
    }
    if (!client.tableExists(tableName)) {
      val kuduTableOptions = new CreateTableOptions().setNumReplicas(replicasNum).addHashPartitions(List(partitionRowKey).asJava, partitionNum)
      client.createTable(tableName, schema, kuduTableOptions)
    }
  }

  def createRangeTable(tableName: String, partitionNum: Int, step: Int, columnList: util.List[ColumnSchema], partitionRowKey: String = "id"): Unit = {
    println(s"Create the table ${tableName} with rangePartition.")
    val schema = new Schema(columnList)
    if (client.tableExists(tableName)) {
      client.deleteTable(tableName)
    }
    if (!client.tableExists(tableName)) {
      val kuduTableOptions = new CreateTableOptions().setNumReplicas(replicasNum).setRangePartitionColumns(List(partitionRowKey).asJava)
      for (i <- 0 until partitionNum) {
        val upper = schema.newPartialRow()
        upper.addInt(partitionRowKey, (i + 1) * step)
        val lower = schema.newPartialRow()
        lower.addInt(partitionRowKey, i * step)
        kuduTableOptions.addRangePartition(lower, upper)
      }
      client.createTable(tableName, schema, kuduTableOptions)
    }
  }

  def openTable(tableName: String): KuduTable = {
    this.client.openTable(tableName)
  }

  def getClientSession(): KuduSession = {
    val session = client.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_SYNC)
    session.setIgnoreAllDuplicateRows(false)
    session.setMutationBufferSpace(5000)
    session
  }

  def getTableScanner(tableName: String, pid: Int, columnList: util.List[String], partitionRowKey: String = "id"): KuduScanner = {
    val kuduTable = openTable(tableName)
    val scanBuilder = client.newScannerBuilder(kuduTable)
    val schema = kuduTable.getSchema
    val partialRowLower = schema.newPartialRow()
    partialRowLower.addInt(partitionRowKey, pid * step)
    println(s"lower ${partitionRowKey} " + pid * step)
    val partialRowUpper = schema.newPartialRow()
    partialRowUpper.addInt(partitionRowKey, (pid + 1) * step)
    println(s"upper ${partitionRowKey} " + (pid + 1) * step)
    scanBuilder.setProjectedColumnNames(columnList)
      .lowerBound(partialRowLower)
      .exclusiveUpperBound(partialRowUpper)
      .build()
  }

  def getTableRandomScanner[T](tableName: String, rowList: util.List[T], columnList: util.List[String], columnString: String = "id"): KuduScanner = {
    val kuduTable = openTable(tableName)
    val scanBuilder = client.newScannerBuilder(kuduTable)
    val schema = kuduTable.getSchema
    val predicateList = KuduPredicate.newInListPredicate(schema.getColumn(columnString), rowList)
    scanBuilder.setProjectedColumnNames(columnList)
      .addPredicate(predicateList)
      .build()
  }

  def cleanTable(tableName: String, pid: Int): Unit = {
    cleanTable(tableName, pid, this.step)
  }

  def cleanTable(tableName: String, pid: Int, step: Int, partitionRowKey: String = "id"): Unit = {
    val kuduTable = client.openTable(tableName)
    val schema = kuduTable.getSchema
    val partialRowLower = schema.newPartialRow()
    partialRowLower.addInt(partitionRowKey, pid * step)
    val partialRowUpper = schema.newPartialRow()
    partialRowUpper.addInt(partitionRowKey, (pid + 1) * step)
    val alterTableDeleteOptions = new AlterTableOptions().dropRangePartition(partialRowLower, partialRowUpper)
    val alterTableAddOptions = new AlterTableOptions().addRangePartition(partialRowLower, partialRowUpper)
    client.alterTable(kuduTable.getName, alterTableDeleteOptions)
    client.alterTable(kuduTable.getName, alterTableAddOptions)
  }

  def deleteTable(tableName: String): Unit = {
    if (client.tableExists(tableName)) {
      println(s"Delete the table ${tableName}.")
      client.deleteTable(tableName)
    } else {
      println(s"Table ${tableName} is not exists.Please Check.")
    }
  }

  def closeClient(): Unit = {
    client.close()
  }
}
