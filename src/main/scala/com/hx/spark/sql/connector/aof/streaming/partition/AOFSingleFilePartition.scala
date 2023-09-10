package com.hx.spark.sql.connector.aof.streaming.partition

import com.hx.spark.sql.connector.aof.streaming.meta.DirectorySnifferEndpoint
import org.apache.spark.sql.connector.read.InputPartition

/**
 * @author AC
 */
private[aof] case class AOFSingleFilePartition(node: String, path: String, start: Long, end: Long) extends InputPartition {

  /**
   * 分区将被优先发送给preferredLocations,但是Spark不保证一定能够发送至node指定的节点(即保证NODE_LOCAL本地行)。
   * 如果数据没有发送至node指定节点(数据本地性降级),这将导致executor端接受的文件偏移量与节点的文件不匹配,造成数据丢失及重复消费。
   * [[DirectorySnifferEndpoint]]及[[AOFPartitionReader]]实现数据本地行性验逻辑,以避免上述问题。
   * `spark.locality.wait.node`是driver成功将InputPartition发送至指定executor的最大超时时间,因此在网络环境恶劣时可以增大该参数，避免数据本地性降级。
   */
  override def preferredLocations(): Array[String] = Array(node)
}