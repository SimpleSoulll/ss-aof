package com.hx.spark.sql.connector.aof.meta

import org.apache.spark.sql.connector.read.InputPartition

import java.util.regex.Pattern

/**
 * 以InputPartition形式封装元数据采集相关信息,并在executor通过[[DirectorySnifferEndpoint]]启动元数据采集线程
 *
 * @param node             目标节点
 * @param directory        采集目录
 * @param pattern          目标文件的正则
 * @param zookeeperAddress zookeeper地址
 * @param metaStore        与driver的元数据通信节点
 * @author AC
 */
private[aof] case class LaunchSnifferEndpoint(node: String, directory: String, pattern: Pattern, zookeeperAddress: String, metaStore: String) extends InputPartition {

  /**
   * 分区将被优先发送给preferredLocations,但是Spark不保证一定能够发送至node指定的节点(即保证NODE_LOCAL本地行)。
   * 如果数据没有发送至node指定节点(数据本地性降级),这将导致driver端接受的文件元数据紊乱,即drier接受到A节点的元数据实际上出自B节点
   * [[DirectorySnifferEndpoint]]及[[com.hx.spark.sql.connector.aof.partition.AOFPartitionReader]]实现数据本地行校验逻辑,以避免上述问题。
   * `spark.locality.wait.node`是driver成功将InputPartition发送至指定executor的最大超时时间,因此在网络环境恶劣时可以增大该参数，避免数据本地性降级。
   */
  override def preferredLocations(): Array[String] = Array(node)
}
