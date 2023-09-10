package com.hx.spark.sql.connector.aof.partition

import com.hx.spark.sql.connector.aof.Logging
import com.hx.spark.sql.connector.aof.meta.{DirectorySnifferEndpoint, LaunchSnifferEndpoint}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * @author AC
 */
private[aof] object AOFPartitionReaderFactory extends PartitionReaderFactory with Logging {

  /**
   * 根据分区的类型创建对应的Reader
   *
   * @param partition 分区信息
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = partition match {
    case p: AOFSingleFilePartition => new AOFPartitionReader(p)
    case p: LaunchSnifferEndpoint =>
      logger.debug(s"Create directory sniffer endpoint partition for ${p.node}")
      new DirectorySnifferEndpoint(p)
  }
}
