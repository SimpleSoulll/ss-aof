package com.hx.spark.sql.connector.aof.streaming.partition

import com.hx.spark.sql.connector.aof.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.unsafe.types.UTF8String

import java.io.RandomAccessFile

/**
 * executor根据分区信息读取本地文件
 *
 * @author AC
 */
private[aof] class AOFPartitionReader(inputPartition: InputPartition) extends PartitionReader[InternalRow] with Logging {

  private val partition = inputPartition.asInstanceOf[AOFSingleFilePartition]

  private val aof = new RandomAccessFile(partition.path, "r")

  private var lines: Iterator[UTF8String] = _

  override def next(): Boolean = {
    partition match {
      // 读取偏移量为[start, end]的数据
      case AOFSingleFilePartition(node, path, start, end) =>
        val hasAvailable = end > start
        if (hasAvailable && lines == null) {
          val bytes = new Array[Byte](end.intValue() - start.intValue())
          logger.debug(s"Target file $path has ${bytes.length} bytes available at $node")
          aof.seek(start)
          aof.read(bytes)
          // 按行分割文本
          lines = UTF8String.fromBytes(bytes).split(UTF8String.fromString("\n"), Int.MaxValue).dropRight(1).toIterator
        }
        hasAvailable && lines.hasNext
    }
  }

  override def get(): InternalRow = new GenericInternalRow(Array[Any](lines.next()))

  override def close(): Unit = aof.close()
}
