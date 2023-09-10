package com.hx.spark.sql.connector.aof.offset

import com.hx.util.JsonSerializer
import org.apache.spark.sql.connector.read.streaming.Offset

/**
 * 全局Offset,包含各节点所有文件的偏移量
 *
 * @param directory 监听目录
 * @param offsets   各节点的文件偏移量
 * @param ts        时间戳标签
 * @author AC
 */
private[aof] case class GlobalOffsets(directory: String, offsets: Map[String, Map[String, FileOffset]], ts: Long = System.currentTimeMillis()) extends Offset {

  override def json(): String = JsonSerializer.encode(this)
}
