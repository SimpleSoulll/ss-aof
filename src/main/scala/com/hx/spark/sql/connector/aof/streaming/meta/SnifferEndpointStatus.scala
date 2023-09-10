package com.hx.spark.sql.connector.aof.streaming.meta

/**
 * executor端文件元数据采集进程的状态
 *
 * @author AC
 */
private[aof] object SnifferEndpointStatus extends Enumeration {

  type Status = Value

  val Unknown, Running, Stop = Value
}
