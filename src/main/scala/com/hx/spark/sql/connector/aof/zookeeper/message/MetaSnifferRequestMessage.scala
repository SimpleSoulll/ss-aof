package com.hx.spark.sql.connector.aof.zookeeper.message

/**
 * 向executor请求文件元信息时的消息
 *
 * @author AC 
 */
private[aof] case class MetaSnifferRequestMessage(session: String, requireMd5: Boolean, sampleLength: Int)
