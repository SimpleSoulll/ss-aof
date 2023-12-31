package com.hx.spark.sql.connector.aof.zookeeper.message

import com.hx.spark.sql.connector.aof.meta.FileMeta

/**
 * executor响应driver元数据请求的消息
 *
 * @author AC
 */
private[aof] case class DirectoryMetaSnifferMessage(session: String, meta: List[FileMeta])
