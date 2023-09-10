package com.hx.spark.sql.connector.aof.streaming.offset

/**
 * @param offset 文件读取的偏移量
 * @param md5    文件的MD5值
 * @author AC
 */
private[aof] case class FileOffset(offset: Long, md5: String)
