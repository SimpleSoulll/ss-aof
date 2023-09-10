package com.hx.spark.sql.connector.aof.meta

/**
 * 文件元数据
 *
 * @param node 文件所在节点
 * @param name 文件名
 * @param size 文件大小
 * @param md5  文件的MD5
 * @author AC
 */
private[aof] case class FileMeta(node: String, name: String, size: Long, md5: String = "") extends java.io.Serializable
