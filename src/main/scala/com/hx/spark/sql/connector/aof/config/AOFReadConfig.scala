package com.hx.spark.sql.connector.aof.config

import com.hx.spark.sql.connector.aof.offset.StartingOffsets
import com.hx.spark.sql.connector.aof.zookeeper.OffsetStore
import com.hx.spark.sql.connector.aof.{AOFReadException, Logging}

import java.util.regex.Pattern

/**
 * 参数及配置
 *
 * @author AC
 */
private[aof] class AOFReadConfig(properties: Map[String, String]) extends WithSparkConf(properties) with Logging {

  /** 目标文件绝对路径, 支持正则 */
  val path: String = getString("path", "")

  private val pathNodes = path.split("/")

  /** 目标目录 */
  val directory: String = pathNodes.dropRight(1).mkString("/")

  private val name: String = pathNodes.last

  /** zookeeper地址 */
  val zookeeperAddress: String = getString("zookeeper.address", "localhost:2181")

  /** 预编译文件匹配正则 */
  val pattern: Pattern = Pattern.compile(name)

  /** 初次读取文件时的初始偏移量 */
  val startingOffsets: StartingOffsets.Offset = get("starting.offsets").map(StartingOffsets.withName).getOrElse(StartingOffsets.earliest)

  /** 每次读取的最大字节数, 防止startingOffsets=earliest时，初次读取过量数据而OOM */
  val maxBytes: Long = getInt("bytes.max", Int.MaxValue)

  /** 文件的元数据同步节点(zookeeper) */
  val metaStore: String = getString("meta.store", s"/higgs/aof$directory/meta")

  /** driver等待executor同步元数据的超时时长 */
  val metaAwaitTimeout: Long = getInt("meta.await.timeout.ms", 3000)

  /** 开启MD5校验,通过收集文件前dm5.sample.length字节的数据计算MD5,通过该MD5判断文件是否被删除(违反append-only约束) */
  val enableMd5Check: Boolean = getBoolean("md5.check.enabled", default = true)

  /** 计算MD5使用的样本长度 */
  val md5SampleLength: Int = getInt("md5.sample.length", 1024)

  val offsetBackupProviderClass: String = getString("offset.backup.provider", classOf[OffsetStore].getCanonicalName)

  if (logger.isDebugEnabled) {
    logger.debug(s"Integrated configuration ${conf.mkString(", ")}")
    val configInfo = getClass.getDeclaredFields.map { param =>
      s"${param.getName} = ${param.get(this)}"
    }.mkString(", ")
    logger.debug(s"Resolved configurations $configInfo")
  }

  def validate(): Unit = {
    if (path.isEmpty) throw new AOFReadException("option path not set")
  }
}
