package com.hx.spark.sql.connector.aof.offset

import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import com.hx.spark.sql.connector.aof.{AOFReadException, Logging}

/**
 * @author AC 
 */
private[aof] abstract class OffsetBackupProvider(configuration: AOFReadConfig) {

  /**
   * 备份Offset
   *
   * @param offset 备份的文件偏移量
   */
  def store(offset: GlobalOffsets): Unit

  /**
   * 获取Offset备份
   *
   * @return 最近一次提交的备份
   */
  def fetch(): Option[GlobalOffsets]

  /** 关闭相关资源 */
  def close(): Unit
}

private[aof] object OffsetBackupProvider extends Logging {

  /**
   * 反射创建自定义的Offset备份实例
   *
   * @param providerClass Offset备份的实现类
   * @param configuration 配置文件
   * @return Offset备份实例
   */
  def getProvider(providerClass: String, configuration: AOFReadConfig): OffsetBackupProvider = {
    try {
      Class.forName(providerClass).getConstructor(classOf[AOFReadConfig]).newInstance(configuration).asInstanceOf[OffsetBackupProvider]
    } catch {
      case ex: ClassNotFoundException => throw new AOFReadException(s"OffsetBackupProvider class $providerClass not found", ex)
      case ex: Exception => throw new AOFReadException(s"Failed to initialize class $providerClass", ex)
    }
  }
}
