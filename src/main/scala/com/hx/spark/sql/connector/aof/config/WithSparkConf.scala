package com.hx.spark.sql.connector.aof.config

import com.hx.spark.sql.connector.aof.Logging
import org.apache.spark.sql.SparkSession

/**
 * 整合SparkConf中的参数
 *
 * @author AC 
 */
private[aof] class WithSparkConf(properties: Map[String, String]) extends Logging {

  private val prefix = "spark.aof."

  // SparkConf中参数的优先级更高
  protected val conf: Map[String, String] = properties ++ SparkSession.active.sparkContext.getConf.getAllWithPrefix(prefix)

  def getString(key: String, default: String): String = conf.getOrElse(key, default)

  def getInt(key: String, default: Int): Int = conf.get(key).map(_.toInt).getOrElse(default)

  def getBoolean(key: String, default: Boolean): Boolean = conf.get(key).map(_.toBoolean).getOrElse(default)

  def get(key: String): Option[String] = conf.get(key)
}
