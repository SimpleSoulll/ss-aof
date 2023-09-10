package com.hx.spark.sql.connector.aof

import org.apache.spark.sql.SparkSession

/**
 * @author AC
 */
private[aof] object ApplicationInfo {

  private lazy val sc = SparkSession.active.sparkContext

  // driver节点地址
  private lazy val driver: String = sc.getConf.get("spark.driver.host")

  def executors: Set[String] = sc.statusTracker.getExecutorInfos.map(_.host()).toSet.filterNot(_.equals(driver))
}
