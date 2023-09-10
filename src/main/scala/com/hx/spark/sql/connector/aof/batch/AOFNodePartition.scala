package com.hx.spark.sql.connector.aof.batch

import org.apache.spark.sql.connector.read.InputPartition

import java.util.regex.Pattern

/**
 * @author AC
 */
private[aof] case class AOFNodePartition(node: String, directory: String, pattern: Pattern) extends InputPartition {

  override def preferredLocations(): Array[String] = Array(node)
}
