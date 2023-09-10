package com.hx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger

/**
 * Test
 *
 * @author AC
 */
object Main extends App {

  private val spark = SparkSession.builder().appName("aof-spec").getOrCreate()

  val options = Map("checkpointLocation" -> args(1))

  val df = spark.readStream.format("com.hx.spark.sql.connector.aof").options(options).option("path", args(0)).load()

  val query = df.writeStream.format("console").options(options).option("truncate", value = false).option("numRows", 100).trigger(new ProcessingTimeTrigger(3000)).start()

  query.awaitTermination()
}
