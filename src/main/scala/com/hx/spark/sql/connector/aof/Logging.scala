package com.hx.spark.sql.connector.aof

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author AC 
 */
trait Logging {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
