package com.hx.spark.sql.connector.aof

/**
 * @author AC
 */
private[aof] class AOFReadException(message: String, cause: Exception = null) extends Exception(message, cause)
