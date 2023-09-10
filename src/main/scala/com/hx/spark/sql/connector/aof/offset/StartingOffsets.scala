package com.hx.spark.sql.connector.aof.offset

/**
 * @author AC 
 */
private[aof] object StartingOffsets extends Enumeration {

  type Offset = Value
  val latest, earliest = Value
}
