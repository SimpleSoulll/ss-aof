package com.hx.spark.sql.connector.aof

import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * @author AC
 */
private[aof] class AOFStreamingTable(_schema: StructType, config: AOFReadConfig) extends Table with SupportsRead {

  override def name(): String = config.path

  override def capabilities(): util.Set[TableCapability] = util.Set.of[TableCapability](TableCapability.MICRO_BATCH_READ)

  override def schema(): StructType = _schema

  private val stream = new AOFMicroBatchStream(config)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {

    override def readSchema(): StructType = _schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = stream
  }

  override def toString: String = s"${config.path} streaming table"

}
