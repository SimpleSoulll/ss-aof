package com.hx.spark.sql.connector.aof

import com.hx.spark.sql.connector.aof.batch.{AOFNodePartition, AOFNodeReader}
import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, SupportsPushDownFilters}
import org.apache.spark.sql.sources.Filter

/**
 * @author AC
 */
private[aof] class AOFBatchReader(config: AOFReadConfig) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    ApplicationInfo.executors.map { executor =>
      new AOFNodePartition(executor, config.directory, config.pattern)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    (partition: InputPartition) => new AOFNodeReader(partition)
  }


}
