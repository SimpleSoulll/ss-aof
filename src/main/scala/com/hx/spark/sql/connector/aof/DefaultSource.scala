package com.hx.spark.sql.connector.aof

import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * @author AC
 */
private[aof] class DefaultSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "aof"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType(Array(StructField("value", DataTypes.StringType, nullable = false)))

  override def getTable(_schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {

    lazy val config = new AOFReadConfig(properties.asScala.toMap)

    config.validate()

    new AOFTable(_schema, config)
  }

  override def supportsExternalMetadata(): Boolean = false

  override def toString: String = "source for append-only files"
}
