package com.hx.spark.sql.connector.aof.batch

import com.hx.spark.sql.connector.aof.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @author AC
 */
private[aof] class AOFNodeReader(inputPartition: InputPartition) extends PartitionReader[InternalRow] with Logging {

  private var lines: Iterator[(String, UTF8String)] = _

  override def next(): Boolean = {
    if (lines == null) {
      val contents = new ListBuffer[(String, UTF8String)]
      inputPartition match {
        case AOFNodePartition(_, directory, pattern) =>
          val targetDirectory = new File(directory)
          if (targetDirectory.exists()) {
            targetDirectory.listFiles(f => pattern.matcher(f.getName).matches()).foreach { f =>
              val source = Source.fromFile(f)
              val name = f.getName
              source.getLines().map(UTF8String.fromString).foreach(utf8String => contents.append(name -> utf8String))
              source.close()
            }
          }
      }
      lines = contents.toIterator
    }
    lines != null && lines.hasNext
  }

  override def get(): InternalRow = {
    lines.next() match {
      case (name, line) => new GenericInternalRow(Array[Any](name, line))
    }
  }

  override def close(): Unit = {

  }
}
