package com.hx.spark.sql.connector.aof

import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import com.hx.spark.sql.connector.aof.streaming.meta.{FileMeta, LaunchSnifferEndpoint, SnifferEndpointStatus}
import com.hx.spark.sql.connector.aof.streaming.offset.{FileOffset, GlobalOffsets, OffsetBackupProvider, StartingOffsets}
import com.hx.spark.sql.connector.aof.streaming.partition.{AOFPartitionReaderFactory, AOFSingleFilePartition}
import com.hx.spark.sql.connector.aof.streaming.zookeeper.MetaCoordinator
import com.hx.util.JsonSerializer
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationLong

/**
 * @author AC
 */
private[aof] class AOFMicroBatchStream(config: AOFReadConfig) extends MicroBatchStream with SupportsAdmissionControl with Logging {

  // driver与executor通信获取各节点文件元数据信息
  private lazy val metaCoordinator = new MetaCoordinator(config.zookeeperAddress, config.directory, config.metaStore, config.enableMd5Check, config.md5SampleLength)

  // 追踪记录各节点元数据服务启动状态
  private val endpointStatusTracer = new mutable.HashMap[String, SnifferEndpointStatus.Value]()

  // 实例化Offset备份实现类
  private lazy val offsetStore = OffsetBackupProvider.getProvider(config.offsetBackupProviderClass, config)

  // 启动时读取zookeeper中的offset备份
  private var finalOffset: GlobalOffsets = offsetStore.fetch().getOrElse(GlobalOffsets(config.directory, Map()))

  final override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def reportLatestOffset(): Offset = finalOffset

  private def getLimitedEndOffset(node: String, nodeOffsets: Map[String, FileOffset], name: String, size: Long, maxBytes: Long): Long = {
    nodeOffsets.get(name) match {
      case Some(FileOffset(offset, _)) =>
        // 起始偏移量就已经超过了文件大小,意味着文件已被替换/更新/删除,弃用offset,直接从文件起始位置读取
        if (offset > size) {
          logger.warn(s"Start offset of file $name at $node exceeds file length, which violates the append-only convention, continue at beginning of target file")
          0L
        } else {
          // 按maxBytes计划读取的终止偏移量
          val nextOffset = offset + maxBytes
          // 计划的偏移量超过了文件大小,则读至文件结尾
          if (nextOffset > size) size else nextOffset
        }
      case None =>
        // Offset未记录任何文件的偏移量(本地及Zookeeper均无Offset记录),说明应用第一次启动,根据`starting.offsets`为文件设置初始偏移量
        if (nodeOffsets.isEmpty) {
          config.startingOffsets match {
            case StartingOffsets.earliest => if (maxBytes > size) size else maxBytes
            case StartingOffsets.latest => size
          }
          // Offset仅未记录该文件的偏移,则说明该文件刚创建,从起始位置读文件
        } else 0L
    }
  }

  override def getDefaultReadLimit: ReadLimit = ReadLimit.maxRows(config.maxBytes)

  /**
   * 检查driver本地的Offset文件
   *
   * @param localStartOffset 本地Offset
   */
  private def localStartOffsetCheck(localStartOffset: GlobalOffsets): GlobalOffsets = {
    // 本地的Offset文件落后于zookeeper中备份的Offset记录,说明driver在其他节点启动过(cluster模式),启用本地Offset,启用zookeeper最新的Offset记录
    if (localStartOffset.ts < finalOffset.ts) {
      logger.debug("Local offsets has fallen behind, use offsets in zookeeper instead")
      // 此时finalOffset由zookeeper备份生成
      finalOffset.copy()
    } else localStartOffset
  }

  /**
   * 获取最新的Offset,将被作为planPartitions的endOffset
   *
   * @param start 起始偏移量
   * @param limit 读取的字节数限制
   * @return endOffset
   */
  override def latestOffset(start: Offset, limit: ReadLimit): Offset = {
    val localStartOffset = start.asInstanceOf[GlobalOffsets]
    val startOffset = localStartOffsetCheck(localStartOffset)
    val maxBytes = limit.asInstanceOf[ReadMaxRows].maxRows()
    // 应用分配的所有executor
    val offsets = ApplicationInfo.executors.map { executor =>
      endpointStatusTracer.getOrElse(executor, SnifferEndpointStatus.Unknown) match {
        // executor上元数据服务已启动
        case SnifferEndpointStatus.Running =>
          val nodeOffsets = startOffset.offsets.getOrElse(executor, Map())
          // 向executor请求文件的元数据信息
          executor -> Await.result(metaCoordinator.requestForMeta(executor).map(_.map {
            case FileMeta(node, name, size, md5) => name -> {
              val currentMd5 = nodeOffsets.get(name).map(_.md5).getOrElse("")
              // 如果文件md5发生变化,意味着文件被替换/删除/更新,此时弃用offset,从文件起始处重新读取文件
              if (currentMd5.nonEmpty && !currentMd5.equals(md5)) {
                logger.warn(s"MD5 of the first ${config.md5SampleLength} bytes in file $name at $node changed from $currentMd5 to $md5, which violates the append-only convention, continue at beginning of target file")
                FileOffset(0, md5)
              } else {
                // 正常处理,计算文件读取的终止偏移量
                FileOffset(getLimitedEndOffset(node, nodeOffsets, name, size, maxBytes), md5)
              }
            }
            // 获取元数据出错直接抛出异常
          }.toMap).recover { case e: Exception => throw e }, config.metaAwaitTimeout.millis)
        // executor节点的元数据服务尚未启动,则本Batch放弃读该节点文件
        case _ => executor -> startOffset.offsets.getOrElse(executor, Map())
      }
    }.toMap
    logger.debug(s"Use offset $offsets as end offset")
    // 通过finalOffset记录endOffset,在Batch完成执行commit时会备份finalOffset至zookeeper
    finalOffset = GlobalOffsets(config.directory, offsets)
    finalOffset.copy()
  }

  /**
   * 当本地不存在offset文件时初始化startOffset
   *
   * @return
   */
  override def initialOffset(): Offset = {
    // 从zookeeper获取备份的Offset,如果没有则使用空的Offset
    offsetStore.fetch() match {
      case Some(zkOffset) => logger.info(s"Find backup offsets $zkOffset in zookeeper, use as initial offsets")
        zkOffset
      case None => logger.info("No available offset in zookeeper, use empty GlobalOffsets as initial offset")
        GlobalOffsets(config.directory, Map())
    }
  }

  /**
   * 监听目录存在未记录偏移量的文件时,获取其初始偏移量
   *
   * @param startOffsets 已存在的文件偏移
   * @param name         文件名
   * @param node         文件所在节点
   * @param end          文件的endOffset
   * @return 文件的起始位置
   */
  private def getFileStartingOffset(startOffsets: GlobalOffsets, name: String, node: String, end: Long): Long = {
    // 没有任何文件的起始偏移记录,则任务初次启动,根据starting.offset指定起始偏移
    if (startOffsets.offsets.isEmpty) {
      config.startingOffsets match {
        case StartingOffsets.earliest => logger.info(s"Read file $name at $node from offset 0"); 0L
        case StartingOffsets.latest => logger.info(s"Read file $name at $node from offset $end"); end
      }
      // 仅该文件没有起始偏移,则改文件被认为是新创建的文件,起始偏移设置为文件开头
    } else {
      logger.info(s"Read newly created file $name at $node from offset 0")
      0L
    }
  }

  /**
   * 为不同节点的文件创建分区
   *
   * @param start 起始偏移量
   * @param end   终止偏移量
   * @return 节点的分区信息
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffsets = start.asInstanceOf[GlobalOffsets]
    val endOffsets = end.asInstanceOf[GlobalOffsets]
    endOffsets.offsets.toArray.flatMap {
      // 如果节点的元数据服务已启动,则节点能够读取数据,为其创建分区
      case (node, offsets) if endpointStatusTracer.getOrElse(node, SnifferEndpointStatus.Unknown).equals(SnifferEndpointStatus.Running) =>
        logger.debug(s"Planning file partitions from start offset ${startOffsets.offsets.getOrElse(node, Map())} to end offset $offsets for $node")
        offsets.toArray.map {
          case (name, end) =>
            // 读取文件的起始偏移
            val start = startOffsets.offsets.get(node).flatMap(_.get(name).map(_.offset))
              // 如果文件仅有终止偏移而没有起始偏移
              .getOrElse(getFileStartingOffset(startOffsets, name, node, end.offset))
            val rectifiedStart = if (start > end.offset) end.offset else start
            AOFSingleFilePartition(node, s"${config.directory}/$name", rectifiedStart, end.offset)
        }.asInstanceOf[Array[InputPartition]]
      // 如果节点的元数据服务未启动,则创建由于启动元数据服务的分区
      case (node, _) =>
        // 记录节点的元数据服务状态为Unknown
        endpointStatusTracer.put(node, SnifferEndpointStatus.Unknown)
        // 追踪节点元数据服务状态
        metaCoordinator.trackEndpointStatus(node, endpointStatusTracer.update(_, SnifferEndpointStatus.Running))
        logger.debug(s"Planning partitions for sniffer endpoint at $node, requires to check md5 in next batch")
        Array(LaunchSnifferEndpoint(node, config.directory, config.pattern, config.zookeeperAddress, config.metaStore)).asInstanceOf[Array[InputPartition]]
    }
  }

  /** @inheritdoc */
  override def createReaderFactory(): PartitionReaderFactory = AOFPartitionReaderFactory

  /** @inheritdoc */
  override def deserializeOffset(json: String): Offset = {
    logger.debug(s"Deserialize offset $json")
    JsonSerializer.decode[GlobalOffsets](json)
  }

  /** @inheritdoc */
  override def commit(end: Offset): Unit = {
    // 写入zookeeper备份
    offsetStore.store(finalOffset)
    logger.debug(s"Committed offset ${end.asInstanceOf[GlobalOffsets]} stored to zookeeper as backup")
  }

  /** @inheritdoc */
  override def stop(): Unit = {
    offsetStore.store(finalOffset)
    metaCoordinator.stop()
    offsetStore.close()
  }

  /** @inheritdoc */
  override def toString: String = s"streaming source with file ${config.path}"
}
