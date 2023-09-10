package com.hx.spark.sql.connector.aof.streaming.meta

import com.hx.spark.sql.connector.aof.Logging
import com.hx.spark.sql.connector.aof.streaming.zookeeper.message.{DirectoryMetaSnifferMessage, MetaSnifferRequestMessage}
import com.hx.util.JsonSerializer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.retry.RetryNTimes
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.io.{File, FileInputStream}
import java.security.MessageDigest

/**
 * executor启动文件元数据采集线程,响应driver元数据请求及stop请求
 *
 * @author AC
 */
private[aof] class DirectorySnifferEndpoint(endpoint: LaunchSnifferEndpoint) extends PartitionReader[InternalRow] with Logging {

  // 创建zk连接
  private val client = CuratorFrameworkFactory.builder.connectString(endpoint.zookeeperAddress)
    .sessionTimeoutMs(10 * 60 * 1000).retryPolicy(new RetryNTimes(0, 0))
    .build
  client.start()

  // 启动元数据采集线程并通知driver状态
  override def next(): Boolean = {
    setupSniffer().start()
    writeAndWatchStatus()
    false
  }

  // 接受driver元数剧请求的节点
  private val requestNode = s"${endpoint.metaStore}/request/${endpoint.node}"

  // 响应driver元数据请求的节点
  private val responseNode = s"${endpoint.metaStore}/response/${endpoint.node}"

  // 通知driver元数据采集线程状态的节点
  private val statusNode = s"${endpoint.metaStore}/status/${endpoint.node}"

  // 节点监听器
  private val requestNodeCache = new NodeCache(client, requestNode)
  private val statusNodeCache = new NodeCache(client, statusNode)

  private val md5Digest = MessageDigest.getInstance("MD5")

  private def writeAndWatchStatus(): Unit = {
    // 写入状态
    client.setData().forPath(statusNode, SnifferEndpointStatus.Running.toString.getBytes)
    logger.debug(s"Write status ${SnifferEndpointStatus.Running.toString} on $statusNode at ${endpoint.node}")
    statusNodeCache.getListenable.addListener(() => {
      val status = new String(statusNodeCache.getCurrentData.getData)
      logger.debug(s"Receive $status command on $statusNode at ${endpoint.node}, stop zookeeper node caches and client, delete request/response/status nodes")
      // 接收到driver发起的stop命令,关闭相关资源,删除节点
      if (status.equals(SnifferEndpointStatus.Stop.toString)) {
        requestNodeCache.close()
        statusNodeCache.close()
        client.delete().forPath(requestNode)
        client.delete().forPath(responseNode)
        client.delete().forPath(statusNode)
        client.close()
      }
    })
    statusNodeCache.start()
  }

  private def setupSniffer(): Thread = new Thread(() => {
    logger.debug(s"Watch meta request on $requestNode at ${endpoint.node}")
    requestNodeCache.getListenable.addListener(() => { // 监听到节点请求时的回调
      val requestMessage = JsonSerializer.decode[MetaSnifferRequestMessage](new String(requestNodeCache.getCurrentData.getData))
      val session = requestMessage.session
      logger.debug(s"Receive meta request $requestMessage on $requestNode at ${endpoint.node}")
      // 接受到driver的元数据请求
      if (session.nonEmpty) {
        val targetDirectory = new File(endpoint.directory)
        /**
         * 目标目录不为空时,收集相关信息,根据requireMd5([[com.hx.spark.sql.connector.aof.config.AOFReadConfig.enableMd5Check]])计算md5
         */
        if (targetDirectory.exists()) {
          val metas = targetDirectory.listFiles(f => endpoint.pattern.matcher(f.getName).matches()).toList.map { f =>
            if (requestMessage.requireMd5) {
              FileMeta(endpoint.node, f.getName, f.length(), getFileMd5(f, requestMessage.sampleLength))
            } else {
              FileMeta(endpoint.node, f.getName, f.length())
            }
          }
          logger.debug(s"Response meta ${metas.mkString(", ")} on $responseNode at ${endpoint.node}")
          // 将采集的文件元数据信息写入响应节点
          client.setData().forPath(responseNode, JsonSerializer.encode(DirectoryMetaSnifferMessage(session, metas)).getBytes)
        } else {
          // 目标目录为空时,返回空的元数据列表
          logger.warn(s"Target directory ${endpoint.directory} not exists at ${endpoint.node}, response empty meta data on $responseNode")
          client.setData().forPath(responseNode, JsonSerializer.encode(DirectoryMetaSnifferMessage(session, List())).getBytes)
        }
      }
    })
    // 启动元数据请求节点的监听器
    requestNodeCache.start()
    logger.debug(s"Directory sniffer endpoint started at ${endpoint.node}")
  })

  /**
   * 如果文件长度大于采样字节数,则采样数据并计算md5
   *
   * @param f            目标文件
   * @param sampleLength 采样字节数
   * @return md5
   */
  private def getFileMd5(f: File, sampleLength: Int): String = {
    val fi = new FileInputStream(f)
    if (f.length() >= sampleLength) {
      val bytes = new Array[Byte](sampleLength)
      fi.read(bytes)
      fi.close()
      md5Digest.update(bytes)
      md5Digest.digest().map(byte => "%02x".format(byte)).mkString
    } else new String()
  }

  override def get(): InternalRow = null

  override def close(): Unit = {
  }
}
