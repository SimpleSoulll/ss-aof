package com.hx.spark.sql.connector.aof.streaming.zookeeper

import com.hx.spark.sql.connector.aof.streaming.meta.{FileMeta, SnifferEndpointStatus}
import com.hx.spark.sql.connector.aof.streaming.zookeeper.message.{DirectoryMetaSnifferMessage, MetaSnifferRequestMessage}
import com.hx.spark.sql.connector.aof.{AOFReadException, Logging}
import com.hx.util.JsonSerializer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.retry.RetryNTimes

import java.util.UUID
import scala.concurrent.{Future, Promise}

/**
 * driver与executor通信,追踪/请求executor监听目录中文件的元数据
 *
 * @param address      zookeeper地址
 * @param directory    监听目录
 * @param metaStore    元数据请求通信节点
 * @param requireMd5   是否计算文件MD5.
 *                     [[com.hx.spark.sql.connector.aof.config.AOFReadConfig.enableMd5Check]]
 * @param sampleLength MD5采样字节数.
 *                     [[com.hx.spark.sql.connector.aof.config.AOFReadConfig.md5SampleLength]]
 * @author AC
 *
 */
private[aof] class MetaCoordinator(address: String, directory: String, metaStore: String, requireMd5: Boolean, sampleLength: Int) extends Logging {

  import ZkHelper.Operations

  private val client = CuratorFrameworkFactory.builder()
    .connectString(address)
    .sessionTimeoutMs(10 * 60 * 1000)
    .retryPolicy(new RetryNTimes(0, 0))
    .build()
  client.start()

  /**
   * 向executor请求文件的元数据
   *
   * @param node executor的地址
   * @return executor响应的元数据信息
   */
  def requestForMeta(node: String): Future[List[FileMeta]] = {
    val requestNode = s"$metaStore/request/$node"
    val responseNode = s"$metaStore/response/$node"
    client.createNodeIfNotExists(requestNode)
    client.createNodeIfNotExists(responseNode)
    val session = UUID.randomUUID().toString
    val requestMessage = MetaSnifferRequestMessage(session, requireMd5, sampleLength)
    // executor响应节点监听器
    val responseNodeCache = new NodeCache(client, responseNode)
    val metaPromise = Promise[List[FileMeta]]()
    responseNodeCache.getListenable.addListener(() => { // 回调
      try {
        val content = new String(responseNodeCache.getCurrentData.getData)
        if (content.nonEmpty) {
          logger.debug(s"Get meta response content: $content")
          val message = JsonSerializer.decode[DirectoryMetaSnifferMessage](content)
          // response的session与request请求时的session须一致
          if (message.session.equals(session)) {
            metaPromise.success(message.meta)
          } else {
            metaPromise.failure(new AOFReadException(s"failed to get file metadata with session id $session on $responseNode at $node"))
          }
          // 关闭监听器并清理response节点数据
          responseNodeCache.close()
          client.setData().forPath(responseNode, Array())
        }
      } catch {
        case ex: Exception => metaPromise.failure(new AOFReadException(s"failed to deserialize metadata message from $node", ex))
      }
    })
    // 启动response监听器
    responseNodeCache.start(true)
    logger.debug(s"Request meta with session id $session on $requestNode at $node")
    // 写请求节点发起元数据请求,executor监听该节点
    client.setData().forPath(requestNode, JsonSerializer.encode(requestMessage).getBytes)
    metaPromise.future
  }

  /**
   * 追踪节点元数据服务状态
   *
   * @param node 节点地址
   * @param tracker 状态为Running时执行的操作
   */
  def trackEndpointStatus(node: String, tracker: String => Unit): Unit = {
    val statusNode = s"$metaStore/status/$node"
    client.createNodeIfNotExists(statusNode)
    // status节点监听器,executor在启动元数据服务后会将状态写入该节点
    val statusNodeCache = new NodeCache(client, statusNode)
    statusNodeCache.getListenable.addListener(() => { // 回调
      val status = new String(statusNodeCache.getCurrentData.getData)
      // 如果executor写入的状态为Running则执行tracker并关闭监听器
      if (status.equals(SnifferEndpointStatus.Running.toString)) {
        logger.debug(s"Receive sniffer endpoint status $status at $node")
        tracker.apply(node)
        statusNodeCache.close()
      }
    })
    // 启动监听器
    statusNodeCache.start()
  }

  def stopSnifferEndpoint(node: String): Unit = {
    client.setData().forPath(s"$metaStore/status/$node", SnifferEndpointStatus.Stop.toString.getBytes)
  }

  def stop(): Unit = client.close()
}