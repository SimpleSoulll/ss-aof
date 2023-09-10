package com.hx.spark.sql.connector.aof.zookeeper

import com.hx.spark.sql.connector.aof.config.AOFReadConfig
import com.hx.spark.sql.connector.aof.offset.{GlobalOffsets, OffsetBackupProvider}
import com.hx.util.JsonSerializer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes

/**
 * 使用zookeeper中备份Offset
 *
 * @author AC 
 */
private[aof] class OffsetStore(configuration: AOFReadConfig) extends OffsetBackupProvider(configuration) {

  private val client = CuratorFrameworkFactory.builder()
    .connectString(configuration.zookeeperAddress)
    .sessionTimeoutMs(10 * 60 * 1000)
    .connectionTimeoutMs(60 * 1000)
    .retryPolicy(new RetryNTimes(0, 0))
    .build()
  client.start()

  private val storeNode = s"/higgs/aof${configuration.directory}/offset"

  if (client.checkExists().forPath(storeNode) == null) {
    client.create().creatingParentsIfNeeded().forPath(storeNode, Array())
  }

  /**
   * @inheritdoc
   */
  override def store(offset: GlobalOffsets): Unit = {
    storeNode.synchronized {
      client.setData().forPath(storeNode, offset.json().getBytes)
    }
  }

  /**
   * @inheritdoc
   */
  override def fetch(): Option[GlobalOffsets] = {
    storeNode.synchronized {
      val json = new String(client.getData.forPath(storeNode))
      if (json.isEmpty) None else Some(JsonSerializer.decode[GlobalOffsets](json))
    }
  }

  /**
   * @inheritdoc
   */
  override def close(): Unit = client.close()
}
