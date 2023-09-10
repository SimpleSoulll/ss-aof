package com.hx.spark.sql.connector.aof.zookeeper

import org.apache.curator.framework.CuratorFramework

/**
 * @author AC 
 */
private[aof] object ZkHelper {

  implicit class Operations(private val client: CuratorFramework) extends AnyVal {

    private def createNodeIfNotExists(node: String, data: String = ""): Unit = {
      if (client.checkExists().forPath(node) == null) {
        client.create().creatingParentsIfNeeded.forPath(node, data.getBytes)
      }
    }

    def createNodeIfNotExists(node: String): Unit = createNodeIfNotExists(node)

    def createAndSetData(node: String, data: String): Unit = createNodeIfNotExists(node, data)
  }
}
