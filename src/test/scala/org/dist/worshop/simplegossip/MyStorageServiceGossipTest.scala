package org.dist.worshop.simplegossip

import org.dist.kvstore.client.Client
import org.dist.kvstore.{InetAddressAndPort, RowMutationResponse}
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.dist.workshop.simplegossip.MyStorageService
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters._

class MyStorageServiceGossipTest extends FunSuite {
  test("should gossip state to all the nodes in the cluster") {
    val localIp = new Networks().ipv4Address
    val seedIpAndPort = InetAddressAndPort(localIp, 8080)
    val clientListenAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
    val seedStorageService = new MyStorageService(seedIpAndPort, clientListenAddress, seedIpAndPort)
    seedStorageService.start()

    val storages = new java.util.ArrayList[MyStorageService]()
    val basePort = 8081
    val serverCount = 5
    for (i ← 1 to serverCount) {
      val clientAddress = InetAddressAndPort(localIp, TestUtils.choosePort())
      val storage = new MyStorageService(seedIpAndPort, clientAddress, InetAddressAndPort(localIp, basePort + i))
      storage.start()
      storages.add(storage)
    }

    TestUtils.waitUntilTrue(() ⇒ {
      //serverCount + 1 seed
      storages.asScala.toList.map(storageService ⇒ storageService.gossiper.endpointStatemap.size() == serverCount + 1).reduce(_ && _)
    }, "Waiting for all the endpoints to be available on all nodes", 15000)

    storages.asScala.foreach(s ⇒ {
      assert(seedStorageService.gossiper.endpointStatemap.values().contains(s.gossiper.token))
    })

  }
}
