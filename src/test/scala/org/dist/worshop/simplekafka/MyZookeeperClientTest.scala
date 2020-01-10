package org.dist.worshop.simplekafka

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyZookeeperClient, MyZookeeperClientImpl}

class MyZookeeperClientTest extends ZookeeperTestHarness {
  test("should register broker") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: MyZookeeperClient = new MyZookeeperClientImpl(config)

    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))

    assert(zookeeperClient.getAllBrokerIds().size == 1)
    assert(zookeeperClient.getAllBrokerIds().contains(0))
  }

}
