package org.dist.worshop.simplekafka

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.SimpleSocketServer
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyBrokerChangeListener, MyController, MyZookeeperClient, MyZookeeperClientImpl}
import org.mockito.Mockito

class MyBrokerChangeListenerTest extends ZookeeperTestHarness {

  test("should listen to broker changes") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: MyZookeeperClient = new MyZookeeperClientImpl(config)
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller: MyController = new MyController(zookeeperClient,config.brokerId,socketServer)
    val brokerListener = new MyBrokerChangeListener(zookeeperClient,controller)
    zookeeperClient.subscribeBrokerChangeListener(brokerListener)


    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8002))


    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 3)
  }

}
