package org.dist.worshop.simplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyController, MySimpleSocketServer, MyZookeeperClient}
import org.mockito.{ArgumentMatchers, Mockito}

class MyControllerTest extends ZookeeperTestHarness {

  test("Should elect first server as controller and get all live brokers") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    //  val zookeeperClient: MyZookeeperClient = new MyZookeeperClientImpl(config)
    val zookeeperClient: MyZookeeperClient = Mockito.mock(classOf[MyZookeeperClient])
    val socketServer = Mockito.mock(classOf[MySimpleSocketServer])
    val controller = new MyController(zookeeperClient, config.brokerId, socketServer)
    //    zookeeperClient.registerBroker(Broker(1, "10.10.10.10", 9000));
    Mockito.when(zookeeperClient.subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())).thenReturn(None)
    Mockito.when(zookeeperClient.getAllBrokers()).thenReturn(Set(Broker(1, "10.10.10.10", 9000)))

    controller.startup()

    Mockito.verify(zookeeperClient).subscribeBrokerChangeListener(ArgumentMatchers.any[IZkChildListener]())
    assert(controller.liveBrokers == Set(Broker(1, "10.10.10.10", 9000)))
  }

}
