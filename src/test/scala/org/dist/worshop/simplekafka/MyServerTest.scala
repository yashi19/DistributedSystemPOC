package org.dist.worshop.simplekafka

import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyController, MyServer, MySimpleSocketServer, MyZookeeperClient}
import org.mockito.Mockito

class MyServerTest extends ZookeeperTestHarness {

  test("should register itself to zookeeper on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: MyZookeeperClient = Mockito.mock(classOf[MyZookeeperClient])
    val leaderElector: MyController = Mockito.mock(classOf[MyController])
    val socketServer: MySimpleSocketServer = Mockito.mock(classOf[MySimpleSocketServer])
    var server = new MyServer(config, client, leaderElector, socketServer)

    server.startup()

    Mockito.verify(client).registerSelf()
    Mockito.verify(socketServer).startup()
  }

  test("should elect controller on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: MyZookeeperClient = Mockito.mock(classOf[MyZookeeperClient])
    val leaderElector: MyController = Mockito.mock(classOf[MyController])
    val socketServer: MySimpleSocketServer = Mockito.mock(classOf[MySimpleSocketServer])
    var server = new MyServer(config, client, leaderElector, socketServer)
    server.startup()
    Mockito.verify(client).registerSelf()
    Mockito.verify(leaderElector).startup()
  }

}
