package org.dist.workshop.simplekafka

import org.dist.queue.common.Logging
import org.dist.queue.server.Config

class MyServer(val config:Config, val zookeeperClient: MyZookeeperClient, val controller:MyController, val socketServer: MySimpleSocketServer) extends Logging {
  def startup() = {
    socketServer.startup()
    zookeeperClient.registerSelf()
    controller.startup()

    info(s"Server ${config.brokerId} started with log dir ${config.logDirs}")
  }

  def shutdown()= {
    zookeeperClient.shutdown()
    socketServer.shutdown()
  }
}
