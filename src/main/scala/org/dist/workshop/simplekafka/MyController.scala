package org.dist.workshop.simplekafka

import java.util.concurrent.atomic.AtomicInteger

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, SimpleSocketServer}

class MyController(val zookeeperClient: MyZookeeperClient, val brokerId: Int) {

  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = -1

  def startup(): Unit = {
    zookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }

  def shutdown() = {
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers();
    zookeeperClient.subscribeBrokerChangeListener(new MyBrokerChangeListener(zookeeperClient,this))
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

}
