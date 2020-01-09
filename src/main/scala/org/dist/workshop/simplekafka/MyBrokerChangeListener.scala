package org.dist.workshop.simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._

class MyBrokerChangeListener(zookeeperClient: MyZookeeperClient,controller: MyController) extends IZkChildListener with Logging {
  this.logIdent = "[BrokerChangeListener started]: "

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- controller.liveBrokers.map(broker => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      newBrokers.foreach(controller.addBroker(_))

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }
}