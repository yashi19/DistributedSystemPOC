package org.dist.workshop.simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils.Broker

class MyBrokerChangeListener(zookeeperClient: MyZookeeperClient) extends IZkChildListener with Logging {
  this.logIdent = "[BrokerChangeListener started]: "

  import scala.jdk.CollectionConverters._
  var liveBrokers: Set[Broker] = Set()

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }


  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- liveBrokers.map(broker => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      newBrokers.foreach(addBroker(_))

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }
}