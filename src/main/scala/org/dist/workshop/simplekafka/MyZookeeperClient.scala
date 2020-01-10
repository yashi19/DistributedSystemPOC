package org.dist.workshop.simplekafka

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.server.Config
import org.dist.queue.utils.ZKStringSerializer
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, PartitionReplicas}

import scala.jdk.CollectionConverters._

trait MyZookeeperClient {
  def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]]

  def tryCreatingControllerPath(leaderId: String)

  def registerBroker(broker: Broker)

  def getAllBrokerIds(): Set[Int]

  def getBrokerInfo(brokerId: Int): Broker

  def getAllBrokers(): Set[Broker]

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]]

  def subscribeControllerChangeListener(controller: MyController): Unit

  def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas]

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas])
}

class MyZookeeperClientImpl(config: Config) extends MyZookeeperClient {
  val BROKER_IDS_PATH = "/brokers/ids"
  val CONTROLLER_PATH = "/controllers"
  val BROKER_TOPIC_PATH = "/brokers/topics"


  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)

  def getBrokerPath(id: Int) = {
    BROKER_IDS_PATH + "/" + id
  }

  private def getTopicPath(topicName: String) = {
    BROKER_TOPIC_PATH + "/" + topicName
  }

  def createParentPath(zkClient: ZkClient, brokerPath: String) = {
    val parentDir = brokerPath.substring(0, brokerPath.lastIndexOf('/'))
    if (parentDir.length != 0)
      zkClient.createPersistent(parentDir, true)
  }

  def createEphemeralPath(zkClient: ZkClient, brokerPath: String, brokerData: String) = {
    try {
      zkClient.createEphemeral(brokerPath, brokerData)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(zkClient, brokerPath)
        zkClient.createEphemeral(brokerPath, brokerData)
      }
    }
  }

  override def registerBroker(broker: Broker): Unit = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  override def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BROKER_IDS_PATH).asScala.map(_.toInt).toSet
  }

  override def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  override def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BROKER_IDS_PATH, listener)
    Option(result).map(_.asScala.toList)
  }

  override def subscribeControllerChangeListener(controller: MyController): Unit = {
    zkClient.subscribeDataChanges(CONTROLLER_PATH, new MyControllerChangeListener(zkClient, controller))
  }

  override def tryCreatingControllerPath(controllerId: String): Unit = {
    try {
      createEphemeralPath(zkClient, CONTROLLER_PATH, controllerId)
    } catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = zkClient.readData(CONTROLLER_PATH)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  override def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BROKER_IDS_PATH).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  override def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BROKER_IDS_PATH, listener)
    Option(result).map(_.asScala.toList)
  }

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = {
    val partitionAssignments: String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[PartitionReplicas]]() {})
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }


  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]) = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }


}
