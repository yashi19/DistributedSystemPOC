package org.dist.workshop.simplegossip

import java.math.BigInteger
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util

import org.dist.kvstore.{GossipDigest, GossipDigestSyn, Header, InetAddressAndPort, JsonSerDes, Message,RowMutation, RowMutationResponse, Stage, Verb}
import org.dist.util.SocketIO
import org.slf4j.LoggerFactory

trait MessageResponseHandler {
  def response(msg: Message): Unit
}

case class MyGossipDigestAck(val digestList: List[GossipDigest],
                           val epStateMap: Map[InetAddressAndPort, BigInteger])

case class MyGossipDigestAck2(val epStateMap: Map[InetAddressAndPort, BigInteger])


class MyMessagingService(val gossiper: MyGossiper, storageService: MyStorageService) {

  gossiper.setMessageService(this)

  val callbackMap = new util.HashMap[String, MessageResponseHandler]()

  def init(): Unit = {
  }

  def listen(localEp: InetAddressAndPort): Unit = {
    assert(gossiper != null)
    new MyTcpListener(localEp, gossiper, storageService, this).start()
  }

  def sendRR(message: Message, to: List[InetAddressAndPort], messageResponseHandler: MessageResponseHandler): Unit = {
    callbackMap.put(message.header.id, messageResponseHandler)
    to.foreach(address => sendTcpOneWay(message, address))
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[Message](clientSocket, classOf[Message]).write(message)
  }

  def sendUdpOneWay(message: Message, to: InetAddressAndPort) = {
    //for control messages like gossip use udp.
  }
}

