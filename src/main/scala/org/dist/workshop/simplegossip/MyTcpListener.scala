package org.dist.workshop.simplegossip

import java.net.{InetSocketAddress, ServerSocket}

import org.dist.kvstore.{InetAddressAndPort, Message, Verb}
import org.dist.util.SocketIO
import org.dist.workshop.simplegossip.digest.handler.{MyGossipDigestAck2Handler, MyGossipDigestSynAckHandler, MyGossipDigestSynHandler}
import org.slf4j.LoggerFactory

class MyTcpListener(localEp: InetAddressAndPort, gossiper: MyGossiper, storageService: MyStorageService, messagingService: MyMessagingService) extends Thread {
  private val logger = LoggerFactory.getLogger(classOf[MyTcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))

    logger.info(s"Listening on ${localEp}")

    while (true) {
      val socket = serverSocket.accept()
      val message = new SocketIO[Message](socket, classOf[Message]).read()
      logger.debug(s"Got message ${message}")

      if (message.header.verb == Verb.GOSSIP_DIGEST_SYN) {
        new MyGossipDigestSynHandler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK) {
        new MyGossipDigestSynAckHandler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK2) {
        new MyGossipDigestAck2Handler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.RESPONSE) {
        val handler = messagingService.callbackMap.get(message.header.id)
        if (handler != null) handler.response(message)
      }
      //      else if (message.header.verb == Verb.ROW_MUTATION) {
      //        new RowMutationHandler(storageService, messagingService).handleMessage(message)
      //      }
    }
  }


  //  class RowMutationHandler(storageService: MyStorageService, messagingService: MyMessagingService) {
  //    def handleMessage(rowMutationMessage: Message) = {
  //      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
  //      val success = storageService.apply(rowMutation)
  //      val response = RowMutationResponse(1, rowMutation.key, success)
  //      val responseMessage = Message(Header(localEp, Stage.RESPONSE_STAGE, Verb.RESPONSE, rowMutationMessage.header.id), JsonSerDes.serialize(response))
  //      messagingService.sendTcpOneWay(responseMessage, rowMutationMessage.header.from)
  //    }
  //  }


}
