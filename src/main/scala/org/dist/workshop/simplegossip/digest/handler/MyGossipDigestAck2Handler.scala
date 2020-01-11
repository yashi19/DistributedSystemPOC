package org.dist.workshop.simplegossip.digest.handler

import org.dist.kvstore.{JsonSerDes, Message}
import org.dist.workshop.simplegossip.{MyGossipDigestAck2, MyGossiper, MyMessagingService}

class MyGossipDigestAck2Handler(gossiper: MyGossiper, messagingService: MyMessagingService) {
  def handleMessage(ack2Message: Message): Unit = {
    val gossipDigestAck2 = JsonSerDes.deserialize(ack2Message.payloadJson.getBytes, classOf[MyGossipDigestAck2])
    val epStateMap = gossipDigestAck2.epStateMap
    gossiper.applyStateLocally(epStateMap)
  }
}
