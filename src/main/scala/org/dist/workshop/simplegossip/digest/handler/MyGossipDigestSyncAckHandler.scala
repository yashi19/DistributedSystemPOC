package org.dist.workshop.simplegossip.digest.handler

import java.math.BigInteger
import java.util

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes, Message}
import org.dist.workshop.simplegossip.{MyGossipDigestAck, MyGossiper, MyMessagingService}


class MyGossipDigestSynAckHandler(gossiper: MyGossiper, messagingService: MyMessagingService) {
  def handleMessage(synAckMessage: Message): Unit = {
    val gossipDigestSynAck: MyGossipDigestAck = JsonSerDes.deserialize(synAckMessage.payloadJson.getBytes, classOf[MyGossipDigestAck])
    val epStateMap: Map[InetAddressAndPort, BigInteger] = gossipDigestSynAck.epStateMap
    if (epStateMap.size > 0) {
      gossiper.applyStateLocally(epStateMap)
    }

    /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
    val deltaEpStateMap = new util.HashMap[InetAddressAndPort, BigInteger]

    for (gDigest <- gossipDigestSynAck.digestList) {
      val addr = gDigest.endPoint
      val localEpStatePtr = gossiper.getStateFor(addr)
      if (localEpStatePtr != null) deltaEpStateMap.put(addr, localEpStatePtr)
    }

    val ack2Message = gossiper.makeGossipDigestAck2Message(deltaEpStateMap)
    messagingService.sendTcpOneWay(ack2Message, synAckMessage.header.from)
  }
}