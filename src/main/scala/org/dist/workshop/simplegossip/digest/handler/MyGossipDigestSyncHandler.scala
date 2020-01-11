package org.dist.workshop.simplegossip.digest.handler

import java.math.BigInteger
import java.util

import org.dist.kvstore.{GossipDigest, GossipDigestSyn, InetAddressAndPort, JsonSerDes, Message}
import org.dist.workshop.simplegossip.{MyGossiper, MyMessagingService}

class MyGossipDigestSynHandler(gossiper: MyGossiper, messagingService: MyMessagingService) {
  def handleMessage(synMessage: Message): Unit = {
    val gossipDigestSyn = JsonSerDes.deserialize(synMessage.payloadJson.getBytes, classOf[GossipDigestSyn])

    val deltaGossipDigest = new util.ArrayList[GossipDigest]()
    val deltaEndPointStates = new util.HashMap[InetAddressAndPort, BigInteger]()
    gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)

    val synAckMessage = gossiper.makeGossipDigestAckMessage(deltaGossipDigest, deltaEndPointStates)
    messagingService.sendTcpOneWay(synAckMessage, synMessage.header.from)
  }
}
