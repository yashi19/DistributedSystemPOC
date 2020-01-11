package org.dist.workshop.simplegossip.digest

import java.util

import org.dist.kvstore.{EndPointState, GossipDigest, InetAddressAndPort}

import scala.jdk.CollectionConverters._


object GossipDigestAck {
  def create(gDigestList: List[MyGossipDigest], epStateMap: Map[InetAddressAndPort, EndPointState]) = {
    val map: util.Map[String, EndPointState] = new util.HashMap[String, EndPointState]()
    val set = epStateMap.keySet
    for (key <- set) {
      val newKey = s"${key.address.getHostAddress}:${key.port}"
      map.put(newKey, epStateMap.asJava.get(key))
    }
    MyGossipDigestAck(gDigestList, map.asScala.toMap)
  }
}

case class MyGossipDigestAck(val gDigestList: List[MyGossipDigest],
                           val epStateMap: Map[String, EndPointState]) {

  def digestList = if (gDigestList == null) List[MyGossipDigest]() else gDigestList

  def stateMap() = {
    if (epStateMap == null) {
      new util.HashMap[InetAddressAndPort, EndPointState]().asScala
    } else {
      val map: util.Map[InetAddressAndPort, EndPointState] = new util.HashMap[InetAddressAndPort, EndPointState]()
      val set = epStateMap.keySet
      for (key <- set) {
        val splits = key.split(":")
        map.put(InetAddressAndPort.create(splits(0), splits(1).toInt), epStateMap.asJava.get(key))
      }
      map.asScala
    }
  }

}