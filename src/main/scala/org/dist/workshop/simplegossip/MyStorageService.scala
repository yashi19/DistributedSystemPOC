package org.dist.workshop.simplegossip

import java.math.BigInteger

import org.apache.log4j.Logger
import org.dist.kvstore._
import org.dist.kvstore.locator.RackUnawareStrategy

class MyStorageService(seed: InetAddressAndPort, clientListenAddress: InetAddressAndPort, val localEndPoint: InetAddressAndPort) {
  val ReplicationFactor = 2
  private val logger = Logger.getLogger(classOf[MyStorageService])

  val token = newToken()
  val tokenMetadata = new TokenMetadata()
  val gossiper = new MyGossiper(seed, localEndPoint, token, tokenMetadata)
  val messagingService = new MyMessagingService(gossiper, this)
  val storageProxy = new MyStorageProxy(clientListenAddress, this, messagingService)

  def start() = {
    // Started TCP listener
    messagingService.listen(localEndPoint)
    gossiper.start()
    tokenMetadata.update(token, localEndPoint)
    storageProxy.start()
  }

  def newToken() = {
    val guid = GuidGenerator.guid
    var token = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token
  }

  def getNStorageEndPointMap(key: String) = {
    val token: BigInteger = FBUtilities.hash(key)
    new RackUnawareStrategy(tokenMetadata).getStorageEndPoints(token, tokenMetadata.cloneTokenEndPointMap)
  }

}
