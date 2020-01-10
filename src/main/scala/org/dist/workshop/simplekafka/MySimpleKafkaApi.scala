package org.dist.workshop.simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicaRequest, PartitionInfo}


class MySimpleKafkaApi(config: Config, replicaManager: MyReplicaManager) {
  var aliveBrokers = List[Broker]()
  var leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]

  def handle(request: RequestOrResponse): RequestOrResponse = {
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey => {
        val leaderAndReplicasRequest: LeaderAndReplicaRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        leaderAndReplicasRequest.leaderReplicas.foreach(leaderAndReplicas ⇒ {
          val topicAndPartition = leaderAndReplicas.topicPartition
          val leader = leaderAndReplicas.partitionStateInfo.leader
          if (leader.id == config.brokerId)
            replicaManager.makeLeader(topicAndPartition)
          else
            replicaManager.makeFollower(topicAndPartition, leader.id)
        })
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
      }

      //      case _ ⇒ RequestOrResponse(0, "Unknown Request", request.correlationId)
    }
  }
}
