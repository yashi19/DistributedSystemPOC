package org.dist.worshop.simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, SimpleKafkaApi, TopicMetadataResponse, UpdateMetadataRequest}
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyReplicaManager, MySimpleKafkaApi}

class MySimpleKafkaApiTest extends ZookeeperTestHarness {

  test("should create leader and follower replicas") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new MyReplicaManager(config)
    val simpleKafkaApi = new MySimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1)

    simpleKafkaApi.handle(request)

    assert(replicaManager.allPartitions.size() == 1)
    val partition = replicaManager.allPartitions.get(TopicAndPartition("topic1", 0))
    assert(partition.logFile.exists())
  }

  test("should update meta data req") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new MyReplicaManager(config)
    val simpleKafkaApi = new MySimpleKafkaApi(config, replicaManager)
    val listOfLeadersAndBrokers = List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)))))
    val listOfBrokers = List(Broker(0, "10.10.10.10", 8000),Broker(1, "10.10.10.10", 8000))
    val updateMetadataRequest = UpdateMetadataRequest(listOfBrokers,listOfLeadersAndBrokers)
    val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), 1)

    simpleKafkaApi.handle(request)

    assert(simpleKafkaApi.aliveBrokers == listOfBrokers)
    assert(simpleKafkaApi.leaderCache.get(TopicAndPartition("topic1", 0)) == PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))

  }

}
