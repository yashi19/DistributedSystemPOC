package org.dist.worshop.simplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.dist.util.Networks
import org.dist.workshop.simplekafka.{MyController, MyCreateTopicCommand, MyReplicaAssignmentStrategy, MyZookeeperClient, MyZookeeperClientImpl}
import org.mockito.{ArgumentMatchers, Mockito}

class MyCreateTopicCommandTest extends ZookeeperTestHarness {

  test("Should create topic with given partitions and replications") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: MyZookeeperClient = new MyZookeeperClientImpl(config)

    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(1, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.10", 8000))
    val partitionAssigner: MyReplicaAssignmentStrategy = Mockito.mock(classOf[MyReplicaAssignmentStrategy])
    val noOfPartitions = 2
    val replicationFactor = 2
    val brokerList = List(0,1,2)

    val createTopic = new MyCreateTopicCommand(zookeeperClient,partitionAssigner);

    Mockito.when(partitionAssigner.assignReplicasToBrokers(brokerList,noOfPartitions,replicationFactor))
                          .thenReturn(Set(PartitionReplicas(1,List(1,2)) ,PartitionReplicas(2,List(0,2) )))

    createTopic.createTopic("topic-name",noOfPartitions,replicationFactor)
    assert(zookeeperClient.getPartitionAssignmentsFor("topic-name" ) == List(PartitionReplicas(1,List(1, 2)), PartitionReplicas(2,List(0, 2))))
    Mockito.verify(partitionAssigner).assignReplicasToBrokers(brokerList,noOfPartitions,replicationFactor)
  }

}
