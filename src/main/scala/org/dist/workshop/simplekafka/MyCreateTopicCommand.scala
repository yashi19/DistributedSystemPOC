package org.dist.workshop.simplekafka

class MyCreateTopicCommand(zookeeperClient: MyZookeeperClient, partitionAssigner: MyReplicaAssignmentStrategy = new MyReplicaAssignmentStrategy()) {

  def createTopic(topicName: String, noOfPartitions: Int, replicationFactor: Int) = {
    val brokerIds = zookeeperClient.getAllBrokerIds()
    //assign replicas to partition
    val partitionReplicas = partitionAssigner.assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
    // register topic with partition assignments to zookeeper
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }
}
