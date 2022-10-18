package kafka.migration

import kafka.cluster.Broker
import kafka.migration.ZkMigrationClient.brokerToBrokerRegistration
import kafka.server.ConfigType
import kafka.utils.Logging
import kafka.zk.{BrokerIdZNode, BrokerIdsZNode, KafkaZkClient}
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper.{ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.{Endpoint, Uuid}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{BrokerRegistration, VersionRange}
import org.apache.kafka.migration.{KRaftMigrationDriver, MigrationClient}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.zookeeper.client.ZKClientConfig

import java.util
import java.util.{Collections, Optional}
import java.util.function.Consumer
import scala.jdk.CollectionConverters._

object ZkMigrationClient {
  def brokerToBrokerRegistration(broker: Broker, epoch: Long): BrokerRegistration = {
      new BrokerRegistration(broker.id, epoch, Uuid.ZERO_UUID,
        Collections.emptyList[Endpoint], Collections.emptyMap[String, VersionRange],
        Optional.empty(), false, false)
  }

  def main(args: Array[String]): Unit = {
    val zkClient = KafkaZkClient("localhost:2181", false, 60000, 30000, 10, Time.SYSTEM, "migration", new ZKClientConfig())
    val migrationSupport = new ZkMigrationClient(zkClient)
    val driver = new KRaftMigrationDriver(migrationSupport)
    driver.beginMigration()
    /*migrationSupport.claimControllerLeadership()
    migrationSupport.migrateTopics(MetadataVersion.latest(), batch => {
      System.err.println("")
      batch.forEach(record => System.err.println(record))
    })*/
  }
}

class ZkMigrationClient(zkClient: KafkaZkClient) extends MigrationClient with Logging {
  def claimControllerLeadership(): Unit = {
    val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(3000, force=true)
    System.err.println(epoch)
    System.err.println(epochZkVersion)
  }

  def migrateTopics(metadataVersion: MetadataVersion,
                    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, assignments) =>
      val partitions = assignments.keys.toSeq
      val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
      val topicBatch = new util.ArrayList[ApiMessageAndVersion]()
      topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
        .setName(topic)
        .setTopicId(topicIdOpt.get), TopicRecord.HIGHEST_SUPPORTED_VERSION))

      assignments.foreach { case (topicPartition, replicaAssignment) =>
        val leaderIsrAndEpoch = leaderIsrAndControllerEpochs(topicPartition)
        topicBatch.add(new ApiMessageAndVersion(new PartitionRecord()
          .setTopicId(topicIdOpt.get)
          .setPartitionId(topicPartition.partition)
          .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
          .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
          .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
          .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
          .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
          .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
          .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
          .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value()), PartitionRecord.HIGHEST_SUPPORTED_VERSION))
      }

      val props = topicConfigs(topic)
      props.forEach { case (key: Object, value: Object) =>
        topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.TOPIC.id)
          .setResourceName(topic)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }

      recordConsumer.accept(topicBatch)
    }
  }

  override def readAllMetadata(batchConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    migrateTopics(MetadataVersion.latest(), batchConsumer)
  }

  override def watchZkBrokerRegistrations(listener: MigrationClient.BrokerRegistrationListener): Unit = {
    val brokersHandler = new ZNodeChildChangeHandler() {
      override val path: String = BrokerIdsZNode.path

      override def handleChildChange(): Unit = listener.onBrokersChange()
    }
    System.err.println("Adding /brokers watch")
    zkClient.registerZNodeChildChangeHandler(brokersHandler)

    def brokerHandler(brokerId: Int): ZNodeChangeHandler = {
      new ZNodeChangeHandler() {
        override val path: String = BrokerIdZNode.path(brokerId)

        override def handleDataChange(): Unit = listener.onBrokerChange(brokerId)
      }
    }

    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster()
    curBrokerAndEpochs.foreach { case (broker, _) =>
      System.err.println(s"Adding /brokers/${broker.id} watch")
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerHandler(broker.id))
    }

    listener.onBrokersChange()
  }

  override def readBrokerRegistration(brokerId: Int): Optional[BrokerRegistration] = {
    val brokerAndEpoch = zkClient.getAllBrokerAndEpochsInCluster(Seq(brokerId))
    if (brokerAndEpoch.isEmpty) {
      Optional.empty()
    } else {
      Optional.of(brokerToBrokerRegistration(brokerAndEpoch.head._1, brokerAndEpoch.head._2))
    }
  }

  override def readBrokerIds(): util.Set[Integer] = {
    zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava
  }
}
