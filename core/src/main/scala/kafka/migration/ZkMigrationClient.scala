package kafka.migration

import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, StateChangeLogger}
import kafka.coordinator.transaction.ZkProducerIdManager
import kafka.migration.ZkMigrationClient.brokerToBrokerRegistration
import kafka.server.{ConfigEntityName, ConfigType, KafkaConfig, ZkAdminManager}
import kafka.utils.Logging
import kafka.zk.{AdminZkClient, BrokerIdZNode, BrokerIdsZNode, KafkaZkClient, ProducerIdBlockZNode}
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper.{ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.{Endpoint, Uuid}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata.{ClientQuotaRecord, ConfigRecord, PartitionRecord, ProducerIdsRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{AbstractResponse, UpdateMetadataRequest, UpdateMetadataResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{BrokerRegistration, VersionRange}
import org.apache.kafka.migration.{KRaftMigrationDriver, MigrationClient}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.zookeeper.client.ZKClientConfig

import java.util
import java.util.{Collections, Optional, Properties}
import java.util.function.Consumer
import scala.jdk.CollectionConverters._

object ZkMigrationClient {
  def brokerToBrokerRegistration(broker: Broker, epoch: Long): BrokerRegistration = {
      new BrokerRegistration(broker.id, epoch, Uuid.ZERO_UUID,
        Collections.emptyList[Endpoint], Collections.emptyMap[String, VersionRange],
        Optional.empty(), false, false)
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    val config = KafkaConfig.fromProps(props)
    val logger = new StateChangeLogger(-1, inControllerContext = false, None)
    val zkClient = KafkaZkClient("localhost:2181", false, 60000, 30000, 10, Time.SYSTEM, "migration", new ZKClientConfig())
    val channelManager = new ControllerChannelManager(() => -1, config, Time.SYSTEM, new Metrics(), logger)
    val migrationSupport = new ZkMigrationClient(zkClient, channelManager)
    val driver = new KRaftMigrationDriver(migrationSupport)
    driver.beginMigration()

    /*migrationSupport.claimControllerLeadership()
    migrationSupport.migrateTopics(MetadataVersion.latest(), batch => {
      System.err.println("")
      batch.forEach(record => System.err.println(record))
    })*/
  }
}

class ZkMigrationClient(zkClient: KafkaZkClient,
                        controllerChannelManager: ControllerChannelManager) extends MigrationClient with Logging {
  def claimControllerLeadership(): Unit = {
    val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(3000, force=true)
    System.err.println(epoch)
    System.err.println(epochZkVersion)
  }

  def sendUMR(): Unit = {
    val (controllerEpoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(3000, force=true)
    Thread.sleep(5000)
    val brokersAndEpochs = zkClient.getAllBrokerAndEpochsInCluster()
    brokersAndEpochs.foreach { case (broker, epoch) =>
      System.err.println(s"Sending (empty) UMR to ${broker.id}")
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(7,
        3000, controllerEpoch, epoch, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap())
      controllerChannelManager.sendRequest(broker.id, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        System.err.println(updateMetadataResponse)
      })
    }
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

  def migrateBrokerConfigs(metadataVersion: MetadataVersion,
                           recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.Broker)
    val batch = new util.ArrayList[ApiMessageAndVersion]()
    zkClient.getEntitiesConfigs(ConfigType.Broker, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = if (broker == ConfigEntityName.Default) {
        ""
      } else {
        broker
      }
      props.forEach { case (key: Object, value: Object) =>
        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(brokerResource)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }
    }
    recordConsumer.accept(batch)
  }

  def migrateClientQuotas(metadataVersion: MetadataVersion,
                          recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)

    def migrateEntityType(entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(entityType).foreach { case (name, props) =>
        val entity = new EntityData().setEntityType(entityType).setEntityName(name)
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(List(entity).asJava)
            .setKey(key)
            .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
        }
        recordConsumer.accept(batch)
      }
    }

    migrateEntityType(ConfigType.User)
    migrateEntityType(ConfigType.Client)
    adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach { case (name, props) =>
      // Lifted from ZkAdminManager
      val components = name.split("/")
      if (components.size != 3 || components(1) != "clients")
        throw new IllegalArgumentException(s"Unexpected config path: ${name}")
      val entity = List(
        new EntityData().setEntityType(ConfigType.User).setEntityName(components(0)),
        new EntityData().setEntityType(ConfigType.Client).setEntityName(components(2))
      )

      val batch = new util.ArrayList[ApiMessageAndVersion]()
      ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
        batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
          .setEntity(entity.asJava)
          .setKey(key)
          .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
      }
      recordConsumer.accept(batch)
    }

    migrateEntityType(ConfigType.Ip)
  }

  def migrateNextProducerId(metadataVersion: MetadataVersion,
                            recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt match {
      case Some(data) =>
        val producerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
        recordConsumer.accept(List(new ApiMessageAndVersion(new ProducerIdsRecord()
          .setBrokerEpoch(-1)
          .setBrokerId(producerIdBlock.assignedBrokerId)
          .setNextProducerId(producerIdBlock.firstProducerId), ProducerIdsRecord.HIGHEST_SUPPORTED_VERSION)).asJava)
      case None => // Nothing to migrate
    }
  }

  override def readAllMetadata(batchConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    migrateTopics(MetadataVersion.latest(), batchConsumer)
    migrateBrokerConfigs(MetadataVersion.latest(), batchConsumer)
    migrateClientQuotas(MetadataVersion.latest(), batchConsumer)
    migrateNextProducerId(MetadataVersion.latest(), batchConsumer)
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

  override def addZkBroker(brokerId: Int): Unit = {
    val brokerAndEpoch = zkClient.getAllBrokerAndEpochsInCluster(Seq(brokerId))
    controllerChannelManager.addBroker(brokerAndEpoch.head._1)
  }

  override def removeZkBroker(brokerId: Int): Unit = {
    controllerChannelManager.removeBroker(brokerId)
  }
}
