package org.apache.kafka.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.AbstractControlRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface MigrationClient {

    interface BrokerRegistrationListener {
        void onBrokerChange(Integer brokerId);
        void onBrokersChange();
    }

    ZkControllerState claimControllerLeadership(int kraftControllerId);

    void readAllMetadata(Consumer<List<ApiMessageAndVersion>> batchConsumer, Consumer<Integer> brokerIdConsumer);

    void watchZkBrokerRegistrations(BrokerRegistrationListener listener);

    void addZkBroker(int brokerId);

    void removeZkBroker(int brokerId);

    Optional<ZkBrokerRegistration> readBrokerRegistration(int brokerId);

    Set<Integer> readBrokerIds();

    MigrationRecoveryState getOrCreateMigrationRecoveryState(MigrationRecoveryState initialState);

    MigrationRecoveryState setMigrationRecoveryState(MigrationRecoveryState initialState);

    MigrationRecoveryState createTopic(String topicName, Uuid topicId, MigrationRecoveryState state);

    MigrationRecoveryState updateTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, MigrationRecoveryState state, boolean create);

    MigrationRecoveryState createKRaftBroker(int brokerId, BrokerRegistration brokerRegistration, MigrationRecoveryState state);

    MigrationRecoveryState updateKRaftBroker(int brokerId, BrokerRegistration brokerRegistration, MigrationRecoveryState state);

    MigrationRecoveryState removeKRaftBroker(int brokerId, MigrationRecoveryState state);

    void sendRequestToBroker(int brokerId,
                             AbstractControlRequest.Builder<? extends AbstractControlRequest> request,
                             Consumer<AbstractResponse> callback);
}
