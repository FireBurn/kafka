package org.apache.kafka.migration;

import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface MigrationClient {
    interface BrokerRegistrationListener {
        void onBrokerChange(Integer brokerId);
        void onBrokersChange();
    }

    void readAllMetadata(Consumer<List<ApiMessageAndVersion>> batchConsumer);

    void watchZkBrokerRegistrations(BrokerRegistrationListener listener);

    void addZkBroker(int brokerId);

    void removeZkBroker(int brokerId);

    Optional<BrokerRegistration> readBrokerRegistration(int brokerId);

    Set<Integer> readBrokerIds();
}
