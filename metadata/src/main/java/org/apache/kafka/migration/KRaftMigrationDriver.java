package org.apache.kafka.migration;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;

import java.util.HashSet;
import java.util.Set;

/**
 * Orchestrates the migration. Has its own event loop to deal with asynchronous ZK callbacks.
 */
public class KRaftMigrationDriver {
    class BrokerListener implements MigrationClient.BrokerRegistrationListener {
        @Override
        public void onBrokerChange(Integer brokerId) {
            eventQueue.append(new BrokerIdChangeEvent(brokerId));
        }

        @Override
        public void onBrokersChange() {
            eventQueue.append(new BrokersChangeEvent());
        }
    }

    class BeginEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            client.readAllMetadata(batch -> {
                System.err.println("Batch:");
                batch.forEach(System.err::println);
            });
            client.watchZkBrokerRegistrations(new BrokerListener());
        }
    }

    class BrokersChangeEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            Set<Integer> updatedBrokerIds = client.readBrokerIds();
            Set<Integer> added = new HashSet<>(updatedBrokerIds);
            added.removeAll(brokerIds);

            Set<Integer> removed = new HashSet<>(brokerIds);
            removed.removeAll(updatedBrokerIds);

            System.err.println("Brokers added: " + added + ", removed: " + removed);
            added.forEach(brokerId -> {
                client.addZkBroker(brokerId);
                System.err.println(client.readBrokerRegistration(brokerId));
            });
            removed.forEach(client::removeZkBroker);

            brokerIds.addAll(added);
            brokerIds.removeAll(removed);

            // TODO integrate with ClusterControlManager
        }
    }

    class BrokerIdChangeEvent implements EventQueue.Event {
        private final int brokerId;

        BrokerIdChangeEvent(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public void run() throws Exception {
            System.err.println("Broker " + brokerId + " changed!");
            System.err.println(client.readBrokerRegistration(brokerId));
        }
    }

    private final MigrationClient client;

    private final KafkaEventQueue eventQueue;

    private final Set<Integer> brokerIds = new HashSet<>();

    public KRaftMigrationDriver(MigrationClient client) {
        this.client = client;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
    }

    public void beginMigration() {
        eventQueue.append(new BeginEvent());
    }
}
