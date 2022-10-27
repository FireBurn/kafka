package org.apache.kafka.migration;

import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Orchestrates the migration. Has its own event loop to deal with asynchronous ZK callbacks.
 */
public class KRaftMigrationDriver {

    class MetadataLogListener implements KRaftMetadataListener {
        MetadataImage image = MetadataImage.EMPTY;
        MetadataDelta delta = new MetadataDelta(image);

        @Override
        public void handleLeaderChange(boolean isActive, int epoch) {
            eventQueue.append(new KRaftLeaderEvent(isActive, nodeId, epoch));
        }

        @Override
        public void handleRecord(long offset, int epoch, ApiMessage record) {
            if (record.apiKey() == MetadataRecordType.NO_OP_RECORD.id()) {
                return;
            }

            eventQueue.append(new EventQueue.Event() {
                @Override
                public void run() throws Exception {
                    if (delta == null) {
                        delta = new MetadataDelta(image);
                    }
                    delta.replay(offset, epoch, record);
                }

                @Override
                public void handleException(Throwable e) {
                    log.error("Had an exception in " + this.getClass().getSimpleName(), e);
                }
            });
        }

        public void syncToZK() {
            eventQueue.append(new ZkWriteEvent(){
                @Override
                public void run() throws Exception {
                    if (delta == null) {
                        return;
                    }

                    log.info("Sync metadata to ZK");
                    try {
                        apply(__ -> migrationState(delta.highestOffset(), delta.highestEpoch()));
                        if (delta.topicsDelta() != null) {
                            delta.topicsDelta().changedTopics().forEach((topicId, topicDelta) -> {
                                // Ensure the topic exists
                                if (image.topics().getTopic(topicId) == null) {
                                    System.err.println("Creating topic " + topicDelta.name() + " in ZK");
                                    // TODO include assignment
                                    apply(migrationState -> client.createTopic(topicDelta.name(), topicId, topicDelta.partitionChanges(), migrationState));
                                } else {
                                    System.err.println("Updating topic " + topicDelta.name() + " in ZK");
                                    apply(migrationState -> client.updateTopicPartitions(Collections.singletonMap(topicDelta.name(), topicDelta.partitionChanges()), migrationState));
                                }
                            });
                        }

                        if (delta.clusterDelta() != null) {
                            delta.clusterDelta().changedBrokers().forEach((brokerId, brokerRegistrationOpt) -> {
                                if (brokerRegistrationOpt.isPresent() && image.cluster().broker(brokerId) == null) {
                                    apply(migrationState -> client.createKRaftBroker(brokerId, brokerRegistrationOpt.get(), migrationState));
                                } else if (brokerRegistrationOpt.isPresent()) {
                                    apply(migrationState -> client.updateKRaftBroker(brokerId, brokerRegistrationOpt.get(), migrationState));
                                } else {
                                    apply(migrationState -> client.removeKRaftBroker(brokerId, migrationState));
                                }
                            });
                        }
                    } finally {
                        image = delta.apply();
                        delta = null;
                    }
                }
            });
        }
    }

    class ZkBrokerListener implements MigrationClient.BrokerRegistrationListener {
        @Override
        public void onBrokerChange(Integer brokerId) {
            eventQueue.append(new BrokerIdChangeEvent(brokerId));
        }

        @Override
        public void onBrokersChange() {
            eventQueue.append(new BrokersChangeEvent());
        }
    }

    abstract class RPCResponseEvent<T extends ApiMessage> implements EventQueue.Event {
        private final int brokerId;
        private final T data;

        RPCResponseEvent(int brokerId, T data) {
            this.brokerId = brokerId;
            this.data = data;
        }

        int brokerId() {
            return brokerId;
        }
        T data() {
            return data;
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    abstract class ZkWriteEvent implements EventQueue.Event {
        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class UpdateMetadataResponseEvent extends RPCResponseEvent<UpdateMetadataResponseData> {
        UpdateMetadataResponseEvent(int brokerId, UpdateMetadataResponseData data) {
            super(brokerId, data);
        }

        @Override
        public void run() throws Exception {
            System.err.println("response from " + brokerId() + ": " + data());
        }
    }

    class PollEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            System.err.println("poll");
            switch (migrationState) {
                case UNINITIALIZED:
                    log.info("Recovering migration state");
                    apply(client::getOrCreateMigrationRecoveryState);
                    client.watchZkBrokerRegistrations(new ZkBrokerListener());
                    String maybeDone = recoveryState.zkMigrationComplete() ? "done" : "not done";
                    log.info("Recovered migration state {}. ZK migration is {}.", recoveryState, maybeDone);
                    transitionTo(MigrationState.INACTIVE);
                    break;
                case INACTIVE:
                    break;
                case NEW_LEADER:
                    eventQueue.append(new BecomeZkLeaderEvent());
                    break;
                case NOT_READY:
                    break;
                case ZK_MIGRATION:
                    eventQueue.append(new MigrateMetadataEvent());
                    break;
                case DUAL_WRITE:
                    listener.syncToZK(); // TODO move to event
                    break;
            }

            // Poll again after some time
            long deadline = time.nanoseconds() + NANOSECONDS.convert(10, SECONDS);
            eventQueue.scheduleDeferred(
                    "poll",
                    new EventQueue.DeadlineFunction(deadline),
                    new PollEvent());
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class MigrateMetadataEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            if (migrationState != MigrationState.ZK_MIGRATION) {
                log.warn("Skipping ZK migration, already done");
                return;
            }

            Set<Integer> brokersWithAssignments = new HashSet<>();
            log.info("Begin migration from ZK");
            consumer.beginMigration();
            try {
                // TODO use metadata transaction here
                List<CompletableFuture<?>> futures = new ArrayList<>();
                client.readAllMetadata(batch -> futures.add(consumer.acceptBatch(batch)), brokersWithAssignments::add);
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).get();

                Set<Integer> brokersWithRegistrations = new HashSet<>(zkBrokerRegistrations.keySet());
                brokersWithAssignments.removeAll(brokersWithRegistrations);
                if (!brokersWithAssignments.isEmpty()) {
                    //throw new IllegalStateException("Cannot migrate data with offline brokers: " + brokersWithAssignments);
                    log.error("Offline ZK brokers detected: {}", brokersWithAssignments);
                }

                // Update the migration state
                OffsetAndEpoch offsetAndEpoch = consumer.completeMigration();
                apply(__ -> migrationState(offsetAndEpoch.offset, offsetAndEpoch.epoch));
            } catch (Throwable t) {
                log.error("Migration failed", t);
                consumer.abortMigration();
            } finally {
                // TODO Just skip to dual write for now
                apply(client::setMigrationRecoveryState);
                transitionTo(MigrationState.DUAL_WRITE);
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BrokersChangeEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            Set<Integer> updatedBrokerIds = client.readBrokerIds();
            Set<Integer> added = new HashSet<>(updatedBrokerIds);
            added.removeAll(zkBrokerRegistrations.keySet());

            Set<Integer> removed = new HashSet<>(zkBrokerRegistrations.keySet());
            removed.removeAll(updatedBrokerIds);

            System.err.println("Brokers added: " + added + ", removed: " + removed);
            added.forEach(brokerId -> {
                Optional<ZkBrokerRegistration> broker = client.readBrokerRegistration(brokerId);
                if (broker.isPresent()) {
                    client.addZkBroker(brokerId);
                    zkBrokerRegistrations.put(brokerId, broker.get());
                } else {
                    throw new IllegalStateException("Saw broker " + brokerId + " added, but registration data is missing");
                }
            });
            removed.forEach(brokerId -> {
                client.removeZkBroker(brokerId);
                zkBrokerRegistrations.remove(brokerId);
            });

            // TODO actually verify the IBP and clusterID
            boolean brokersReady = zkBrokerRegistrations.values().stream().allMatch(broker ->
                broker.isMigrationReady() && broker.ibp().isPresent() && broker.clusterId().isPresent());

            if (brokersReady) {
                System.err.println("Brokers ready");
                //transitionTo(MigrationState.READY);
            } else {
                System.err.println("Brokers not ready");
                //transitionTo(MigrationState.INELIGIBLE);
            }
            // TODO integrate with ClusterControlManager
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BrokerIdChangeEvent implements EventQueue.Event {
        private final int brokerId;

        BrokerIdChangeEvent(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public void run() throws Exception {
            // TODO not sure this is expected. Can registration data change at runtime?
            System.err.println("Broker " + brokerId + " changed!");
            System.err.println(client.readBrokerRegistration(brokerId));
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class KRaftLeaderEvent implements EventQueue.Event {
        private final boolean isActive;
        private final int kraftControllerId;
        private final int kraftControllerEpoch;

        KRaftLeaderEvent(boolean isActive, int kraftControllerId, int kraftControllerEpoch) {
            this.isActive = isActive;
            this.kraftControllerId = kraftControllerId;
            this.kraftControllerEpoch = kraftControllerEpoch;
        }

        @Override
        public void run() throws Exception {
            if (migrationState == MigrationState.UNINITIALIZED) {
                // If we get notified about being the active controller before we have initialized, we need
                // to reschedule this event.
                eventQueue.append(new PollEvent());
                eventQueue.append(this);
                return;
            }

            if (!isActive) {
                apply(state -> state.mergeWithControllerState(ZkControllerState.EMPTY));
                transitionTo(MigrationState.INACTIVE);
            } else {
                // Apply the new KRaft state
                apply(state -> state.withNewKRaftController(kraftControllerId, kraftControllerEpoch));
                transitionTo(MigrationState.NEW_LEADER);
                eventQueue.append(new BecomeZkLeaderEvent());
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BecomeZkLeaderEvent extends ZkWriteEvent {
        @Override
        public void run() throws Exception {
            ZkControllerState zkControllerState = client.claimControllerLeadership(recoveryState.kraftControllerId());
            apply(state -> state.mergeWithControllerState(zkControllerState));

            if (!recoveryState.zkMigrationComplete()) {
                transitionTo(MigrationState.ZK_MIGRATION);
            } else {
                transitionTo(MigrationState.DUAL_WRITE);
            }
        }
    }

    private final Time time;
    private final Logger log;
    private final int nodeId;
    private final MigrationClient client;
    private final KafkaEventQueue eventQueue;
    private volatile MigrationState migrationState;
    private volatile MigrationRecoveryState recoveryState;
    private final Map<Integer, ZkBrokerRegistration> zkBrokerRegistrations = new HashMap<>();
    private final MetadataLogListener listener = new MetadataLogListener();
    private ZkMetadataConsumer consumer;

    public KRaftMigrationDriver(int nodeId, MigrationClient client) {
        this.nodeId = nodeId;
        this.time = Time.SYSTEM;
        this.log = LoggerFactory.getLogger(KRaftMigrationDriver.class); // TODO use LogContext
        this.migrationState = MigrationState.UNINITIALIZED;
        this.recoveryState = MigrationRecoveryState.EMPTY;
        this.client = client;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
    }

    public void setMigrationCallback(ZkMetadataConsumer consumer) {
        this.consumer = consumer;
    }

    private MigrationRecoveryState migrationState(long metadataOffset, long metadataEpoch) {
        return new MigrationRecoveryState(recoveryState.kraftControllerId(), recoveryState.kraftControllerEpoch(),
                metadataOffset, metadataEpoch, time.milliseconds(), recoveryState.migrationZkVersion(), recoveryState.controllerZkVersion());
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    public KRaftMetadataListener listener() {
        return listener;
    }

    private void apply(Function<MigrationRecoveryState, MigrationRecoveryState> stateMutator) {
        MigrationRecoveryState beforeState = KRaftMigrationDriver.this.recoveryState;
        KRaftMigrationDriver.this.recoveryState = stateMutator.apply(beforeState);
        System.err.println("Before: " + beforeState);
        System.err.println("After: " + KRaftMigrationDriver.this.recoveryState);

    }

    private void transitionTo(MigrationState newState) {
        // TODO enforce state machine
        /*if (newState == migrationState) {
            return;
        }

        switch (migrationState) {
            case UNKNOWN:
                if (newState != MigrationState.INELIGIBLE) {
                    throw new IllegalStateException();
                }
                break;
            case INELIGIBLE:
                if (newState != MigrationState.READY) {
                    throw new IllegalStateException();
                }
                break;
        }*/

        migrationState = newState;
        eventQueue.append(new PollEvent());
    }
}
