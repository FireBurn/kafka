package org.apache.kafka.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.BrokerRegistration;

import java.util.Objects;
import java.util.Optional;


public class ZkBrokerRegistration {
    private final BrokerRegistration registration;
    private final String ibp;
    private final Uuid clusterId;
    private final boolean migrationReady;

    public ZkBrokerRegistration(BrokerRegistration registration, String ibp, Uuid clusterId, boolean migrationReady) {
        this.registration = registration;
        this.ibp = ibp;
        this.clusterId = clusterId;
        this.migrationReady = migrationReady;
    }

    public BrokerRegistration brokerRegistration() {
        return registration;
    }

    public Optional<String> ibp() {
        return Optional.ofNullable(ibp);
    }

    public Optional<Uuid> clusterId() {
        return Optional.ofNullable(clusterId);
    }

    public boolean isMigrationReady() {
        return migrationReady;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZkBrokerRegistration that = (ZkBrokerRegistration) o;
        return migrationReady == that.migrationReady && registration.equals(that.registration) && ibp.equals(that.ibp) && clusterId.equals(that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(registration, ibp, clusterId, migrationReady);
    }

    @Override
    public String toString() {
        return "ZkBrokerRegistration{" +
                "registration=" + registration +
                ", ibp='" + ibp + '\'' +
                ", clusterId=" + clusterId +
                ", migrationReady=" + migrationReady +
                '}';
    }
}
