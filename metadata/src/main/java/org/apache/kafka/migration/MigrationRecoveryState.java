package org.apache.kafka.migration;

import java.util.Objects;

public class MigrationRecoveryState {

    public static MigrationRecoveryState EMPTY = new MigrationRecoveryState(-1, -1, -1, -1, -1, -1, -1);

    private final int kraftControllerId;

    private final int kraftControllerEpoch;

    private final long kraftMetadataOffset;

    private final long kraftMetadataEpoch;

    private final long lastUpdatedTimeMs;

    private final int migrationZkVersion;

    private final int controllerZkVersion;

    public MigrationRecoveryState(int kraftControllerId, int kraftControllerEpoch,
                                  long kraftMetadataOffset, long kraftMetadataEpoch,
                                  long lastUpdatedTimeMs, int migrationZkVersion, int controllerZkVersion) {
        this.kraftControllerId = kraftControllerId;
        this.kraftControllerEpoch = kraftControllerEpoch;
        this.kraftMetadataOffset = kraftMetadataOffset;
        this.kraftMetadataEpoch = kraftMetadataEpoch;
        this.lastUpdatedTimeMs = lastUpdatedTimeMs;
        this.migrationZkVersion = migrationZkVersion;
        this.controllerZkVersion = controllerZkVersion;
    }

    public MigrationRecoveryState mergeWithControllerState(ZkControllerState state) {
        return new MigrationRecoveryState(
            this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, state.zkVersion());
    }

    public MigrationRecoveryState withZkVersion(int zkVersion) {
        return new MigrationRecoveryState(
                this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
                this.kraftMetadataEpoch, this.lastUpdatedTimeMs, zkVersion, this.controllerZkVersion);
    }

    public MigrationRecoveryState withNewKRaftController(int controllerId, int controllerEpoch) {
        return new MigrationRecoveryState(
                controllerId, controllerEpoch, this.kraftMetadataOffset,
                this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, this.controllerZkVersion);
    }

    public int kraftControllerId() {
        return kraftControllerId;
    }

    public int kraftControllerEpoch() {
        return kraftControllerEpoch;
    }

    public long kraftMetadataOffset() {
        return kraftMetadataOffset;
    }

    public long kraftMetadataEpoch() {
        return kraftMetadataEpoch;
    }

    public long lastUpdatedTimeMs() {
        return lastUpdatedTimeMs;
    }

    public int migrationZkVersion() {
        return migrationZkVersion;
    }

    public int controllerZkVersion() {
        return controllerZkVersion;
    }

    public boolean zkMigrationComplete() {
        return kraftMetadataOffset > 0;
    }

    @Override
    public String toString() {
        return "MigrationRecoveryState{" +
                "kraftControllerId=" + kraftControllerId +
                ", kraftControllerEpoch=" + kraftControllerEpoch +
                ", kraftMetadataOffset=" + kraftMetadataOffset +
                ", kraftMetadataEpoch=" + kraftMetadataEpoch +
                ", lastUpdatedTimeMs=" + lastUpdatedTimeMs +
                ", migrationZkVersion=" + migrationZkVersion +
                ", controllerZkVersion=" + controllerZkVersion +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationRecoveryState that = (MigrationRecoveryState) o;
        return kraftControllerId == that.kraftControllerId && kraftControllerEpoch == that.kraftControllerEpoch && kraftMetadataOffset == that.kraftMetadataOffset && kraftMetadataEpoch == that.kraftMetadataEpoch && lastUpdatedTimeMs == that.lastUpdatedTimeMs && migrationZkVersion == that.migrationZkVersion && controllerZkVersion == that.controllerZkVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kraftControllerId, kraftControllerEpoch, kraftMetadataOffset, kraftMetadataEpoch, lastUpdatedTimeMs, migrationZkVersion, controllerZkVersion);
    }
}
