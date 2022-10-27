package org.apache.kafka.migration;

public enum MigrationState {
    UNINITIALIZED(false),   // Initial state
    INACTIVE(false),        // State when not the active controller
    NEW_LEADER(false),      // State after KRaft leader election and before ZK leadership claim
    NOT_READY(true),        // The cluster is not ready to begin migration
    ZK_MIGRATION(true),     // The cluster has satisfied the migration criteria
    DUAL_WRITE(true);       // The data has been migrated

    private final boolean isActiveController;

    MigrationState(boolean isActiveController) {
        this.isActiveController = isActiveController;
    }

    boolean isActiveController() {
        return isActiveController;
    }
}
