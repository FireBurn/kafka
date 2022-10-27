package org.apache.kafka.migration;

import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface ZkMetadataConsumer {
    void beginMigration();
    CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch);
    OffsetAndEpoch completeMigration();
    void abortMigration();
}
