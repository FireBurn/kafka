package org.apache.kafka.migration;

import org.apache.kafka.common.protocol.ApiMessage;

public interface KRaftMetadataListener {

    void handleLeaderChange(boolean isActive, int epoch);

    void handleRecord(long offset, int epoch, ApiMessage record);
}
