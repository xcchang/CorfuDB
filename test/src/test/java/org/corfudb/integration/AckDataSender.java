package org.corfudb.integration;

import lombok.Data;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;

import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Data
public class AckDataSender implements DataSender {

    private UUID snapshotSyncRequestId;
    private long baseSnapshotTimestamp;
    private LogReplicationSourceManager sourceManager;
    private ExecutorService channel;

    public AckDataSender() {
        channel = Executors.newSingleThreadExecutor();
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        // Emulate it was sent over the wire and arrived on the source side
        // channel.execute(() -> sourceManager.receive(message));
        return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {
        CompletableFuture<LogReplicationEntry> ackCF = new CompletableFuture<>();
        messages.forEach(msg -> send(msg));
        return ackCF;
    }

    @Override
    public void onError(LogReplicationError error) {
        fail("On Error received for log entry sync");
    }
}
