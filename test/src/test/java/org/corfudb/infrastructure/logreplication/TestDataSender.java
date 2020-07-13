package org.corfudb.infrastructure.logreplication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationAckMessage;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * Test Implementation of Snapshot Data Sender
 */
@Slf4j
public class TestDataSender implements DataSender {

    @Getter
    private Queue<LogReplicationEntry> entryQueue = new LinkedList<>();

    public TestDataSender() {
    }

    @Override
    public CompletableFuture<LogReplicationAckMessage> send(LogReplicationEntry message) {
        if (message != null && message.getPayload() != null) {
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_MESSAGE) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                // Ignore, do not account Start and End Markers as messages
                entryQueue.add(message);
                log.info("add message to the entryQueue {}", entryQueue.size());
            }
        }

        CompletableFuture<LogReplicationAckMessage> cf = new CompletableFuture<>();
        LogReplicationAckMessage ack = LogReplicationAckMessage.generateAck(message.getMetadata());
        cf.complete(ack);

        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationAckMessage> send(List<LogReplicationEntry> messages) {

        CompletableFuture<LogReplicationAckMessage> lastSentMessage = new CompletableFuture<>();

        if (messages != null && !messages.isEmpty()) {
            CompletableFuture<LogReplicationAckMessage> tmp;

            for (LogReplicationEntry message : messages) {
                tmp = send(message);
                if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_TRANSFER_END) ||
                        message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                    lastSentMessage = tmp;
                }
            }
        }

        return lastSentMessage;
    }

    @Override
    public CompletableFuture<LogReplicationQueryMetadataResponse> sendQueryMetadataRequest() throws ExecutionException, InterruptedException {
        log.warn("Not implemented");
        return null;
    }

    public void reset() {
        entryQueue.clear();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
