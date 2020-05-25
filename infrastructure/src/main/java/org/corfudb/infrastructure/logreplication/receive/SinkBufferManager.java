package org.corfudb.infrastructure.logreplication.receive;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.HashMap;

@Slf4j
/**
 * For snapshot sync and log entry sync, it is possible that the messages generated by the primary site will
 * be delivered out of order due to message loss due to network connect loss or congestion. At the backup site
 * we keep a buffer to store the out of order messages and apply them at the backup site in order.
 * For snapshot sync, the message will be applied according the seqNumber.
 * For log entry sync, each message has a pre pointer that is a timestamp of the premessage, this guarantees that
 * the messages will be applied in order.
 * At the same time, we should back an ACK to the primary site to notify the primary site any possible data loss.
 */
public class SinkBufferManager {
    // It is implemented as a hashmap.
    HashMap<Long, LogReplicationEntry> buffer;
    LogReplicationSinkManager sinkManager;
    MessageType type;
    int size;

    // How frequent in time, the ack will be sent.
    private int ackCycleTime;

    // If ackCycleCnt number of messages has been processed, it will trigger an ack too.
    private int ackCycleCnt;

    // Reset the ackCnt after sending an ACK
    private int ackCnt = 0;
    private long ackTime = 0;

    // The message with the key are expecting.
    // For snapshot sync, the ack should be nextKey - 1
    // For log entry sync, the ack is the nextKey which is the timestamp the last log
    // entry has been processed.
    long nextKey;

    public SinkBufferManager(MessageType type, int ackCycleTime, int ackCycleCnt, int size, long nextKey, LogReplicationSinkManager sinkManager) {
        this.type = type;
        this.ackCycleTime = ackCycleTime;
        this.ackCycleCnt = ackCycleCnt;
        this.size = size;
        this.sinkManager = sinkManager;
        this.nextKey = nextKey;
        buffer = new HashMap<>();
    }

    long getKey(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry) {
        switch (entry.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
                return entry.getMetadata().getSnapshotSyncSeqNum();
            case LOG_ENTRY_MESSAGE:
                return entry.getMetadata().getPreviousTimestamp();
            default:
                log.warn("wrong type of metadata {}", entry.getMetadata());
                return -1;
        }
    }

    long getNextKey(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry) {
        switch (entry.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
                return entry.getMetadata().getSnapshotSyncSeqNum() + 1;
            case LOG_ENTRY_MESSAGE:
                return entry.getMetadata().getTimestamp();
            default:
                log.warn("wrong type of metadata {}", entry.getMetadata());
                return -1;
        }
    }

    void processBuffer() {
        while (true) {
            LogReplicationEntry dataMessage = buffer.get(nextKey);
            if (dataMessage == null)
                return;
            sinkManager.receiveWithoutBuffering(dataMessage);
            nextKey = getNextKey(dataMessage);
        }
    }

    boolean shouldAck() {
        ackCnt++;
        long currentTime = java.lang.System.currentTimeMillis();
        if (ackCnt == ackCycleCnt || (currentTime - ackTime) >= ackCycleTime) {
            ackCnt = 0;
            ackTime = currentTime;
            return true;
        }

        return false;
    }

    LogReplicationEntryMetadata makeAckMessage(LogReplicationEntry entry) {
        long ackTimestamp;
        MessageType messageType;
        switch (type) {
            case SNAPSHOT_MESSAGE:
                ackTimestamp = nextKey - 1;
                messageType = MessageType.SNAPSHOT_REPLICATED;
                break;
            case LOG_ENTRY_MESSAGE:
                ackTimestamp = nextKey;
                messageType = MessageType.LOG_ENTRY_REPLICATED;
                break;
            default:
                log.error("Wrong type of message {}", type);
        }

        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.LOG_ENTRY_REPLICATED,
                entry.getMetadata().getSiteEpoch(),
                entry.getMetadata().getSyncRequestId(), nextKey,
                entry.getMetadata().getSnapshotTimestamp());
        return metadata;
    }

    /**
     * If the message is the expected message, will push down to sinkManager to process it.
     * Then process the message in the buffer if the next expected messages are in the buffer in order.
     * Otherwise put the received message into the buffer.
     * At the end according to the ack policy, send ack.
     * @param dataMessage
     */
    public LogReplicationEntry processMsgAndBuffer(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry dataMessage) {
        if (dataMessage.getMetadata().getMessageMetadataType() != type) {
            log.warn("Got msg type {} but expecting type {}", dataMessage.getMetadata().getMessageMetadataType(), type);
            return null;
        }

        long key = getKey(dataMessage);

        if (getKey(dataMessage) == nextKey) {
            sinkManager.receiveWithoutBuffering(dataMessage);
            nextKey = getNextKey(dataMessage);
            processBuffer();
        } else {
            buffer.put(key, dataMessage);
        }

        /*
         * send ack up to
         */
        if (shouldAck()) {
            LogReplicationEntryMetadata metadata = makeAckMessage(dataMessage);
            return new LogReplicationEntry(metadata, new byte[0]);
        }

        return null;
    }
}