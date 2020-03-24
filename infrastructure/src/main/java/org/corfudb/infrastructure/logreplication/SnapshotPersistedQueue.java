package org.corfudb.infrastructure.logreplication;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

@Slf4j
/**
 * While the sink manager receive the message, it will write to the persistent queue first.
 * When the full snapshot has been written to the persistent queue, it starts to replay at the
 * corfu database by calling snapshot writer.
 *
 * Each snapshot has its own persistent queue.
 * While writing to the persistent queue,
 * it will inspect and update the persistent queue metadata in one transaction.
 */
public class SnapshotPersistedQueue {
    private static final String SNAPSHOT_PERSISTENT_QUEUE_NAME = "snapshotPersistentQueue";
    CorfuQueue<LogReplicationEntry> corfuQueue;
    CorfuRuntime runtime;
    PersistedWriterMetadata persistedWriterMetadata;
    long snapshot;

    public SnapshotPersistedQueue(CorfuRuntime runtime, PersistedWriterMetadata persistedWriterMetadata) {
        this.runtime = runtime;
        this.persistedWriterMetadata = persistedWriterMetadata;
    }

    /** create a persistent queue with timestamp LastSnapStart as name
     *
     */
    void setupWritePersistedQueue() {
        snapshot = persistedWriterMetadata.getLastSnapStart();
        corfuQueue = new CorfuQueue<>(runtime, SNAPSHOT_PERSISTENT_QUEUE_NAME + snapshot);
    }

    /**
     * open a persistent queue with timestamp LastSnapTransferDone as name
     *
     */
    boolean setupReadPersistedQueue() {
        snapshot = persistedWriterMetadata.getLastSnapTransferDone();
        if (persistedWriterMetadata.getLastSnapStart() != snapshot) {
            log.error(" lastSnapStart {} != lastSnapTransferred {}. Could not read persistedQueue", persistedWriterMetadata.getLastSnapStart(),
                    persistedWriterMetadata.getLastSnapTransferDone());
            return false;
        }

        corfuQueue = new CorfuQueue<>(runtime, SNAPSHOT_PERSISTENT_QUEUE_NAME + snapshot);
        return true;
    }

    /**
     * read from persistent metadata to check it is the ongoing snapshot transfer.
     * If it is not the ongoing snapshot transfer discard the message
     * If the message's snapshot is not correct discard it.
     * Update queueRecvSeq number in persistent data
     * @param entry
     */
    void write(LogReplicationEntry entry) throws TransactionAbortedException {
        long entrySnapshot = entry.getMetadata().getSnapshotTimestamp();
        long entrySeqNumber = entry.getMetadata().getSnapshotSyncSeqNum();

        if (entry.getMetadata().getSnapshotTimestamp() != snapshot) {
            log.error(" entry {} timestamp is not equal to snapshot {} expecting", entry, snapshot);
            return;
        }
        // Start a transaction, write entry to the queue, update seqNumber
        try {
            runtime.getObjectsView().TXBegin();
            long snapshot = persistedWriterMetadata.getLastSnapStart();
            if (snapshot == entrySnapshot && persistedWriterMetadata.getLastSnapTransferDone() < snapshot &&
                    ((persistedWriterMetadata.getLastSnapSeqNum() + 1) == entrySeqNumber)) {
                corfuQueue.enqueue(entry);
                persistedWriterMetadata.setLastSnapSeqNum(entrySeqNumber);
            } else {
                log.warn("skip message {} writing to the corfuQueue according to the metadata snapStart {} snapTransferred {} snapSeqNum {}",
                        entry.getMetadata(), persistedWriterMetadata.getLastSnapStart(), persistedWriterMetadata.getLastSnapTransferDone(),
                        persistedWriterMetadata.getLastSnapSeqNum());
            }
        } catch (TransactionAbortedException e) {
            log.error("caught an exception {}", e);
        } finally {
            runtime.getObjectsView().TXEnd();
        }
    }

    /*public void apply(LogReplicationEntry message) {
        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Expecting sequencer {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getMessageMetadataType(), MessageType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        byte[] payload = message.getPayload();
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(payload));

        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }

        processOpaqueEntry(message, opaqueEntry);
        recvSeq++;
    }*/

    public void write(List<LogReplicationEntry> messages) throws Exception {
        for (LogReplicationEntry msg : messages) {
            try {
                write(msg);
            } catch (TransactionAbortedException e) {
                log.error("caught transaction aborted {}", e);
            }
        }
    }

    public void read() {

    }
}
