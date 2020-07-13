package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;
import java.util.UUID;

/**
 * This message represents a log entry to be replicated across remote sites. It is also used
 * as an ACK for a replicated entry, where the payload is empty.
 *
 * @author annym
 */
@Data
public class LogReplicationAckMessage implements ICorfuPayload<LogReplicationAckMessage> {

    private LogReplicationEntryMetadata metadata;

    public LogReplicationAckMessage(LogReplicationEntryMetadata metadata) {
        this.metadata = metadata;
    }

    public LogReplicationAckMessage(MessageType type, long epoch, UUID syncRequestId, long entryTS, long preTS, long snapshot, long sequence) {
        this.metadata = new LogReplicationEntryMetadata(type, epoch, syncRequestId, entryTS, preTS, snapshot, sequence);
    }

    public LogReplicationAckMessage(ByteBuf buf) {
        metadata = ICorfuPayload.fromBuffer(buf, LogReplicationEntryMetadata.class);
    }

    public static LogReplicationAckMessage generateAck(LogReplicationEntryMetadata metadata) {
        return new LogReplicationAckMessage(metadata);
    }

    public static LogReplicationAckMessage fromProto(Messages.LogReplicationEntry proto) {
        LogReplicationEntryMetadata metadata = LogReplicationEntryMetadata.fromProto(proto.getMetadata());
        return new LogReplicationAckMessage(metadata);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, metadata);
    }
}