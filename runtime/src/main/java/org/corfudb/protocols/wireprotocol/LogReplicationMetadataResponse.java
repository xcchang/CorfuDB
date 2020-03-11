package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

public class LogReplicationMetadataResponse implements ICorfuPayload<LogReplicationMetadataResponse> {
    long snapshotEpoch;
    long lastSnapshotStart;
    long lastSnapTransferDone;
    long lastSnapApplyDone;
    long lastSnapSeqNum;
    long lastLogProcessed;


    public LogReplicationMetadataResponse(long epoch, long lastSnapshotStart, long lastSnapTransferDone,
                                          long lastSnapApplyDone, long lastSnapSeqNum, long lastLongProcessed) {
        this.snapshotEpoch = epoch;
        this.lastSnapshotStart = lastSnapshotStart;
        this.lastSnapTransferDone = lastSnapTransferDone;
        this.lastSnapApplyDone = lastSnapApplyDone;
        this.lastSnapSeqNum = lastSnapSeqNum;
        this.lastLogProcessed = lastLongProcessed;
    }

    public LogReplicationMetadataResponse(ByteBuf buf) {
        snapshotEpoch = ICorfuPayload.fromBuffer(buf, Long.class);
        lastSnapshotStart = ICorfuPayload.fromBuffer(buf, Long.class);
        lastSnapTransferDone = ICorfuPayload.fromBuffer(buf, Long.class);
        lastSnapApplyDone = ICorfuPayload.fromBuffer(buf, Long.class);
        lastSnapSeqNum = ICorfuPayload.fromBuffer(buf, Long.class);
        lastLogProcessed = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, snapshotEpoch);
        ICorfuPayload.serialize(buf, lastSnapshotStart);
        ICorfuPayload.serialize(buf, lastSnapTransferDone);
        ICorfuPayload.serialize(buf, lastSnapApplyDone);
        ICorfuPayload.serialize(buf, lastSnapSeqNum);
        ICorfuPayload.serialize(buf, lastLogProcessed);
    }
}