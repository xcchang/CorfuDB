package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

public class LogReplicationQueryLeadershipResponse implements ICorfuPayload<LogReplicationQueryLeadershipResponse>  {
    @Getter
    long epoch;

    @Getter
    String ipAddress;

    public LogReplicationQueryLeadershipResponse(long epoch, String ipAddress) {
        this.epoch = epoch;
        this.ipAddress = ipAddress;
    }

    public LogReplicationQueryLeadershipResponse (ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        ipAddress = ICorfuPayload.fromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, ipAddress);
    }
}
