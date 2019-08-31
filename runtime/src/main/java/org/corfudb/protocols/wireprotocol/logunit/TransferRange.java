package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import java.util.List;

@AllArgsConstructor
@Getter
@Builder
public class TransferRange implements ICorfuPayload<TransferRange> {
    private final List<Long> addresses;
    private final String currentEndpoint;
    private final String endpoint;

    public TransferRange(ByteBuf buf){
        addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
        currentEndpoint = ICorfuPayload.fromBuffer(buf, String.class);
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
    }
    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        ICorfuPayload.serialize(buf, currentEndpoint);
        ICorfuPayload.serialize(buf, endpoint);
    }


}
