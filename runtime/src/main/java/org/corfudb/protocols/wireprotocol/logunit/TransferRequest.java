package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public class TransferRequest implements ICorfuPayload<TransferRequest> {

    private final String endpoint;
    private final int port;
    private final List<Long> addresses;

    public TransferRequest(ByteBuf buf){
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
        port = ICorfuPayload.fromBuffer(buf, int.class);
        addresses = ICorfuPayload.listFromBuffer(buf, long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
        ICorfuPayload.serialize(buf, port);
        ICorfuPayload.serialize(buf, addresses);
    }
}
