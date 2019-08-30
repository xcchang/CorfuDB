package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;



@Getter
@Builder
@AllArgsConstructor
public class TransferQueryResponse implements ICorfuPayload<TransferQueryResponse> {
    private final boolean active;

    public TransferQueryResponse(ByteBuf buf){
        active = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, Boolean.class);
    }
}
