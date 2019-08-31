package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;


@Getter
@Builder
@AllArgsConstructor
public class PiggyBack implements ICorfuPayload<PiggyBack> {

    private final String endpoint;

    public PiggyBack(ByteBuf buf){
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
    }
}