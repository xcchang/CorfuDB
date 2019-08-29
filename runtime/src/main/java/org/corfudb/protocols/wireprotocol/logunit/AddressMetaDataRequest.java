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
public class AddressMetaDataRequest implements ICorfuPayload<AddressMetaDataRangeMsg> {
    private final List<Long> addresses;


    public AddressMetaDataRequest(ByteBuf buf){
        addresses =  ICorfuPayload.listFromBuffer(buf, long.class);
    }
    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
    }
}
