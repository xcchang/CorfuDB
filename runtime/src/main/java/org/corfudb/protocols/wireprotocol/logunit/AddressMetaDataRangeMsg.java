package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import java.util.Map;

@Getter
@Builder
@AllArgsConstructor
public class AddressMetaDataRangeMsg implements ICorfuPayload<AddressMetaDataRangeMsg> {
    private final Map<Long, AddressMetaDataMsg> addressMetaDataMap;

    public AddressMetaDataRangeMsg(ByteBuf buf){
        addressMetaDataMap = ICorfuPayload.mapFromBuffer(buf, long.class, AddressMetaDataMsg.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addressMetaDataMap);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class AddressMetaDataMsg implements ICorfuPayload<AddressMetaDataMsg>{
        public int checksum;
        public int length;
        public long offset;

        public AddressMetaDataMsg(ByteBuf buf){
            checksum = ICorfuPayload.fromBuffer(buf, int.class);
            length = ICorfuPayload.fromBuffer(buf, int.class);
            offset = ICorfuPayload.fromBuffer(buf, long.class);
        }

        @Override
        public void doSerialize(ByteBuf buf) {
            ICorfuPayload.serialize(buf, checksum);
            ICorfuPayload.serialize(buf, length);
            ICorfuPayload.serialize(buf, offset);
        }
    }
}
