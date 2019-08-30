package org.corfudb.protocols.wireprotocol.logunit;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import java.util.Map;

@Getter
@Builder
@AllArgsConstructor
public class AddressMetaDataRangeMsg implements ICorfuPayload<AddressMetaDataRangeMsg> {
    private final Map<Long, AddressMetaDataMsg> addressMetaDataMap;

    public AddressMetaDataRangeMsg(ByteBuf buf){
        addressMetaDataMap = ICorfuPayload.mapFromBuffer(buf, Long.class, AddressMetaDataMsg.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addressMetaDataMap);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString(callSuper = false)
    public static class AddressMetaDataMsg implements ICorfuPayload<AddressMetaDataMsg>{
        public int checksum;
        public int length;
        public long offset;

        public AddressMetaDataMsg(ByteBuf buf){
            checksum = ICorfuPayload.fromBuffer(buf, Integer.class);
            length = ICorfuPayload.fromBuffer(buf, Integer.class);
            offset = ICorfuPayload.fromBuffer(buf, Long.class);
        }

        @Override
        public void doSerialize(ByteBuf buf) {
            ICorfuPayload.serialize(buf, checksum);
            ICorfuPayload.serialize(buf, length);
            ICorfuPayload.serialize(buf, offset);
        }
    }
}
