package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by mwei on 8/9/16.
 */
@Builder
@AllArgsConstructor
public class WriteRequest implements ICorfuPayload<WriteRequest>, IMetadata {

    @Getter
    final WriteMode writeMode;

    @Getter
    final ILogData data;

    @Getter
    Map<String, String> traceCtx = Collections.emptyMap();

    @SuppressWarnings("unchecked")
    public WriteRequest(ByteBuf buf) {
        writeMode = ICorfuPayload.fromBuffer(buf, WriteMode.class);
        data = ICorfuPayload.fromBuffer(buf, LogData.class);
        traceCtx = ICorfuPayload.mapFromBuffer(buf, String.class, String.class);
    }

    public WriteRequest(WriteMode writeMode, Map<UUID, Long> streamAddresses, ByteBuf buf) {
        this(writeMode, DataType.DATA, streamAddresses, buf);
    }

    public WriteRequest(WriteMode writeMode, DataType dataType, Map<UUID, Long> streamAddresses,
                        ByteBuf buf) {
        this.writeMode = writeMode;
        this.data = new LogData(dataType, buf);
    }

    public WriteRequest(ILogData data, Map<String, String> traceCtx) {
        this.traceCtx = traceCtx;
        writeMode = WriteMode.NORMAL;
        this.data = data;


    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, writeMode);
        ICorfuPayload.serialize(buf, data);
        ICorfuPayload.serialize(buf, traceCtx);
    }

    @Override
    public EnumMap<LogUnitMetadataType, Object> getMetadataMap() {
        return data.getMetadataMap();
    }
}
