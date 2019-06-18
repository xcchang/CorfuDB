package org.corfudb.runtime.kv.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.runtime.KvStore;

@AllArgsConstructor
public class DbKey<T extends Message> {
    @Getter
    private final KvStore.Key key;
    private final Class<T> payloadType;

    public void xxx() {
        //key.getSchema().getMetaInfo()
    }

    public Result<T, KvStoreException> payload() {
        return Result.of(() -> {
            try {
                return key.getPayload().unpack(payloadType);
            } catch (InvalidProtocolBufferException ex) {
                throw new KvStoreException(ex);
            }
        });
    }
}
