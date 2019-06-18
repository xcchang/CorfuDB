package org.corfudb.runtime.kv.core;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.corfudb.runtime.KvStore;
import org.corfudb.runtime.KvStore.Key;
import org.corfudb.runtime.KvStore.MessageSchema;

public class KvBuilder {

    private KvBuilder() {
        //
    }

    public static <T extends Message> DbKey<T> buildKey(T payload) {
        Key key = Key.newBuilder()
                .setPayload(Any.pack(payload))
                .setSchema(getMessageSchema(payload))
                .build();

        Class<T> payloadType = (Class<T>) payload.getClass();
        return new DbKey<>(key, payloadType);
    }

    public static <T extends Message> DbValue<T> buildValue(T payload) {
        KvStore.Value value = KvStore.Value.newBuilder()
                .setSchema(getMessageSchema(payload))
                .setPayload(Any.pack(payload))
                .build();

        Class<T> payloadType = (Class<T>) payload.getClass();

        return new DbValue<>(value, payloadType);
    }

    private static <T extends Message> MessageSchema getMessageSchema(T payload) {
        return MessageSchema.newBuilder()
                .setMetaInfo(payload.getDescriptorForType().toProto())
                .build();
    }
}
