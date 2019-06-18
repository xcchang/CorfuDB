package org.corfudb.runtime.kv.core;

import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import org.corfudb.runtime.KvStore;

@AllArgsConstructor
public class DbValue<T extends Message> {
    private final KvStore.Value value;
    private final Class<T> payloadType;
}
