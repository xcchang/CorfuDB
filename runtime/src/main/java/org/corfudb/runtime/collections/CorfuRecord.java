package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Data;
import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;

/**
 * Created by zlokhandwala on 2019-08-15.
 */
@Data
public class CorfuRecord<V extends Message> {
    private final V payload;
    private final RecordMetadata metadata;
}
