package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import org.corfudb.runtime.CorfuStoreMetadata.RecordMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Created by zlokhandwala on 2019-08-15.
 */
@Getter
@ToString
@AllArgsConstructor
public class CorfuRecord<V extends Message> {
    private final V payload;
    private final RecordMetadata metadata;
}
