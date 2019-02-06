package org.corfudb.infrastructure.log;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@AllArgsConstructor
class SegmentHandle {
    @Getter
    final long segment;

    @NonNull
    @Getter
    final FileChannel writeChannel;

    @NonNull
    @Getter
    final FileChannel readChannel;

    @NonNull
    @Getter
    String fileName;

    @Getter
    private final ConcurrentMap<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    public void close() {
        IOUtils.closeQuietly(writeChannel);
        IOUtils.closeQuietly(readChannel);
    }

    void updateKnownAddresses(Long address, AddressMetaData metaData) {
        knownAddresses.put(address, metaData);
    }

    void updateKnownAddresses(Map<Long, AddressMetaData> knownAddresses) {
        this.knownAddresses.putAll(knownAddresses);
    }

    public boolean contains(Long globalAddress) {
        return knownAddresses.containsKey(globalAddress);
    }
}