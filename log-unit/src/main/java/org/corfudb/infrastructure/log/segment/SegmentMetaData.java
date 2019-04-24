package org.corfudb.infrastructure.log.segment;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.file.Path;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class SegmentMetaData {
    private final long number;
    private final long size;
    private final Path dbDir;

    public Path getSegmentPath() {
        return dbDir.resolve(String.valueOf(number));
    }

    public long getLatestAddress() {
        if (number == 0) {
            return size;
        }

        return (number +1) * size;
    }

    public long getFirstGlobalAddress() {
        return number * size;
    }

    public boolean contains(long address) {
        return address < getLatestAddress() && address > getFirstGlobalAddress();
    }
}
