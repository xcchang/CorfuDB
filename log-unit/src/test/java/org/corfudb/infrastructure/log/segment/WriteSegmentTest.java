package org.corfudb.infrastructure.log.segment;

import org.corfudb.infrastructure.performance.log.DataGenerator;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WriteSegmentTest {

    @Test
    public void testWrite() throws IOException {
        SegmentMetaData metaData = new SegmentMetaData(0, 10_000, Paths.get("db_dir"));
        Files.deleteIfExists(metaData.getSegmentPath());

        WriteSegment writeSegment = SegmentManager.getWriteSegment(metaData);

        for (int addr = 0; addr < metaData.getLatestAddress(); addr++) {
            byte[] data = DataGenerator.generateData(128);
            writeSegment.write(addr, data);
        }
    }

}