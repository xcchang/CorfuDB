package org.corfudb.infrastructure.log.segment;

import static org.junit.Assert.assertEquals;

import org.corfudb.infrastructure.log.index.AddressIndex;
import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
import org.corfudb.infrastructure.performance.log.DataGenerator;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ReadSegmentTest {

    @Test
    public void testUpdateIndex() throws IOException {
        //write segment
        SegmentMetaData metaData = new SegmentMetaData(0, 10_000, Paths.get("db_dir"));
        writeData(metaData);

        FileChannel readChannel = SegmentManager.getReadChannel(metaData);
        AddressIndex index = NonAddressIndex.NON_ADDRESS;

        ReadSegment segment = new ReadSegment(metaData, index, readChannel);
        segment = segment.updateIndex();

        assertEquals(99, segment.getIndex().getAddress().getAddress());
    }

    private void writeData(SegmentMetaData metaData) throws IOException {
        Files.deleteIfExists(metaData.getSegmentPath());

        WriteSegment writeSegment = SegmentManager.getWriteSegment(metaData);
        for (int addr = 0; addr < 100; addr++) {
            byte[] data = DataGenerator.generateData(128);
            writeSegment.write(addr, data);
        }
    }

    @Test
    public void testReadEntry() throws IOException {
        SegmentMetaData metaData = new SegmentMetaData(0, 10_000, Paths.get("db_dir"));
        FileChannel channel = SegmentManager.getReadChannel(metaData);
        AddressIndex index = NonAddressIndex.NON_ADDRESS;

        ReadSegment segment = new ReadSegment(metaData, index, channel);
    }

}