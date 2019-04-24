package org.corfudb.infrastructure.log.segment;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.file.Paths;

public class SegmentMetaDataTest {

    @Test
    public void testMetaData(){
        SegmentMetaData segment = new SegmentMetaData(0, 10_000, Paths.get(""));
        assertEquals(10_000, segment.getLatestAddress());
        assertEquals(0, segment.getFirstGlobalAddress());

        segment = new SegmentMetaData(1, 10_000, Paths.get(""));
        assertEquals(20_000, segment.getLatestAddress());

        segment = new SegmentMetaData(1, 10_000, Paths.get(""));
        assertEquals(10_000, segment.getFirstGlobalAddress());
    }

}