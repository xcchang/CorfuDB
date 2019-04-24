package org.corfudb.infrastructure.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.infrastructure.log.Types.StreamLogAddress;
import org.corfudb.infrastructure.log.segment.Segment.SegmentState;
import org.corfudb.infrastructure.log.segment.SegmentManager.WriteSegmentManager;

import java.util.NavigableSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamLog {

    private final MemTable<StreamLogAddress> memTable;
    private final WriteSegmentManager writeSegmentManager;

    private final ExecutorService writerService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("LogUnit-Write-Processor-%d")
                    .build());

    public StreamLog(MemTable<StreamLogAddress> memTable, WriteSegmentManager writeSegmentManager) {
        this.memTable = memTable;
        this.writeSegmentManager = writeSegmentManager;
        this.writerService.submit(this::writeProcessor);
    }

    private void writeProcessor() {
        try {
            while (true) {
                SegmentState currentSegmentState;
                SegmentState nextSegmentState;

                switch (memTable.size()) {
                    case 0:
                    case 1:
                        StreamLogAddress address = memTable.getSingleEntry();
                        writeSegmentManager.write(address.getAddress(), address);

                        //check if current segment is completed
                        break;
                    default:
                        NavigableSet<StreamLogAddress> entries = memTable.get();
                        //
                }


                //save buffer
                //get write segment(s), check if we can save all records in one segment
                //save entries


                return;
            }
        } catch (Exception e) {
            writerService.shutdown();
        }
    }

}
