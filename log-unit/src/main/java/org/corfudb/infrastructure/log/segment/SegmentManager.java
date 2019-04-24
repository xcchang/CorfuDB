package org.corfudb.infrastructure.log.segment;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.Types.StreamLogAddress;
import org.corfudb.infrastructure.log.index.AddressIndex;
import org.corfudb.infrastructure.log.segment.Segment.SegmentState;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Optional;

@Slf4j
public class SegmentManager {

    public static ReadSegment openForRead(SegmentMetaData info, AddressIndex index) throws IOException {
        FileChannel channel = getReadChannel(info);
        return new ReadSegment(info, index, channel);
    }

    public static FileChannel getReadChannel(SegmentMetaData info) throws IOException {
        EnumSet<StandardOpenOption> options = EnumSet.of(READ, CREATE, SPARSE);

        Path segmentFile = info.getSegmentPath();
        return FileChannel.open(segmentFile, options);
    }

    public static WriteSegment getWriteSegment(SegmentMetaData info) throws IOException {
        EnumSet<StandardOpenOption> options = EnumSet.of(WRITE, CREATE, SPARSE, APPEND);

        Path segmentFile = info.getSegmentPath();
        FileChannel channel = FileChannel.open(segmentFile, options);
        return WriteSegment.builder()
                .info(info)
                .channel(channel)
                .build();
    }

    public static class WriteSegmentManager {
        private final WriteSegment current;
        private final Optional<WriteSegment> next;

        private WriteSegmentManager(WriteSegment current, Optional<WriteSegment> next) {
            this.current = current;
            this.next = next;
        }

        public static WriteSegmentManager build(long address, int segmentSize){
            SegmentMetaData info = ;
            WriteSegment current = getWriteSegment(info);
            return new WriteSegmentManager(current, Optional.empty());
        }

        public WriteSegmentManager next() {
            return new WriteSegmentManager(this.next, nextSegmentState);
        }

        public void write(long address, StreamLogAddress data) {
            WriteSegment segment;

            if (current.contains(address)) {
                segment = current;
            } else if (next.contains(address)) {
                segment = next;
            } else {
                throw new IllegalStateException("Invalid address, out of limitation. Address: " + address);
            }

            segment.write(address, data.toByteArray());
        }

        public SegmentState currentSegmentInitialState(){
            return current.initialState();
        }
    }
}
