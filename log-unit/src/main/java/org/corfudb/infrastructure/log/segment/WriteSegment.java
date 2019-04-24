package org.corfudb.infrastructure.log.segment;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.infrastructure.log.Types.StreamLogAddress;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

@Builder
public class WriteSegment implements Segment, Closeable {
    public static final int ADDRESS_LENGTH = StreamLogAddress.newBuilder()
            .setAddress(-1)
            .setOffset(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    @NonNull
    private final SegmentMetaData info;

    @NonNull
    private final FileChannel channel;

    public SegmentState initialState(){
        return new SegmentState(info);
    }

    private void write(StreamLogAddress address, byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(data.length + address.getSerializedSize());
        buffer.put(address.toByteArray());
        buffer.put(data);
        buffer.flip();

        channel.write(buffer);
    }

    public void write(long addr, byte[] data) {
        try {
            StreamLogAddress address = StreamLogAddress.newBuilder()
                    .setAddress(addr)
                    .setOffset(channel.position())
                    .setLength(ADDRESS_LENGTH + data.length)
                    .build();

            write(address, data);
        } catch (IOException ex) {
            throw new IllegalStateException("Can't write");
        }
    }

    public boolean contains(long globalAddress) {
        return info.contains(globalAddress);
    }

    public long getPosition() throws IOException {
        return channel.position();
    }

    public void fsync() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

}
