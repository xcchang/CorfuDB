package org.corfudb.infrastructure.log.segment;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.Types.StreamLogAddress;
import org.corfudb.infrastructure.log.index.AddressIndex;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

@AllArgsConstructor
@Getter
public class ReadSegment implements Closeable {
    public static final int ADDRESS_LENGTH = StreamLogAddress.newBuilder()
            .setAddress(-1)
            .setOffset(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    @NonNull
    private final SegmentMetaData info;
    @NonNull
    private final AddressIndex index;
    @NonNull
    private final FileChannel channel;

    public ByteBuffer read(long position, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.read(buffer, position);
        return buffer;
    }

    public ReadSegment updateIndex() throws IOException {

        AddressIndex currIndex = index;
        while (true) {
            if (currIndex.isNonAddress() && channel.size() == 0) {
                return this;
            }

            long nextAddressPosition = 0;
            if (currIndex.isAddress()) {
                StreamLogAddress address = currIndex.getAddress();
                nextAddressPosition = address.getOffset() + address.getLength();

                if (address.getAddress() == info.getLatestAddress()) {
                    return this;
                }

                if (nextAddressPosition >= channel.size()) {
                    return this;
                }
            }

            //parse stream log address
            ByteBuffer nextAddressBuffer = read(nextAddressPosition, ADDRESS_LENGTH);
            StreamLogAddress nextAddress = StreamLogAddress.parseFrom(nextAddressBuffer.array());

            currIndex = currIndex.next(nextAddress);

            StreamLogAddress currAddress = currIndex.getAddress();
            if (currAddress.getOffset() + currAddress.getLength() >= channel.size()) {
                return new ReadSegment(info, currIndex, channel);
            }
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
