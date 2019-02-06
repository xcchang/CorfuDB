package org.corfudb.infrastructure.log;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.format.Types;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PerfTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ServerContext getContext() {
        String path = folder.getRoot().getAbsolutePath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

    @Test
    public void testWriteReadPerformance() {
        perf();
    }

    private void perf() {
        LogData entry = buildLogData();
        StreamLog log = new StreamLogFiles(getContext(), false);

        List<LogData> data = new ArrayList<>();
        /*for (int i = 0; i < 10000; i++) {
            byte[] currstreamEntry = RandomStringUtils.randomAlphanumeric(10000).getBytes();
            ByteBuf bb = Unpooled.buffer();
            Serializers.CORFU.serialize(currstreamEntry, bb);
            LogData currentry = new LogData(DataType.DATA, bb);
            data.add(currentry);
        }*/

        long addr = 0;
        for (int i = 0; i < 1000; i++) {
            // Enable checksum, then append and read the same entry
            addr = i;

            log.append(addr, entry);
            //assertThat(log.read(i).getPayload(null)).isEqualTo(streamEntry);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            // Enable checksum, then append and read the same entry
            addr++;

            log.append(addr, entry);
            //assertThat(log.read(i).getPayload(null)).isEqualTo(streamEntry);
        }

        System.out.println("time: " + (System.currentTimeMillis() - start));
    }

    private LogData buildLogData() {
        byte[] streamEntry = "Payload".getBytes();
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(streamEntry, b);
        return new LogData(DataType.DATA, b);
    }

    @Test
    public void testTest() throws IOException, InterruptedException {
        final File logDir = folder.newFolder("stream");
        String entry = "ai_la_le_la_le_yay"; //buildLogData();

        MySegment currSegment = warmUp(logDir, entry);

        long startStart = System.currentTimeMillis();

        //10k for a segment
        long start = System.currentTimeMillis();
        long maxTime = 0;
        int amount = 1000000;

        for (int address = 10000; address < amount; address++) {
            long segment = address / 10000;

            if (segment > currSegment.segment) {
                long segmentTime = System.currentTimeMillis();
                String filePath = logDir.getAbsolutePath();
                filePath += segment;
                filePath += ".log";

                EnumSet<StandardOpenOption> options = EnumSet.of(
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.SPARSE
                );

                Path path = FileSystems.getDefault().getPath(filePath);
                System.out.println("Stream: " + path.toString());
                FileChannel channel = FileChannel.open(path, options);

                currSegment.channel.close();

                currSegment = new MySegment(segment, channel);

                //System.out.println("Segment time: " + (System.currentTimeMillis() - segmentTime));
            }

            long currRecordTime = System.nanoTime();
            currSegment.append(address, entry);
            long currEnd = System.nanoTime();
            maxTime = Math.max(maxTime, (currEnd - currRecordTime));

            if (address % 100000 == 0) {
                long end = System.currentTimeMillis();
                System.err.println("address: " + (address / 1000) + ", ts: " + (end - start));

                start = System.currentTimeMillis();
            }
        }

        long endEnd = System.currentTimeMillis();
        long totalTime = endEnd - startStart;
        System.err.println("Total: " + totalTime);
        System.err.println("Max time for a record: " + TimeUnit.NANOSECONDS.toMillis(maxTime));
        System.err.println("Perf: " + (amount / totalTime * 1000) + " per seconds");

        //Thread.sleep(100000000);
    }

    private MySegment warmUp(File logDir, String entry) throws IOException {
        MySegment currSegment = null;
        for (int address = 0; address < 100000; address++) {
            long segment = address / 10000;

            if (currSegment == null || segment > currSegment.segment) {
                String filePath = logDir.getAbsolutePath();
                filePath += segment;
                filePath += ".log";

                EnumSet<StandardOpenOption> options = EnumSet.of(
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE, StandardOpenOption.SPARSE
                );

                Path path = FileSystems.getDefault().getPath(filePath);
                FileChannel channel = FileChannel.open(path, options);

                if (currSegment != null) {
                    currSegment.channel.close();
                }

                currSegment = new MySegment(segment, channel);
            }

            currSegment.append(address, entry);
        }
        return currSegment;
    }

    @AllArgsConstructor
    static class MySegment {
        @Getter
        private final long segment;

        @NonNull
        @Getter
        private final FileChannel channel;

        public void append(long address, String entry) throws IOException {
            writeRecord(channel, address, entry);
        }

        private void writeRecord(FileChannel channel, long address, String entry) throws IOException {
            //LogEntry logEntry = getLogEntry(address, entry);
            ByteBuffer record = getByteBufferFromObj(entry);

            safeWrite(channel, record);
        }

        private static void safeWrite(FileChannel channel, ByteBuffer buf) throws IOException {
            channel.write(buf);
        }

        private LogEntry getLogEntry(long address, LogData entry) {
            byte[] data = new byte[0];

            if (entry.getData() != null) {
                data = entry.getData();
            }

            LogEntry.Builder logEntryBuilder = LogEntry.newBuilder()
                    .setDataType(Types.DataType.forNumber(entry.getType().ordinal()))
                    .setData(ByteString.copyFrom(data))
                    .setGlobalAddress(address)
                    .addAllStreams(getStrUUID(entry.getStreams()))
                    .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()));

            Optional<Types.DataRank> rank = createProtobufsDataRank(entry);
            rank.ifPresent(logEntryBuilder::setRank);

            if (entry.getClientId() != null && entry.getThreadId() != null) {
                logEntryBuilder.setClientIdMostSignificant(
                        entry.getClientId().getMostSignificantBits());
                logEntryBuilder.setClientIdLeastSignificant(
                        entry.getClientId().getLeastSignificantBits());
                logEntryBuilder.setThreadId(entry.getThreadId());
            }

            if (entry.hasCheckpointMetadata()) {
                logEntryBuilder.setCheckpointEntryType(
                        Types.CheckpointEntryType.forNumber(
                                entry.getCheckpointType().ordinal()));
                logEntryBuilder.setCheckpointIdMostSignificant(
                        entry.getCheckpointId().getMostSignificantBits());
                logEntryBuilder.setCheckpointIdLeastSignificant(
                        entry.getCheckpointId().getLeastSignificantBits());
                logEntryBuilder.setCheckpointedStreamIdLeastSignificant(
                        entry.getCheckpointedStreamId().getLeastSignificantBits());
                logEntryBuilder.setCheckpointedStreamIdMostSignificant(
                        entry.getCheckpointedStreamId().getMostSignificantBits());
                logEntryBuilder.setCheckpointedStreamStartLogAddress(
                        entry.getCheckpointedStreamStartLogAddress());
            }

            return logEntryBuilder.build();
        }

        private Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
            Map<String, Long> stringLongMap = new HashMap<>();

            for (Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
                stringLongMap.put(entry.getKey().toString(), entry.getValue());
            }

            return stringLongMap;
        }

        private Set<String> getStrUUID(Set<UUID> uuids) {
            Set<String> strUUIds = new HashSet<>();

            for (UUID uuid : uuids) {
                strUUIds.add(uuid.toString());
            }

            return strUUIds;
        }

        private Optional<Types.DataRank> createProtobufsDataRank(IMetadata entry) {
            IMetadata.DataRank rank = entry.getRank();
            if (rank == null) {
                return Optional.empty();
            }
            Types.DataRank result = Types.DataRank.newBuilder()
                    .setRank(rank.getRank())
                    .setUuidLeastSignificant(rank.getUuid().getLeastSignificantBits())
                    .setUuidMostSignificant(rank.getUuid().getMostSignificantBits())
                    .build();
            return Optional.of(result);
        }

        private static ByteBuffer getByteBuffer(AbstractMessage message) {
            ByteBuffer buf = ByteBuffer.allocate(message.getSerializedSize());
            buf.put(message.toByteArray());
            buf.flip();
            return buf;
        }

        private static ByteBuffer getByteBufferFromObj(String obj) {
            return ByteBuffer.wrap(obj.getBytes());
        }
    }
}
