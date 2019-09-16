package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by WenbinZhu on 9/6/19.
 */
public class StreamLogCompactorTest extends AbstractCorfuTest {

    private static final ISerializer SERIALIZER = Serializers.PRIMITIVE;

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR;
    }

    private ServerContextBuilder getNewContextBuilder() {
        String path = getDirPath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false);
    }

    private LogData getLogData(LogEntry entry, DataType dataType, IToken token) {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(entry, b);
        LogData ld = new LogData(dataType, b);
        ld.useToken(token);

        return ld;
    }

    @Test
    public void compactionSmokeTest() {
        ServerContext sc = getNewContextBuilder().build();
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // Write to at least two segments, second segment is protected (not compacted).
        final int numIter = StreamLogParams.RECORDS_PER_SEGMENT + 1;
        final UUID streamAId = CorfuRuntime.getStreamID("s1");
        final UUID streamBId = CorfuRuntime.getStreamID("s2");

        final long skipAddress = 25L;
        List<LogData> writeEntries = new ArrayList<>();
        List<LogData> garbageEntries = new ArrayList<>();
        for (long i = 0; i < numIter; i++) {
            SMRLogEntry smrLogEntry = new SMRLogEntry();
            SMRRecord recordA = new SMRRecord("put", new Object[]{"a", String.valueOf(i)}, SERIALIZER);
            SMRRecord recordB = new SMRRecord("put", new Object[]{"b", String.valueOf(i)}, SERIALIZER);
            SMRRecord recordC = new SMRRecord("put", new Object[]{"c", String.valueOf(i)}, SERIALIZER);

            smrLogEntry.addTo(streamAId, recordA);
            smrLogEntry.addTo(streamAId, recordB);
            smrLogEntry.addTo(streamBId, recordC);

            Map<UUID, Long> backpointerMap = new HashMap<>();
            backpointerMap.put(streamAId, i == 0 ? Address.NON_EXIST : i - 1);
            backpointerMap.put(streamBId, i == 0 ? Address.NON_EXIST : i - 1);
            TokenResponse token = new TokenResponse(new Token(-1, i), backpointerMap);
            writeEntries.add(getLogData(smrLogEntry, DataType.DATA, token));

            SMRGarbageEntry garbageEntry = new SMRGarbageEntry();
            if (i == skipAddress) {
                garbageEntry.add(streamAId, 0, new SMRGarbageRecord(Address.NON_ADDRESS, recordA.getSerializedSize()));
                garbageEntry.add(streamBId, 0, new SMRGarbageRecord(Address.NON_ADDRESS, recordC.getSerializedSize()));
            } else {
                garbageEntry.add(streamAId, 0, new SMRGarbageRecord(Address.NON_ADDRESS, recordA.getSerializedSize()));
                garbageEntry.add(streamAId, 1, new SMRGarbageRecord(Address.NON_ADDRESS, recordB.getSerializedSize()));
                garbageEntry.add(streamBId, 0, new SMRGarbageRecord(Address.NON_ADDRESS, recordC.getSerializedSize()));
            }
            garbageEntries.add(getLogData(garbageEntry, DataType.GARBAGE, new Token(-1, i)));
        }

        log.append(writeEntries);
        log.append(garbageEntries);
        log.sync(true);

        // Fist compact() will only set compaction upper bound if using SnapshotLengthFirstPolicy.
        log.getCompactor().compact();
        log.getCompactor().compact();

        // Verify the address at skipAddress is partially compacted.
        LogData ld = log.read(skipAddress);
        SMRLogEntry entry = (SMRLogEntry) ld.getPayload(null);
        SMRRecord recordB = new SMRRecord("put", new Object[]{"b", String.valueOf(skipAddress)}, SERIALIZER);
        recordB.setGlobalAddress(skipAddress);
        assertThat(entry.getStreams().size()).isEqualTo(1);
        assertThat(entry.getSMRUpdates(streamAId)).isEqualTo(Arrays.asList(SMRRecord.COMPACTED_RECORD, recordB));

        // Verify reading a compacted address can return a compacted LogData.
        for (long i = 0; i < numIter; i++) {
            if (i != skipAddress && i < StreamLogParams.RECORDS_PER_SEGMENT) {
                assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
            } else {
                assertThat(log.read(i).getType()).isEqualTo(DataType.DATA);
            }
        }
    }
}
