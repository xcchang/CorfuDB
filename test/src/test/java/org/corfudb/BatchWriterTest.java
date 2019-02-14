package org.corfudb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchWriter;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.PerformantBatchWriter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class BatchWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ServerContextBuilder serverContextBuilder = new ServerContextBuilder();

    private ServerContext serverContext;
    private StreamLogFiles streamLog;

    private ServerContext getContext() {
        String path = folder.getRoot().getAbsolutePath();
        return serverContextBuilder
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

    @Test
    public void oldPerf() throws InterruptedException {
        serverContext = getContext();
        streamLog = new StreamLogFiles(serverContext, true);

        BatchWriter<Long, LogData> batchWriter = new BatchWriter<>(streamLog, 0, true);

        LogUnitServerConfig luCfg = LogUnitServerConfig.parse(serverContext.getServerConfig());

        /**LoadingCache<Long, LogData> dataCache = Caffeine.newBuilder()
         .<Long, LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().length)
         .maximumWeight(luCfg.getMaxCacheSize())
         //.removalListener(this::handleEviction)
         .writer(batchWriter)
         .build(this::handleRetrieval);**/

        final long start = System.currentTimeMillis();
        final int totalRecords = 1000000;
        for (int i = 0; i < totalRecords; i++) {
            // Enable checksum, then append and read the same entry
            final long addr = i;
            LogData entry = buildLogData(addr);

            batchWriter.write(addr, entry);
        }

        long total = System.currentTimeMillis() - start;
        System.out.println("time: " + total);
        //System.out.println("Speed: " + 100000 / streamLog.avgTime.stream().mapToLong(l -> l).sum() * 1000);

        final int timeout = 100000000;
        Thread.sleep(timeout);
    }

    @Test
    public void perf() throws InterruptedException {
        serverContext = getContext();
        streamLog = new StreamLogFiles(serverContext, true);

        //BatchWriter<Long, LogData> batchWriter = new BatchWriter<>(streamLog, 0, true);
        PerformantBatchWriter<Long, LogData> batchWriter = new PerformantBatchWriter<>(streamLog, 0, true);

        LogUnitServerConfig luCfg = LogUnitServerConfig.parse(serverContext.getServerConfig());

        /**LoadingCache<Long, LogData> dataCache = Caffeine.newBuilder()
         .<Long, LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().length)
         .maximumWeight(luCfg.getMaxCacheSize())
         //.removalListener(this::handleEviction)
         .writer(batchWriter)
         .build(this::handleRetrieval);**/

        long start = System.currentTimeMillis();

        final AtomicLong records = new AtomicLong();

        final int totalRecords = 1000000;
        for (int i = 0; i < totalRecords; i++) {
            // Enable checksum, then append and read the same entry
            final long addr = i;
            LogData entry = buildLogData(addr);

            batchWriter.asyncWrite(entry, op -> {
                //ignore
                records.incrementAndGet();
            });
        }

        while (records.get() < totalRecords) {
            final int timeout = 100;
            Thread.sleep(timeout);
        }

        batchWriter.close();
        System.out.println("Wait for termination");
        batchWriter.getWriterService().awaitTermination(50, TimeUnit.MILLISECONDS);

        long total = System.currentTimeMillis() - start;
        System.out.println("time: " + total);
        //System.out.println("Speed: " + totalRecords / streamLog.avgTime.stream().mapToLong(l -> l).sum() * 1000);

        final int timeout = 100000000;
        Thread.sleep(timeout);
    }

    private static final byte[] streamEntry = ("PayloadPayloadPayloadPayloadPayloadPayloadPayloadPayloadPayloadPay")
            .getBytes();
    private static final ByteBuf b = Unpooled.buffer();

    static {
        Serializers.CORFU.serialize(streamEntry, b);
    }

    private LogData buildLogData(long address) {
        LogData data = new LogData(DataType.DATA, b);
        data.setEpoch(0L);
        data.setGlobalAddress(address);

        return data;
    }

    private synchronized LogData handleRetrieval(long address) {
        LogData entry = streamLog.read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }
}