package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class PerformantBatchWriter<K, V> implements AutoCloseable {

    static final int BATCH_SIZE = 50;

    final boolean sync;

    private StreamLog streamLog;

    private PriorityBlockingQueue<AsyncWrite> writeQueue;

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("LogUnit-Write-Processor-%d")
            .build();

    @Getter
    private final ExecutorService writerService = Executors.newSingleThreadExecutor(threadFactory);

    /**
     * The sealEpoch is the epoch up to which all operations have been sealed. Any
     * BatchWriterOperation arriving after the sealEpoch with an epoch less than the sealEpoch
     * is completed exceptionally with a WrongEpochException.
     * This is persisted in the ServerContext by the LogUnitServer to withstand restarts.
     */
    private long sealEpoch;

    /**
     * Returns a new BatchWriter for a stream log.
     *
     * @param streamLog stream log for writes (can be in memory or file)
     * @param sealEpoch All operations stamped with epoch less than the epochWaterMark are
     *                  discarded.
     * @param streamLog stream log for writes (can be in memory or file)
     * @param sync      If true, the batch writer will sync writes to secondary storage
     */
    public PerformantBatchWriter(StreamLog streamLog, long sealEpoch, boolean sync) {
        this.sealEpoch = sealEpoch;
        this.sync = sync;
        this.streamLog = streamLog;
        writeQueue = new PriorityBlockingQueue<>();
        writerService.submit(this::batchProcessing);
    }

    public void asyncWrite(LogData logData, Consumer<CorfuMsg> callBack) {
        writeQueue.add(new AsyncWrite(logData, callBack));
    }

    private void batchProcessing() {

        if (!sync) {
            log.warn("batchWriteProcessor: writes configured to not sync with secondary storage");
        }

        int counter = 0;
        long start = System.currentTimeMillis();

        while (true) {
            List<AsyncWrite> asyncWrites = new ArrayList<>();

            AsyncWrite currentWrite = null;
            try {
                currentWrite = writeQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            asyncWrites.add(currentWrite);
            writeQueue.drainTo(asyncWrites, BATCH_SIZE - 1);

            List<LogData> data = new ArrayList<>();
            for (AsyncWrite write : asyncWrites) {
                data.add(write.logData);
            }

            try {
                counter += data.size();
                if (counter % 1000 == 0) {
                    System.out.println("Writes!!! " + counter + ", time: " + (System.currentTimeMillis() - start));
                }
                streamLog.append(data);
                streamLog.sync(sync);
                for (AsyncWrite write : asyncWrites) {
                    write.onCompleted();
                }
            } catch (Exception ex) {
                throw new IllegalStateException("yay");
            }
        }
    }

    @Override
    public void close() {
        writerService.shutdown();
    }

    @AllArgsConstructor
    private static class AsyncWrite implements Comparable<AsyncWrite> {
        private final LogData logData;
        private final Consumer<CorfuMsg> callBack;

        private void onCompleted() {
            callBack.accept(CorfuMsg.WRITE_OK);
        }

        private void onError(CorfuMsg message) {
            callBack.accept(message);
        }

        @Override
        public int compareTo(AsyncWrite other) {
            return Long.compare(logData.getGlobalAddress(), other.logData.getGlobalAddress());
        }
    }
}