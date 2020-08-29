package org.corfudb.perf.streaming;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

@Slf4j
@AllArgsConstructor
public class Producer implements Runnable {

    /**
     * Id of this producer
     */
    private final UUID id;

    /**
     * Total number of items to produce
     */
    private final int numItems;

    /**
     * The payload to write to the stream on each "produce"
     */
    private final byte[] payload;

    /**
     * Runtime to use.
     */
    private final CorfuRuntime runtime;

    /**
     * How often to log this producers status (i.e. number of elements produced)
     */
    private final long statusUpdateMs;

    /**
     * Last time the status was updated
     */
    private long lastUpdateTimestamp;

    /**
     * Recorder to track latency stats
     */
    private final Recorder recorder = new Recorder(TimeUnit.HOURS.toMillis(1), 5);

    /**
     * Logs the number of tasks this producer completed so far.
     * @param itemsCompleted number of completed tasks
     */
    private void updateStatus(int itemsCompleted) {
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp - lastUpdateTimestamp > statusUpdateMs) {
            log.info("Producer[{}] completed {}", id, itemsCompleted);
        }
    }

    @Override
    public void run() {
        log.debug("starting {}", id);

        IStreamView stream = runtime.getStreamsView().get(id);

        for (int taskNum = 0; taskNum < numItems; taskNum++) {
            long startTimestamp = System.nanoTime();
            stream.append(payload);
            recorder.recordValue(System.nanoTime() - startTimestamp);

            if (statusUpdateMs != 0) {
                updateStatus(taskNum);
            }
        }
    }
}
