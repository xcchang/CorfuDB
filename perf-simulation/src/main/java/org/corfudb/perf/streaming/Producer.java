package org.corfudb.perf.streaming;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

@Slf4j
public class Producer extends Worker {

    /**
     * The payload to write to the stream on each "produce"
     */
    private final byte[] payload;

    public Producer(final UUID id, final CorfuRuntime runtime, long statusUpdateMs,
                    final int numItems, final byte[] payload) {
        super(id, runtime, statusUpdateMs, 0, numItems);
        this.payload = payload;
    }


    /**
     * Logs the number of tasks this producer completed so far.
     * @param itemsCompleted number of completed tasks
     */
    private void updateStatus(final int itemsCompleted) {
        final long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp - lastUpdateTimestamp > statusUpdateMs) {
            log.info("Producer[{}] completed {}", id, itemsCompleted);
            lastUpdateTimestamp = currentTimestamp;
        }
    }

    @Override
    public void run() {
        log.debug("Producer[{}] started", id);
        final long startTime = System.currentTimeMillis();
        final IStreamView stream = runtime.getStreamsView().get(id);

        for (int taskNum = 0; taskNum < numItems; taskNum++) {
            final long startTimestamp = System.nanoTime();
            stream.append(payload);
            recorder.recordValue(System.nanoTime() - startTimestamp);

            if (statusUpdateMs != 0) {
                updateStatus(taskNum);
            }
        }

        final double totalTimeInSeconds =  (System.currentTimeMillis() - startTime * 1.0) / 10e3;
        log.debug("Producer[{}] completed {} in {} seconds", id, numItems, totalTimeInSeconds);
    }
}
