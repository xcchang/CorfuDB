package org.corfudb.perf.streaming;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;

@AllArgsConstructor
public abstract class Worker implements Runnable {

    /**
     * Id of this worker
     */
    protected final UUID id;

    /**
     * Runtime to use.
     */
    protected final CorfuRuntime runtime;

    /**
     * How often to log this workers status (i.e. number of tasks it completed)
     */
    protected final long statusUpdateMs;

    /**
     * Last time the status was updated
     */
    protected long lastUpdateTimestamp;

    /**
     * Recorder to track latency stats
     */
    protected final Recorder recorder = new Recorder(TimeUnit.HOURS.toMillis(1), 5);

    /**
     * Total number of items to produce
     */
    protected final int numItems;

}
