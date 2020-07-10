package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.LogEntrySender;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class implements the Log Replication Finite State Machine.
 *
 * CorfuDB provides a Log Replication functionality, which allows logs to be automatically replicated from a primary
 * to a remote cluster. This feature is particularly useful in the event of failure or data corruption, so the system
 * can failover to the standby/secondary data-store.
 *
 * This functionality is initiated by the application through the LogReplicationSourceManager on the primary cluster and handled
 * through the LogReplicationSinkManager on the destination cluster. This implementation assumes that the application provides its own
 * communication channels.
 *
 * Log Replication on the source cluster is defined by an event-driven finite state machine, with 5 states
 * and 8 events/messages---which can trigger the transition between states.
 *
 * States:
 * ------
 *  - Initialized (initial state)
 *  - In_Log_Entry_Sync
 *  - In_Snapshot_Sync
 *  - Snapshot_Sync_Required
 *  - Stopped
 *
 * Events:
 * ------
 *  - replication_start
 *  - replication_stop
 *  - snapshot_sync_request
 *  - snapshot_sync_complete
 *  - snapshot_sync_continue
 *  - sync_cancel
 *  - log_entry_sync_replicated
 *  - replication_shutdown
 *
 *
 * The following diagram illustrates the Log Replication FSM state transition:
 *
 *
 *                                       replication_stop
 *                      +-------------------------------------------------+
 *    replication_stop  |                                                 |
 *             +-----+  |              replication_stop                   |
 *             |     |  v      v-----------------------------+            |
 *             |    ++--+---------+                          |        +---+--------------------+
 *             +--->+ INITIALIZED +------------------------+ |        | SNAPSHOT_SYNC_REQUIRED +<---+
 *                  +---+----+----+ snapshot_sync_request  | |        +---+---------------+----+    |
 *                      ^    |                             | |            |               ^         |
 *                      |    |                             | |   snapshot |               |         |
 *                      |    |                             | |    sync    |               |         |
 *     replication_stop |    | replication_start           | |    request |               |         |
 *                      |    |                             | |            |               |         |
 *                      |    v                             v |            v               |         |
 *               +------+----+-------+  snapshot_sync    +-+-+------------+-+             |         |
 *         +-----| IN_LOG_ENTRY_SYNC |     request       | IN_SNAPSHOT_SYNC +             |         |
 *         |     |                   +------------------>+                  |             |         |
 *         |     +----+----+---------+                   +---+---+----------+-------------+         |
 *         |       ^  |   ^                                 |    |        ^        sync             |
 *         |       |  |   +---------------------------------+    |        |       cancel            |
 *         + ----- +  |                snapshot_sync             + -------+                         |
 *  log_entry_sync    |                  complete               snapshot_sync                       |
 *    continue        |                                           continue                          |
 *                    +-----------------------------------------------------------------------------+
 *                                                     sync_cancel
 *               replication
 * +---------+    shutdown    +------------+
 * | STOPPED +<---------------+ ALL_STATES |
 * +---------+                +------------+
 *
 *
 */
// TODO(Anny): insert new state to comply with SNAPSHOT_WAIT_COMPLETE event
@Slf4j
public class LogReplicationFSM {

    @Getter
    private long topologyConfigId;

    /**
     * Current state of the FSM.
     */
    @Getter
    private volatile LogReplicationState state;

    /**
     * Map of all Log Replication FSM States (reuse single instance for each state)
     */
    @Getter
    private Map<LogReplicationStateType, LogReplicationState> states = new HashMap<>();

    /**
     * Executor service for FSM state tasks (it can be shared across several LogReplicationFSMs)
     */
    @Getter
    private ExecutorService logReplicationFSMWorkers;

    /**
     * Executor service for FSM event queue consume
     */
    private ExecutorService logReplicationFSMConsumer;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * An observable object on the number of transitions of this state machine (for testing & visibility)
     */
    @VisibleForTesting
    @Getter
    private ObservableValue numTransitions = new ObservableValue(0);

    /**
     * Log Entry Reader (read incremental updated from Corfu Datatore)
     */
    private LogEntryReader logEntryReader;

    /**
     * Snapshot Reader (read data from Corfu Datastore)
     */
    private SnapshotReader snapshotReader;

    /**
     * Version on which snapshot sync is based on.
     */
    @Getter
    private long baseSnapshot = Address.NON_ADDRESS;

    /**
     * Acknowledged timestamp
     */
    @Getter
    private long ackedTimestamp = Address.NON_ADDRESS;

    /**
     * Log Entry Sender (send incremental updates to remote cluster)
     */
    private LogEntrySender logEntrySender;

    /**
     * Snapshot Sender (send snapshot cut to remote cluster)
     */
    private SnapshotSender snapshotSender;

    /**
     * Remote Cluster Descriptor to which this FSM drives the log replication
     */
    private final ClusterDescriptor remoteCluster;

    /**
     * Constructor for LogReplicationFSM, custom read processor for data transformation.
     *
     * @param runtime Corfu Runtime
     * @param config log replication configuration
     * @param remoteCluster remote cluster descriptor
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param readProcessor read processor for data transformation
     * @param workers FSM executor service for state tasks
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, ClusterDescriptor remoteCluster, DataSender dataSender,
                             ReadProcessor readProcessor, ExecutorService workers) {
        // Use stream-based readers for snapshot and log entry sync reads
        this(runtime, new StreamsSnapshotReader(runtime, config), dataSender,
                new StreamsLogEntryReader(runtime, config), readProcessor, config, remoteCluster, workers);

    }

    /**
     * Constructor for LogReplicationFSM, custom readers (as it is not expected to have custom
     * readers, this is used for FSM testing purposes only).
     *
     * @param runtime Corfu Runtime
     * @param snapshotReader snapshot logreader implementation
     * @param dataSender application callback for snapshot and log entry sync messages
     * @param logEntryReader log entry logreader implementation
     * @param readProcessor read processor (for data transformation)
     * @param remoteCluster remote cluster descriptor
     * @param workers FSM executor service for state tasks
     */
    @VisibleForTesting
    public LogReplicationFSM(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                             LogEntryReader logEntryReader, ReadProcessor readProcessor, LogReplicationConfig config,
                             ClusterDescriptor remoteCluster, ExecutorService workers) {

        this.snapshotReader = snapshotReader;
        this.logEntryReader = logEntryReader;
        this.remoteCluster = remoteCluster;

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and send data
        // through the callbacks provided by the application
        snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor, config.getSnapshotSyncBatchSize(), this);
        logEntrySender = new LogEntrySender(runtime, logEntryReader, dataSender, readProcessor, this);

        // Initialize Log Replication 5 FSM states - single instance per state
        initializeStates(snapshotSender, logEntrySender);

        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.logReplicationFSMWorkers = workers;
        this.logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer").build());

        logReplicationFSMConsumer.submit(this::consume);

        log.info("Log Replication FSM initialized, streams to replicate {} to remote cluster {}",
                config.getStreamsToReplicate(), remoteCluster.getClusterId());
    }

    /**
     * Initialize all states for the Log Replication FSM.
     *
     * @param snapshotSender reads and transmits snapshot syncs
     * @param logEntrySender reads and transmits log entry sync
     */
    private void initializeStates(SnapshotSender snapshotSender, LogEntrySender logEntrySender) {
        /*
         * Log Replication State instances are kept in a map to be reused in transitions, avoid creating one
          * per every transition (reduce GC cycles).
         */
        states.put(LogReplicationStateType.INITIALIZED, new InitializedState(this));
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, new InSnapshotSyncState(this, snapshotSender));
        states.put(LogReplicationStateType.IN_LOG_ENTRY_SYNC, new InLogEntrySyncState(this, logEntrySender));
        states.put(LogReplicationStateType.STOPPED, new StoppedState());
    }

    /**
     * Input function of the FSM.
     *
     * This method enqueues log replication events for further processing.
     *
     * @param event LogReplicationEvent to process.
     */
    public synchronized void input(LogReplicationEvent event) {
        try {
            if (state.getType().equals(LogReplicationStateType.STOPPED)) {
                // Log: not accepting events, in stopped state
                return;
            }
            if (event.getType() != LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE) {
                log.trace("Enqueue event {} with ID {}", event.getType(), event.getEventID());
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     *
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            if (state.getType() == LogReplicationStateType.STOPPED) {
                log.info("Log Replication State Machine has been stopped. No more events will be processed.");
                return;
            }

            // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)
            //   Block until an event shows up in the queue.
            log.trace("eventQue {} {}", eventQueue.size(), eventQueue);
            LogReplicationEvent event = eventQueue.take();

            log.trace("process event {}", event);

            if (event.getType() != LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE) {
                log.trace("Log Replication FSM consume event {}", event);
            }

            if (event.getType() == LogReplicationEventType.REPLICATION_START) {
                baseSnapshot = event.getMetadata().getSyncTimestamp();
                ackedTimestamp = baseSnapshot;
                log.info("Log Replication FSM consume event {}", event);
            }

            if (event.getType() == LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED) {
                if (state.getType() == LogReplicationStateType.IN_LOG_ENTRY_SYNC &&
                        state.getTransitionEventId().equals(event.getMetadata().getRequestId())) {
                    log.debug("Log Entry Sync ACK, update last ack timestamp to {}", event.getMetadata().getSyncTimestamp());
                    ackedTimestamp = event.getMetadata().getSyncTimestamp();
                    log.debug("Replicated Event ackTs {} ", ackedTimestamp);
                }
            } else {
                if (event.getType() == LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE) {
                    // Verify it's for the same request, as that request could've been canceled and was received later
                    if (state.getType() == LogReplicationStateType.IN_SNAPSHOT_SYNC &&
                            state.getTransitionEventId().equals(event.getMetadata().getRequestId())) {
                        log.info("Snapshot Sync ACK, update last ack timestamp to {}", event.getMetadata().getSyncTimestamp());
                        baseSnapshot = event.getMetadata().getSyncTimestamp();
                        ackedTimestamp = baseSnapshot;
                    }
                }

                try {
                    LogReplicationState newState = state.processEvent(event);
                    log.trace("Transition from {} to {}", state, newState);
                    transition(state, newState);
                    state = newState;
                    numTransitions.setValue(numTransitions.getValue() + 1);
                } catch (IllegalTransitionException illegalState) {
                    log.error("Illegal log replication event {} when in state {}", event.getType(), state.getType());
                }
            }

            //For testing purpose to notify the event generator the stop of the event.
            if (event.getType() == LogReplicationEventType.REPLICATION_STOP) {
                synchronized (event) {
                    event.notifyAll();
                }
            }

            // Consume one event in the queue and re-submit, this is done so events are consumed in
            // a round-robin fashion for the case of multi-cluster replication.
            logReplicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to final state
     */
    void transition(LogReplicationState from, LogReplicationState to) {
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }

    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
        snapshotReader.setTopologyConfigId(topologyConfigId);
        logEntryReader.setTopologyConfigId(topologyConfigId);
    }

    /**
     * Start consumer again due to cluster switch.
     * It will clean the queue first and prepare the new transfer
     *
     * @param topologyConfigId topology configuration identifier
     */
    public void startFSM(long topologyConfigId) {
        if (state.getType() != LogReplicationStateType.INITIALIZED) {
            log.error("The FSM is not in the correct state {}, expecting state {}",
                    state, LogReplicationStateType.INITIALIZED);
        }

        log.info("start the log replication with topologyConfigId {}", topologyConfigId);
        eventQueue.clear();
        setTopologyConfigId(topologyConfigId);
    }
}
