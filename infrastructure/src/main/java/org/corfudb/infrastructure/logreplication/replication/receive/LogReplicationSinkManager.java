package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.immutables.value.internal.$guava$.annotations.$VisibleForTesting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the Log Replication Manager at the destination.
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class LogReplicationSinkManager implements DataReceiver {
    /*
     * Read SinkManager configuration information from a file.
     * If the file is not available, use the default values.
     */
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";

    private final int DEFAULT_ACK_CNT = 1;
    /*
     * Duration in milliseconds after which an ACK is sent back to the sender
     * if the message count is not reached before
     */
    private int ackCycleTime = DEFAULT_ACK_CNT;

    /*
     * Number of messages received before sending a summarized ACK
     */
    private int ackCycleCnt;

    private int bufferSize;

    private CorfuRuntime runtime;

    private LogEntrySinkBufferManager logEntrySinkBufferManager;
    private SnapshotSinkBufferManager snapshotSinkBufferManager;

    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;
    private RxState rxState;

    private LogReplicationConfig config;

    private long baseSnapshotTimestamp = Address.NON_ADDRESS - 1;

    /*
     * Current topologyConfigId, used to drop out of date messages.
     */
    private long topologyConfigId = 0;

    @$VisibleForTesting
    private int rxMessageCounter = 0;

    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private ObservableValue rxMessageCount = new ObservableValue(rxMessageCounter);

    private ISnapshotSyncPlugin snapshotSyncPlugin;

    private String pluginConfigFilePath;

    // true indicates data is consistent on the local(standby) cluster, false indicates it is not.
    // In Snapshot Sync, if the StreamsSnapshotWriter is in the apply phase, the data is not yet
    // consistent and cannot be read by applications.  Data is always consistent during Log Entry Sync
    private AtomicBoolean dataConsistent = new AtomicBoolean(false);

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     * @param metadataManager
     * @param context
     */
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager,
                                     ServerContext context, long topologyConfigId) {

        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .trustStore((String) context.getServerConfig().get("--truststore"))
                .tsPasswordFile((String) context.getServerConfig().get("--truststore-password-file"))
                .keyStore((String) context.getServerConfig().get("--keystore"))
                .ksPasswordFile((String) context.getServerConfig().get("--keystore-password-file"))
                .tlsEnabled((Boolean) context.getServerConfig().get("--enable-tls"))
                .build())
                .parseConfigurationString(localCorfuEndpoint).connect();
        this.pluginConfigFilePath = context.getPluginConfigFilePath();
        this.topologyConfigId = topologyConfigId;
        initCommonParams(metadataManager, config);
    }

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     */
    @VisibleForTesting
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager, String pluginConfigFilePath) {
        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(localCorfuEndpoint).connect();
        this.pluginConfigFilePath = pluginConfigFilePath;
        initCommonParams(metadataManager, config);
    }

    private void initCommonParams(LogReplicationMetadataManager metadataManager, LogReplicationConfig config) {
        this.logReplicationMetadataManager = metadataManager;
        this.config = config;

        // When the server is up, it will be at LOG_ENTRY_SYNC state by default.
        // The sender will query receiver's status and decide what type of replication to start with.
        // It will transit to SNAPSHOT_SYNC state if it received a SNAPSHOT_START message from the sender.
        this.rxState = RxState.LOG_ENTRY_SYNC;

        // Set the data consistent status.
        setDataConsistent(dataConsistent.get());
        initWriterAndBufferMgr();
    }

    /**
     * Init the writers, Buffer Manager and Snapshot Plugin.
     */
    private void initWriterAndBufferMgr() {
        // Read config first before init other components.
        readConfig();

        // Instantiate Snapshot Sync Plugin, this is an external service which will be triggered on start and end
        // of a snapshot sync.
        snapshotSyncPlugin = getSnapshotPlugin();

        snapshotWriter = new StreamsSnapshotWriter(runtime, config, logReplicationMetadataManager);
        logEntryWriter = new LogEntryWriter(runtime, config, logReplicationMetadataManager);
        logEntryWriter.reset(logReplicationMetadataManager.getLastAppliedSnapshotTimestamp(),
                logReplicationMetadataManager.getLastProcessedLogEntryTimestamp());

        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastProcessedLogEntryTimestamp(), this);
    }

    private ISnapshotSyncPlugin getSnapshotPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getSnapshotSyncPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getSnapshotSyncPluginCanonicalName(), true, child);
            return (ISnapshotSyncPlugin) plugin.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Snapshot Sync Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Read the SinkManager configuration, such as buffer size and how frequent to send ACKs.
     * With changing this config file, we can do more testing to find the most optimal's way to for the setup.
     * If the configFile doesn't exist, use the default values.
     */
    private void readConfig() {
        File configFile = new File(config_file);
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            bufferSize = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(bufferSize)));
            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(ackCycleCnt)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(ackCycleTime)));
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", config_file);
        } catch (IOException e) {
            log.error("IO Exception when reading config file", e);
        }
        log.info("Sink Manager Buffer config queue size {} ackCycleCnt {} ackCycleTime {}",
                bufferSize, ackCycleCnt, ackCycleTime);
    }

    /**
     * Receive a message from the sender.
     *
     * @param message
     * @return
     */
    @Override
    public LogReplicationEntry receive(LogReplicationEntry message) {
        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        log.debug("Sink manager received {} while in {}", message.getMetadata().getMessageMetadataType(), rxState);

         // Ignore messages that have different topologyConfigId.
         // It could be caused by an out-of-date sender or the local node hasn't done the site discovery yet.
         // If there is a siteConfig change, the discovery service will detect it and reset the state.
        if (message.getMetadata().getTopologyConfigId() != topologyConfigId) {
            log.warn("Drop message {}. Topology config id mismatch, local={}, msg={}", message.getMetadata().getMessageMetadataType(),
                    topologyConfigId, message.getMetadata().getTopologyConfigId());
            return null;
        }

        // If it receives a SNAPSHOT_START message, prepare a transition
        if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_START)) {
            if (isValidSnapshotStart(message)) {
                processSnapshotStart(message);
                // The SnapshotPlugin will be called when LR is ready to start a snapshot sync,
                // so the system can prepare for the full sync. Typically, to stop checkpoint/trim
                // during the period of the snapshot sync to prevent data loss from shadow tables
                // (temporal non-checkpointed streams). This is a blocking call.
                log.info("Enter onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
                snapshotSyncPlugin.onSnapshotSyncStart(runtime);
                log.info("OnExit onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
            }
            return null;
        }

        if (!receivedValidMessage(message)) {
            // It is possible that the sender doesn't receive the SNAPSHOT_TRANSFER_COMPLETE ack message and
            // sends the SNAPSHOT_END marker again, but the receiver has already transited to
            // the LOG_ENTRY_SYNC state.
            // In this case send the SNAPSHOT_TRANSFER_COMPLETE ack again so the sender can do the proper transition.
            if (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END) {
                LogReplicationEntryMetadata ackMetadata = snapshotSinkBufferManager.generateAckMetadata(message);
                if (ackMetadata.getMessageMetadataType() == MessageType.SNAPSHOT_TRANSFER_COMPLETE) {
                    log.warn("Resend snapshot sync transfer complete ack. Sink state={}, received={}", rxState,
                            message.getMetadata().getMessageMetadataType());
                    return new LogReplicationEntry(ackMetadata);
                }
            }

            // Drop all other invalid messages
            log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                    message.getMetadata().getMessageMetadataType());

            return null;
        }

        return processReceivedMessage(message);
    }

    /**
     * Process received (valid) message depending on the current rx state (LOG_ENTRY_SYNC or SNAPSHOT_SYNC)
     *
     * @param message received message
     * @return ack
     */
    private LogReplicationEntry processReceivedMessage(LogReplicationEntry message) {
        if (rxState.equals(RxState.LOG_ENTRY_SYNC)) {
            return logEntrySinkBufferManager.processMsgAndBuffer(message);
        } else {
            LogReplicationEntry ack = snapshotSinkBufferManager.processMsgAndBuffer(message);
            // If the snapshot sync apply has completed (determined by the end marker) notify
            // plugin the completion, so checkpoint/trim process is resumed.
            if (ack.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END) {
                processSnapshotSyncApplied(ack);
            }
            return ack;
        }
    }

    private void processSnapshotSyncApplied(LogReplicationEntry ack) {
        long lastAppliedBaseSnapshotTimestamp = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        long latestSnapshotSyncCycleId = logReplicationMetadataManager.getCurrentSnapshotSyncCycleId();
        long ackSnapshotSyncCycleId = ack.getMetadata().getSyncRequestId().getMostSignificantBits() & Long.MAX_VALUE;
        // Verify this snapshot ACK corresponds to the last initialized/valid snapshot sync
        // as a previous one could have been canceled but still processed due to messages being out of order
        if ((ackSnapshotSyncCycleId == latestSnapshotSyncCycleId) &&
                (ack.getMetadata().getSnapshotTimestamp() == lastAppliedBaseSnapshotTimestamp)) {
            // Notify end of snapshot sync. This is a blocking call.
            log.info("Notify Snapshot Sync Plugin completion of snapshot sync id={}, baseSnapshot={}", ackSnapshotSyncCycleId,
                    lastAppliedBaseSnapshotTimestamp);
            log.info("Enter onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
            snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
            log.info("Exit onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
        } else {
            log.warn("SNAPSHOT_SYNC has completed for {}, but new ongoing SNAPSHOT_SYNC is {}",
                    ack.getMetadata().getSnapshotTimestamp(), lastAppliedBaseSnapshotTimestamp);
        }
    }

    /**
     * Verify if current Snapshot Start message determines the start
     * of a valid Snapshot Sync cycle.
     *
     * @param entry received entry
     * @return true, if it is a valid snapshot start marker
     *         false, otherwise
     */
    private boolean isValidSnapshotStart(LogReplicationEntry entry) {
        long topologyConfigId = entry.getMetadata().getTopologyConfigId();
        long messageBaseSnapshot = entry.getMetadata().getSnapshotTimestamp();

        log.debug("Received snapshot sync start marker with request id {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        /*
         * It is out of date message due to resend, drop it.
         */
        if (messageBaseSnapshot <= baseSnapshotTimestamp) {
            // Invalid message and drop it.
            log.warn("Sink Manager, state={} while received message={}. " +
                            "Dropping message with smaller snapshot timestamp than current {}",
                    rxState, entry.getMetadata(), baseSnapshotTimestamp);
            return false;
        }

        /*
         * Fails to set the baseSnapshot at the metadata store, it could be a out of date message,
         * or the current node is out of sync, ignore it.
         */
        if (!logReplicationMetadataManager.setBaseSnapshotStart(topologyConfigId, messageBaseSnapshot)) {
            log.warn("Sink Manager in state {} and received message {}. " +
                            "Dropping Message due to failure to update the metadata store {}",
                    rxState, entry.getMetadata(), logReplicationMetadataManager);
            return false;
        }

        return true;
    }

    /**
     * Process a SNAPSHOT_START message. This message will not be pushed to the buffer,
     * as it triggers a transition and resets the state.
     * If it is requesting a new snapshot with higher timestamp, transition to SNAPSHOT_SYNC state,
     * otherwise ignore the message.
     *
     * @param entry
     */
    private boolean processSnapshotStart(LogReplicationEntry entry) {
        long topologyConfigId = entry.getMetadata().getTopologyConfigId();
        long timestamp = entry.getMetadata().getSnapshotTimestamp();

        /*
         * Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
         */
        snapshotWriter.reset(topologyConfigId, timestamp);

        // Update lastTransferDone with the new snapshot transfer timestamp.
        baseSnapshotTimestamp = entry.getMetadata().getSnapshotTimestamp();

        // Setup buffer manager.
        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastSnapshotTransferredSequenceNumber(), this);

        // Set state in SNAPSHOT_SYNC state.
        rxState = RxState.SNAPSHOT_SYNC;
        log.info("Sink manager entry {} state, snapshot start with {}", rxState, entry.getMetadata());
        return true;
    }

    /**
     * Signal the manager a snapshot sync is about to complete. This is required to transition to log sync.
     */
    private void completeSnapshotApply(LogReplicationEntry inputEntry) {
        log.debug("Complete of a snapshot apply");
        //check if the all the expected message has received
        rxState = RxState.LOG_ENTRY_SYNC;

        logReplicationMetadataManager.setSnapshotAppliedComplete(inputEntry);
        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastProcessedLogEntryTimestamp(), this);

        log.info("Sink manager completed SNAPSHOT transfer {} for ts={} and has transit to {} state.",
                inputEntry.getMetadata().getSyncRequestId(), inputEntry.getMetadata().getTimestamp(), rxState);
    }

    /**
     * Process transferred snapshot messages
     *
     * @param message received entry message
     */
    private void processSnapshotMessage(LogReplicationEntry message) {
        switch (message.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
                snapshotWriter.apply(message);
                break;
            case SNAPSHOT_END:
                if (snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.APPLY_PHASE) {
                    setDataConsistent(false);
                    snapshotWriter.snapshotTransferDone(message);
                    completeSnapshotApply(message);
                    setDataConsistent(true);
                }
                break;
            default:
                log.warn("Message type {} should not be applied during snapshot sync.", message.getMetadata().getMessageMetadataType());
                break;
        }
    }

    /**
     * While processing an in order message, the buffer will callback and process the message
     * @param message
     */
    public void processMessage(LogReplicationEntry message) {
        log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);

        switch (rxState) {
            case LOG_ENTRY_SYNC:
                logEntryWriter.apply(message);
                break;

            case SNAPSHOT_SYNC:
                processSnapshotMessage(message);
                break;

            default:
                log.error("Wrong state {}.", rxState);
        }
    }

    /*
     * Verify if the message is the correct type for the current state.
     * @param message
     * @return
     */
    private boolean receivedValidMessage(LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END)
                || rxState == RxState.LOG_ENTRY_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    private void setDataConsistent(boolean isDataConsistent) {
        dataConsistent.set(isDataConsistent);
        logReplicationMetadataManager.setDataConsistentOnStandby(isDataConsistent);
    }

    /**
     * Update the topology config id
     *
     * @param topologyConfigId
     */
    public void updateTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    /**
     * When there is a cluster role change, the Sink Manager needs do the following:
     *
     * 1. Reset snapshotWriter and logEntryWriter state
     * 2. Reset buffer logEntryBuffer state.
     *
     * */
    public void reset() {
        snapshotWriter.reset(topologyConfigId, logReplicationMetadataManager.getLastAppliedSnapshotTimestamp());
        logEntryWriter.reset(logReplicationMetadataManager.getLastAppliedSnapshotTimestamp(),
                logReplicationMetadataManager.getLastProcessedLogEntryTimestamp());
        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastProcessedLogEntryTimestamp(), this);
    }

    public void shutdown() {
        this.runtime.shutdown();
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}
