package org.corfudb.integration;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.integration.DefaultDataControl.DefaultDataControlConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SourceForwardingDataSender implements DataSender {
    // Runtime to remote/destination Corfu Server
    private CorfuRuntime runtime;

    // Manager in remote/destination site, to emulate the channel, we instantiate the destination receiver
    private LogReplicationSinkManager destinationLogReplicationManager;

    // Destination DataSender
    private AckDataSender destinationDataSender;

    // Destination DataControl
    private DefaultDataControl destinationDataControl;

    private int errorCount = 0;

    @VisibleForTesting
    @Getter
    private ObservableAckMsg ackMessages = new ObservableAckMsg();

    /*
     * 0: no message drop
     * 1: drop some message once
     * 2: drop a particular message 5 times to trigger a timeout error
     */
    final public static int DROP_MSG_ONCE = 1;

    private int ifDropMsg = 0;

    final static int DROP_INCREMENT = 4;

    private int droppingNum = 2;

    private int msgCnt = 0;

    @Getter
    private ObservableValue errors = new ObservableValue(errorCount);

    public SourceForwardingDataSender(String destinationEndpoint, LogReplicationConfig config, int ifDropMsg, LogReplicationMetadataManager metadataManager,
                                      String pluginConfigFilePath) {
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(destinationEndpoint)
                .connect();
        this.destinationDataSender = new AckDataSender();
        this.destinationDataControl = new DefaultDataControl(new DefaultDataControlConfig(false, 0));
        this.destinationLogReplicationManager = new LogReplicationSinkManager(runtime.getLayoutServers().get(0), config, metadataManager, pluginConfigFilePath);
        this.ifDropMsg = ifDropMsg;
        log.info("Init SourceForwardingDataSender with ifDropMsg {}", ifDropMsg);
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        log.trace("Send message: " + message.getMetadata().getMessageMetadataType() + " for:: " + message.getMetadata().getTimestamp());
        if (ifDropMsg > 0 && msgCnt == droppingNum) {
            log.info("****** Drop msg {} log entry ts {}",  msgCnt, message.getMetadata().timestamp);
            if (ifDropMsg == DROP_MSG_ONCE) {
                droppingNum += DROP_INCREMENT;
            }

            return new CompletableFuture<>();
        }

        final CompletableFuture<LogReplicationEntry> cf = new CompletableFuture<>();

        // Emulate Channel by directly accepting from the destination, whatever is sent by the source manager
        LogReplicationEntry ack = destinationLogReplicationManager.receive(message);
        if (ack != null) {
            cf.complete(ack);
        }
        ackMessages.setValue(ack);
        msgCnt++;
        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {
        CompletableFuture<LogReplicationEntry> lastAckMessage = null;
        CompletableFuture<LogReplicationEntry> tmp;

        for (LogReplicationEntry message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                lastAckMessage = tmp;
            }
        }

        try {
            if (lastAckMessage != null) {
                LogReplicationEntry entry = lastAckMessage.get();
                ackMessages.setValue(entry);
            }
        } catch (Exception e) {
            System.out.print("Caught an exception " + e);
        }

        return lastAckMessage;
    }

    @Override
    public void onError(LogReplicationError error) {
        errorCount++;
        errors.setValue(errorCount);
        log.trace("\nSourceFowardingDataSender got an error " + error);
    }

    /*
     * Auxiliary Methods
     */
    public void setSourceManager(LogReplicationSourceManager sourceManager) {
        destinationDataSender.setSourceManager(sourceManager);
        destinationDataControl.setSourceManager(sourceManager);
    }

    // Used for testing purposes to access the LogReplicationSinkManager in Test
    public LogReplicationSinkManager getSinkManager() {
        return destinationLogReplicationManager;
    }

    public CorfuRuntime getWriterRuntime() {
        return this.runtime;
    }

    public void shutdown() {
        if (destinationDataSender != null && destinationDataSender.getSourceManager() != null) {
            destinationDataSender.getSourceManager().shutdown();
        }

        if (destinationLogReplicationManager != null) {
            destinationLogReplicationManager.shutdown();
        }

        if (runtime != null) {
            runtime.shutdown();
        }
    }
}
