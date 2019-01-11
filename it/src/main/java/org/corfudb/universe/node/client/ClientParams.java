package org.corfudb.universe.node.client;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.Node.NodeType;

import java.time.Duration;

@Builder
@Getter
@EqualsAndHashCode(exclude = {"numRetry", "timeout", "pollPeriod", "systemDownHandlerTriggerLimit", "requestTimeout", "idleConnectionTimeout", "connectionTimeout", "connectionRetryRate"})
public class ClientParams implements NodeParams<ClientParams> {
    @Default
    @NonNull
    private final String name = "corfuClient";
    @NonNull
    private final NodeType nodeType = NodeType.CORFU_CLIENT;
    /**
     * Total number of times to retry a workflow if it fails
     */
    @Default
    @NonNull
    private final int numRetry = 5;
    /**
     * Total time to wait before the workflow times out
     */
    @Default
    @NonNull
    private final Duration timeout = Duration.ofSeconds(30);
    /**
     * Poll period to query the completion of the workflow
     */
    @Default
    @NonNull
    private final Duration pollPeriod = Duration.ofMillis(50);

    @Default
    private final int order = 0;

    //Corfu runtime parameters

    /**
     * Number of retries to reconnect to an unresponsive system before invoking the
     * systemDownHandler.
     */
    @Default
    @NonNull
    private int systemDownHandlerTriggerLimit = 20;

    /**
     * {@link Duration} before requests timeout.
     * This is the duration after which the reader hole fills the address.
     */
    @Default
    private Duration requestTimeout = Duration.ofSeconds(5);

    /**
     * This timeout (in seconds) is used to detect servers that
     * shutdown abruptly without terminating the connection properly.
     */
    @Default
    private int idleConnectionTimeout = 30;

    /**
     * {@link Duration} before connections timeout.
     */
    @Default
    private Duration connectionTimeout = Duration.ofMillis(500);

    /**
     * {@link Duration} before reconnecting to a disconnected node.
     */
    @Default
    private Duration connectionRetryRate = Duration.ofSeconds(1);

    @Override
    public int compareTo(ClientParams other) {
        return Integer.compare(order, other.order);
    }
}
