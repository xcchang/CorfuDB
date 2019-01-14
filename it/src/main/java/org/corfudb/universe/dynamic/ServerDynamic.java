package org.corfudb.universe.dynamic;

import org.corfudb.universe.dynamic.events.*;
import org.corfudb.universe.dynamic.rules.UniverseRule;
import org.corfudb.universe.node.server.CorfuServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Dynamic that sequentially alternate between Stop, Start, Disconnect and Reconnect server events, with
 * PutData and GetData events between them. In each iteration, a different server is
 * stop/start.
 * Intended for basic troubleshooting.
 *
 * Created by edilmo on 11/06/18.
 */
public class ServerDynamic extends PutGetDataDynamic {

    /**
     * Max amount of data events between server node events.
     */
    private static final int MAX_CONSECUTIVE_DATA_EVENTS = 4;

    /**
     * Default time between two composite events in milliseconds.
     */
    private static final int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

    /**
     * Default time between two composite events in milliseconds.
     */
    private static final int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

    /**
     * Duration for stop server events generated.
     */
    private static final Duration DEFAULT_STOP_NODE_DURATION = Duration.ofSeconds(10);

    /**
     * Index of the server over which execute the next event.
     */
    private int nextServerIndex = 0;

    /**
     * Index of the type of the next event.
     */
    private int nextEventTypeIndex = 0;

    /**
     * List of server events that this dynamic can provide
     */
    protected List<Function<Map.Entry<String, CorfuServer>, ServerEvent>> possibleServeEvents = new ArrayList<>();

    /**
     * Generate a server event using a different combination of server and type of event, each time that is invoked.
     * The type of events alternates between stop-start-disconnect-connect.
     *
     * @return server event to materialize
     */
    protected ServerEvent generateNextServerEvent() {
        Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
        Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>) mapSet.toArray()[nextServerIndex];
        ServerEvent event = possibleServeEvents.get(nextEventTypeIndex).apply(server);
        nextEventTypeIndex++;
        if (nextEventTypeIndex >= possibleServeEvents.size()) {
            nextEventTypeIndex = 0;
            nextServerIndex++;
            if (nextServerIndex >= this.corfuServers.size()) {
                nextServerIndex = 0;
            }
        }
        return event;
    }

    /**
     * Generate a stop server event
     * @param server server to stop
     * @return server event to materialize
     */
    protected ServerEvent generateStopServerEvent(Map.Entry<String, CorfuServer> server){
        return new StopServerEvent(server.getKey(), server.getValue(), DEFAULT_STOP_NODE_DURATION);
    }

    /**
     * Index of the server over which execute the next stop action.
     */
    private int stopServerIndex = 0;

    /**
     * Generate a stop server event
     * @return server event to materialize
     */
    protected ServerEvent generateStopServerEvent(){
        Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
        Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>) mapSet.toArray()[stopServerIndex];
        stopServerIndex++;
        if (stopServerIndex >= this.corfuServers.size()) {
            stopServerIndex = 0;
        }
        return generateStopServerEvent(server);
    }

    /**
     * Generate a start server event
     * @param server server to start
     * @return server event to materialize
     */
    protected ServerEvent generateStartServerEvent(Map.Entry<String, CorfuServer> server){
        return new StartServerEvent(server.getKey(), server.getValue());
    }

    /**
     * Index of the server over which execute the next start action.
     */
    private int startServerIndex = 0;

    /**
     * Generate a start server event
     * @return server event to materialize
     */
    protected ServerEvent generateStartServerEvent(){
        Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
        Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>) mapSet.toArray()[startServerIndex];
        startServerIndex++;
        if (startServerIndex >= this.corfuServers.size()) {
            startServerIndex = 0;
        }
        return generateStartServerEvent(server);
    }

    /**
     * Generate a disconnect server event
     * @param server server to disconnect
     * @return server event to materialize
     */
    protected ServerEvent generateDisconnectServerEvent(Map.Entry<String, CorfuServer> server){
        return new DisconnectServerEvent(server.getKey(), server.getValue());
    }

    /**
     * Index of the server over which execute the next disconnect action.
     */
    private int disconnectServerIndex = 0;

    /**
     * Generate a disconnect server event
     * @return server event to materialize
     */
    protected ServerEvent generateDisconnectServerEvent(){
        Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
        Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>) mapSet.toArray()[disconnectServerIndex];
        disconnectServerIndex++;
        if (disconnectServerIndex >= this.corfuServers.size()) {
            disconnectServerIndex = 0;
        }
        return generateDisconnectServerEvent(server);
    }

    /**
     * Generate a reconnect server event
     * @param server server to reconnect
     * @return server event to materialize
     */
    protected ServerEvent generateReconnectServerEvent(Map.Entry<String, CorfuServer> server){
        return new ReconnectServerEvent(server.getKey(), server.getValue());
    }

    /**
     * Index of the server over which execute the next reconnect action.
     */
    private int reconnectServerIndex = 0;

    /**
     * Generate a reconnect server event
     * @return server event to materialize
     */
    protected ServerEvent generateReconnectServerEvent(){
        Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
        Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>) mapSet.toArray()[reconnectServerIndex];
        reconnectServerIndex++;
        if (reconnectServerIndex >= this.corfuServers.size()) {
            reconnectServerIndex = 0;
        }
        return generateReconnectServerEvent(server);
    }

    /**
     * Time in milliseconds before update the state.
     */
    @Override
    protected long getDelayForStateUpdate() {
        return DEFAULT_DELAY_FOR_STATE_UPDATE;
    }

    /**
     * Time in milliseconds between the composite event that just happened and the next one.
     */
    @Override
    protected long getIntervalBetweenEvents() {
        return (long) (DEFAULT_INTERVAL_BETWEEN_EVENTS * this.randomNumberGenerator.nextDouble());
    }

    /**
     * Amount of data events generated since the last server node event.
     */
    private int dataEventsGenerated = MAX_CONSECUTIVE_DATA_EVENTS;

    /**
     * Generate a server event after a amount of data events specified by
     * the constant MAX_CONSECUTIVE_DATA_EVENTS.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    protected UniverseEventOperator generateServerOrDataEvent() {
        UniverseEventOperator event;
        if (this.dataEventsGenerated >= MAX_CONSECUTIVE_DATA_EVENTS) {
            event = new UniverseEventOperator.Single(this.generateNextServerEvent());
            this.dataEventsGenerated = 0;
        } else {
            event = super.generateCompositeEvent();
            this.dataEventsGenerated++;
        }
        return event;
    }

    /**
     * Generate a server event after a amount of data events specified by
     * the constant MAX_CONSECUTIVE_DATA_EVENTS.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    @Override
    protected UniverseEventOperator generateCompositeEvent() {
        return generateServerOrDataEvent();
    }

    public ServerDynamic(long longevity, boolean waitForListener) {
        super(longevity, waitForListener);
        possibleServeEvents.add(this::generateStopServerEvent);
        possibleServeEvents.add(this::generateStartServerEvent);
        possibleServeEvents.add(this::generateDisconnectServerEvent);
        possibleServeEvents.add(this::generateReconnectServerEvent);
    }
}
