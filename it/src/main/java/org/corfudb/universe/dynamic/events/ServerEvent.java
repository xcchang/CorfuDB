package org.corfudb.universe.dynamic.events;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.state.State;
import org.corfudb.universe.node.server.CorfuServer;

import java.time.Duration;

/**
 * A event representing a action over a corfu server node.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public abstract class ServerEvent implements UniverseEvent {

    /**
     * Name of the corfu node server associated to the event.
     */
    @Getter
    protected final String serverName;

    /**
     * Corfu server associated to the event.
     */
    protected final CorfuServer corfuServer;

    /**
     * A short description of the action over a corfu server node.
     *
     * @return Short description of the action.
     */
    protected abstract String getActionDescription();

    /**
     * Get a description of the observed change produced in the state of the universe
     * when this events happened.
     */
    @Override
    public String getObservationDescription() {
        return String.format("%s corfu server node %s", this.getActionDescription(), this.serverName);
    }

    public ServerEvent(String serverName, CorfuServer corfuServer) {
        this.serverName = serverName;
        this.corfuServer = corfuServer;
    }
}
