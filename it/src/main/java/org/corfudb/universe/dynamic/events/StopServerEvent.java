package org.corfudb.universe.dynamic.events;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.state.State;
import org.corfudb.universe.node.server.CorfuServer;

import java.time.Duration;

/**
 * StopServerEvent a corfu server node.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public class StopServerEvent extends ServerEvent {

    /**
     * Duration passed to the stop action.
     */
    private final Duration duration;

    /**
     * A short description of the action over a corfu server node.
     *
     * @return Short description of the action.
     */
    @Override
    protected String getActionDescription() {
        return "StopServerEvent";
    }

    /**
     * Get the desire-state of the universe after this event happened.
     * This method is called before {@link UniverseEventOperator::executeRealPartialTransition}.
     * The method must perform the updates directly over the currentDesireState reference.
     *
     * @param currentDesireState Desire-state of the universe before this event happened.
     * @return Desire-state of the universe after this event happened.
     */
    @Override
    public void applyDesirePartialTransition(State currentDesireState) {
        currentDesireState.updateServerStatus(this.serverName, ClusterStatusReport.NodeStatus.DOWN);
    }

    /**
     * Execute the transition of the universe that materialize the occurrence of the event.
     * The method must perform the updates directly over the parameter currentRealState reference.
     *
     * @param currentRealState Real-state of the universe before this event happened.
     */
    @Override
    public void executeRealPartialTransition(State currentRealState) {
        this.corfuServer.stop(this.duration);
    }

    public StopServerEvent(String nodeName, CorfuServer corfuServer, Duration duration){
        super(nodeName, corfuServer);
        this.duration = duration;
    }
}
