package org.corfudb.universe.dynamic.rules;

import org.corfudb.universe.dynamic.events.ReconnectServerEvent;
import org.corfudb.universe.dynamic.events.DisconnectServerEvent;
import org.corfudb.universe.dynamic.events.UniverseEvent;
import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.state.State;

/**
 * Rule to ensure that reconnect events occurred over disconnected servers and disconnect
 * events occurred over connected servers.
 *
 * Created by edilmo on 11/06/18.
 */
public class ReconnectDisconnectNodeRule extends UniverseRule {

    /**
     * Check if the composite event, desire state and real state, are all compliant
     * with the rule defined.
     *
     * @param compositeEvent    Composite event that is happening.
     * @param desireState       Current desire state of the universe.
     * @param realState         Current real state of the universe.
     * @return                  Whether the composite event, desire state and real state,
     *                          are all compliant with the rule.
     */
    @Override
    public boolean check(UniverseEventOperator compositeEvent, State desireState, State realState) {
        boolean pass = true;
        for(UniverseEvent event: compositeEvent.getSimpleEventsToCompose()){
            if(event instanceof DisconnectServerEvent){
                DisconnectServerEvent disconnectServerEventEvent = (DisconnectServerEvent)event;
                if(desireState.isUnresponsiveServer(disconnectServerEventEvent.getServerName())){
                    pass = false;
                    break;
                }
            }else if(event instanceof ReconnectServerEvent) {
                ReconnectServerEvent reconnectServerEventEvent = (ReconnectServerEvent)event;
                if(!desireState.isUnresponsiveServer(reconnectServerEventEvent.getServerName())){
                    pass = false;
                    break;
                }
            }
        }
        return pass;
    }

    public ReconnectDisconnectNodeRule(){
    }
}
