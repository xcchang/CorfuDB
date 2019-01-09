package org.corfudb.universe.dynamic.rules;

import org.corfudb.universe.dynamic.events.StartServerEvent;
import org.corfudb.universe.dynamic.events.StopServerEvent;
import org.corfudb.universe.dynamic.events.UniverseEvent;
import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.state.State;

/**
 * Rule to ensure that start events occurred over stop servers and stops
 * events occurred over running servers.
 *
 * Created by edilmo on 11/06/18.
 */
public class StartStopNodeRule extends UniverseRule {

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
            if(event instanceof StopServerEvent){
                StopServerEvent stopServerEventEvent = (StopServerEvent)event;
                if(desireState.isUnresponsiveServer(stopServerEventEvent.getServerName())){
                    pass = false;
                    break;
                }
            }else if(event instanceof StartServerEvent) {
                StartServerEvent startServerEventEvent = (StartServerEvent)event;
                if(!desireState.isUnresponsiveServer(startServerEventEvent.getServerName())){
                    pass = false;
                    break;
                }
            }
        }
        return pass;
    }

    public StartStopNodeRule(){
    }
}
