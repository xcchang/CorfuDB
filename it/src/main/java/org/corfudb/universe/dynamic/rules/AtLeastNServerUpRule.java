package org.corfudb.universe.dynamic.rules;

import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.state.State;

/**
 * Rule to ensure the minimum amount of nodes that should be up at any point of time.
 *
 * Created by edilmo on 11/06/18.
 */
public class AtLeastNServerUpRule extends UniverseRule {
    /**
     * Minimum amount of nodes that should be up at any point of time.
     */
    private final int minNumNodes;

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
        if((desireState.getServers().size() - desireState.getUnresponsiveServers().size()) < this.minNumNodes)
            pass = false;
        return pass;
    }

    public AtLeastNServerUpRule(int minNumNodes){
        this.minNumNodes = minNumNodes;
    }
}
