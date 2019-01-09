package org.corfudb.universe.dynamic.rules;

import org.corfudb.universe.dynamic.Dynamic;
import org.corfudb.universe.dynamic.events.*;
import org.corfudb.universe.dynamic.state.State;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulate the implementation of a rule that governs the dynamic
 * of a universe.
 * This class offers two list of singletons:
 *      - Pre Rules: the rules that must be checked before the desire transition
 *                   of a event is executed.
 *      - Pro Rules: the rules that must be checked after the desire transition
 *                   of a event is executed.
 * The rules are intended to allow a {@link Dynamic} safely generate events that could
 * be inconsistent by definition but never executed. The relevance of this is decoupled
 * the development of a {@link UniverseEvent} from the development of a {@link Dynamic}.
 *
 * Created by edilmo on 11/06/18.
 */
public abstract class UniverseRule {

    /**
     * The list of rules that must be checked before the desire transition
     * of a event is executed.
     */
    private static List<UniverseRule> preCompositeEventRules = new ArrayList<>();

    /**
     * The list of rules that must be checked after the desire transition
     * of a event is executed.
     */
    private static List<UniverseRule> postCompositeEventRules = new ArrayList<>();

    /**
     * Get the list of rules that must be checked before the desire transition
     * of a event is executed.
     *
     * @return List of pre rules.
     */
    public static List<UniverseRule> getPreCompositeEventRules() {
        if(preCompositeEventRules.isEmpty()){
            preCompositeEventRules.add(new GetDataThatExistRule());
            preCompositeEventRules.add(new StartStopNodeRule());
            preCompositeEventRules.add(new ReconnectDisconnectNodeRule());
        }
        return preCompositeEventRules;
    }

    /**
     * Get the list of rules that must be checked after the desire transition
     * of a event is executed.
     *
     * @return List of post rules.
     */
    public static List<UniverseRule> getPostCompositeEventRules() {
        if(postCompositeEventRules.isEmpty()){
            postCompositeEventRules.add(new AtLeastNServerUpRule(Dynamic.MINIMUM_NODES_UP));
        }
        return postCompositeEventRules;
    }

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
    public abstract boolean check(UniverseEventOperator compositeEvent, State desireState, State realState);
}
