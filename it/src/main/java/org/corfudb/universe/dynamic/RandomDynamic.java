package org.corfudb.universe.dynamic;

import org.corfudb.universe.dynamic.events.UniverseEvent;
import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.rules.UniverseRule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Dynamic that randomly generate composite events
 *
 * Created by edilmo on 11/06/18.
 */
public class RandomDynamic extends ServerDynamic {

    /**
     * Default time between two composite events in milliseconds.
     */
    private static int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

    /**
     * Default time between two composite events in milliseconds.
     */
    private static int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

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
     * List of events that this dynamic can provide
     */
    private final List<Supplier<UniverseEvent>> possibleEvents = new ArrayList<>();

    /**
     * Generate a composite event to materialize in the universe.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    @Override
    protected UniverseEventOperator generateCompositeEvent() {
        List<UniverseEvent> actualEvents = new ArrayList<>();
        int i = 0;
        while (i < this.possibleEvents.size() || actualEvents.isEmpty()) {
            if (this.randomNumberGenerator.nextBoolean()) {
                actualEvents.add(this.possibleEvents.get(i % 3).get());
            }
            i++;
        }
        UniverseEventOperator event = this.randomNumberGenerator.nextBoolean() ?
                new UniverseEventOperator.Sequential(actualEvents) :
                new UniverseEventOperator.Concurrent(actualEvents);
        return event;
    }

    public RandomDynamic(long longevity, boolean waitForListener) {
        super(longevity, waitForListener);
        this.possibleEvents.add(this::generatePutDataEvent);
        this.possibleEvents.add(this::generateGetDataEvent);
        this.possibleEvents.add(this::generateStopServerEvent);
        this.possibleEvents.add(this::generateStartServerEvent);
        this.possibleEvents.add(this::generateDisconnectServerEvent);
        this.possibleEvents.add(this::generateReconnectServerEvent);
    }
}
