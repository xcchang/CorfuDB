package org.corfudb.universe.dynamic;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class PhaseDynamic extends RandomDynamic {

    /**
     * List of phases that this dynamic is going to pass
     */
    private final List<UniversePhase> phases = new ArrayList<>();

    private int currentPhaseIndex;

    private double nextPhasePercentage;

    /**
     * Generate a composite event to materialize in the universe.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    @Override
    protected UniverseEventOperator generateCompositeEvent() {
        UniversePhase phase = phases.get(currentPhaseIndex);
        long elapsed = System.currentTimeMillis() - this.startTime;
        double elapsedPercentage = (double)elapsed / longevity;
        if(elapsedPercentage >= nextPhasePercentage) {
            log.info(String.format("Changing from phase %s", phases.get(currentPhaseIndex).getClass().getName()));
            currentPhaseIndex++;
            if (currentPhaseIndex >= phases.size())
                currentPhaseIndex = 0;
            log.info(String.format("New phase %s", phases.get(currentPhaseIndex).getClass().getName()));
            nextPhasePercentage += phases.get(currentPhaseIndex).getLongevityPercentage();
        }
        return phase.generateCompositeEvent();
    }

    public PhaseDynamic(long longevity, boolean waitForListener) {
        super(longevity, waitForListener);
        phases.add(new SimpleDataPhase());
        phases.add(new SimpleServerPhase());
        phases.add(new RandomPhase());
        currentPhaseIndex = 0;
        nextPhasePercentage = phases.get(currentPhaseIndex).getLongevityPercentage();
    }

    private interface UniversePhase {
        /**
         * Generate a composite event to materialize in the universe.
         * The events generated have not guaranty of execution. They are allowed
         * to execute if they are compliant with the {@link UniverseRule}.
         *
         * @return Composite event generated.
         */
        UniverseEventOperator generateCompositeEvent();

        double getLongevityPercentage();
    }

    private class SimpleDataPhase implements UniversePhase {
        public UniverseEventOperator generateCompositeEvent(){
            return generateDataEvent();
        }

        public double getLongevityPercentage() {
            return 0.2d;
        }
    }

    private class SimpleServerPhase implements UniversePhase {
        public UniverseEventOperator generateCompositeEvent(){
            return generateServerOrDataEvent();
        }

        public double getLongevityPercentage() {
            return 0.3d;
        }
    }

    private class RandomPhase implements UniversePhase {
        public UniverseEventOperator generateCompositeEvent(){
            return generateRandomCompositeEvent();
        }

        public double getLongevityPercentage() {
            return 0.5d;
        }
    }
}
