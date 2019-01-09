package org.corfudb.universe.dynamic;

import org.corfudb.universe.dynamic.events.GetDataEvent;
import org.corfudb.universe.dynamic.events.PutDataEvent;
import org.corfudb.universe.dynamic.events.UniverseEvent;
import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.rules.UniverseRule;

/**
 * Dynamic that sequentially alternate between PutData and GetData events, both over
 * a different stream table each time.
 * Intended for basic troubleshooting.
 *
 * Created by edilmo on 11/06/18.
 */
public class PutGetDataDynamic extends Dynamic {

    /**
     * Default time between two composite events in milliseconds.
     */
    private static int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

    /**
     * Default time between two composite events in milliseconds.
     */
    private static int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

    /**
     * Whether the next event to generate is a put data event or not.
     */
    private boolean generatePut = true;

    /**
     * Index of the client to use for the next put data event to generate.
     */
    private int clientIndexForPutData = 0;

    /**
     * Index table to use for the next put data event to generate.
     */
    private int tableIndexForPutData = 0;

    /**
     * Generate a put data event using a different corfu table each time that is invoked.
     *
     * @return data event to materialize
     */
    protected PutDataEvent generatePutDataEvent() {
        PutDataEvent event = new PutDataEvent(
                this.corfuClients.get(clientIndexForPutData).getCorfuTables().get(tableIndexForPutData),
                this.corfuClients.get(clientIndexForPutData));
        tableIndexForPutData++;
        if (tableIndexForPutData >= this.corfuClients.get(clientIndexForPutData).getCorfuTables().size()) {
            tableIndexForPutData = 0;
            clientIndexForPutData++;
            if (clientIndexForPutData >= this.corfuClients.size()) {
                clientIndexForPutData = 0;
            }
        }
        return event;
    }

    /**
     * Index of the client to use for the next get data event to generate.
     */
    private int clientIndexForGetData = 0;

    /**
     * Index table to use for the next get data event to generate.
     */
    private int tableIndexForGetData = 0;

    /**
     * Generate a get data event using a different corfu table each time that is invoked.
     *
     * @return data event to materialize
     */
    protected GetDataEvent generateGetDataEvent() {
        GetDataEvent event = new GetDataEvent(
                this.corfuClients.get(clientIndexForGetData).getCorfuTables().get(tableIndexForGetData),
                this.corfuClients.get(clientIndexForGetData));
        tableIndexForGetData++;
        if (tableIndexForGetData >= this.corfuClients.get(clientIndexForGetData).getCorfuTables().size()) {
            tableIndexForGetData = 0;
            clientIndexForGetData++;
            if (clientIndexForGetData >= this.corfuClients.size()) {
                clientIndexForGetData = 0;
            }
        }
        return event;
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
     * Generate a single event which content is a put or get event.
     * The invocation to this method alternates between put and get events.
     * And after each pair of put-get, a different table is used.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    @Override
    protected UniverseEventOperator generateCompositeEvent() {
        UniverseEvent event = generatePut ? this.generatePutDataEvent() : this.generateGetDataEvent();
        generatePut = !generatePut;
        return new UniverseEventOperator.Single(event);
    }

    public PutGetDataDynamic(long longevity, boolean waitForListener) {
        super(longevity, waitForListener);
    }
}
