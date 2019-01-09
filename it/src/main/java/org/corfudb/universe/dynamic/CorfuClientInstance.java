package org.corfudb.universe.dynamic;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.universe.dynamic.events.CorfuTableDataGenerationFunction;
import org.corfudb.universe.node.client.CorfuClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describe and holds a specific instance of a corfu client implementation
 * that is used in a {@link Dynamic}.
 *
 * Created by edilmo on 11/06/18.
 */
@Getter
@Setter
public class CorfuClientInstance {
    /**
     * Amount of corfu tables handle for each client in the universe.
     */
    private static final int DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT = 3;

    /**
     * Amount of corfu tables handle for each client in the universe.
     */
    private static final int DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE = 500;

    /**
     * Integer id that is used as a simple seed to make each data generation function different
     * in a deterministic a reproducible way.
     */
    protected int id;

    /**
     * Name of the client, it must be unique.
     */
    private final String name;

    /**
     * Throughput that the client see when is writing data to corfu.
     */
    private final Map<String, Double> lastMeasuredPutThroughputPerTable = new HashMap<>();

    /**
     * Throughput that the client see when is getting data from corfu.
     */
    private Map<String, Double> lastMeasuredGetThroughputPerTable = new HashMap<>();

    /**
     * Data generators for each corfu table handle by this client.
     */
    private final List<CorfuTableDataGenerationFunction> corfuTables = new ArrayList<>();

    /**
     * Corfu client implementation to use.
     */
    private final CorfuClient corfuClient;

    public CorfuClientInstance(int id, String name, CorfuClient corfuClient) {
        this.id = id;
        this.name = name;
        this.corfuClient = corfuClient;
        for (int i = 0; i < DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT; i++) {
            int generatorId = (this.id * DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT) + i;
            CorfuTableDataGenerationFunction generator;
            if ((i % 2) == 0) {
                generator = new CorfuTableDataGenerationFunction.IntegerSequence(generatorId,
                        DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE);
            } else {
                generator = new CorfuTableDataGenerationFunction.SinusoidalStrings(generatorId,
                        DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE);
            }
            this.corfuTables.add(generator);
            this.lastMeasuredGetThroughputPerTable.put(generator.getTableStreamName(), 0.0);
            this.lastMeasuredPutThroughputPerTable.put(generator.getTableStreamName(), 0.0);
        }
    }
}
