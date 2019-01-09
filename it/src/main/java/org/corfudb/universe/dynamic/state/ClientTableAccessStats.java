package org.corfudb.universe.dynamic.state;

import lombok.Data;
import lombok.ToString;
import org.corfudb.universe.dynamic.Stats;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Contains the throughput of gets and puts in a given table for a specific client.
 *
 * Created by edilmo on 11/06/18.
 */
@Data
@ToString
public class ClientTableAccessStats implements Cloneable {

    /**
     * Name of the table stream in corfu.
     */
    private final String tableStreamName;

    /**
     * Throughput that the client see when is writing data to corfu.
     */
    private double putThroughput = 0;

    /**
     * Throughput that the client see when is getting data from corfu.
     */
    private double getThroughput = 0;

    public Object clone(){
        ClientTableAccessStats clone = new ClientTableAccessStats(this.tableStreamName);
        clone.putThroughput = this.putThroughput;
        clone.getThroughput = this.getThroughput;
        return clone;
    }

    /**
     * Computes the average get and put throughput across the tables specified.
     *
     * @return Map with a key for the average get throughput and other for the put throughput.
     */
    public static LinkedHashMap<String, Object> getStatsForCollection(List<ClientTableAccessStats> collection){
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        double getThroughputAverage = collection.stream().mapToDouble(ClientTableAccessStats::getGetThroughput).
                average().orElse(0.0);
        sortedAggregations.put(Stats.GET_THROUGHPUT_AVERAGE, getThroughputAverage);
        double putThroughputAverage = collection.stream().mapToDouble(ClientTableAccessStats::getPutThroughput).
                average().orElse(0.0);
        sortedAggregations.put(Stats.PUT_THROUGHPUT_AVERAGE, putThroughputAverage);
        return sortedAggregations;
    }
}
