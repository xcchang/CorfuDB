package org.corfudb.universe.dynamic.state;

import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.Stats;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the state of a node server that a client see in a point of time.
 *
 * Created by edilmo on 11/06/18.
 */
@Data
@ToString
public class ClientServerState implements Cloneable {

    /**
     * Name of the node server
     */
    private final String serverName;

    /**
     * Endpoint of the node server
     */
    private final String nodeEndpoint;

    /**
     * Whether the server is see it as layout or not.
     */
    private boolean isLayoutServer;

    /**
     * Whether the server is see it as log unit or not.
     */
    private boolean isLogUnitServer;

    /**
     * Whether the server is see it as primary or not.
     */
    private boolean isPrimarySequencer;

    /**
     * Connectivity to the node.
     */
    private ClusterStatusReport.NodeStatus status = ClusterStatusReport.NodeStatus.UP;

    public Object clone(){
        ClientServerState clone = new ClientServerState(this.serverName, this.nodeEndpoint);
        clone.isLayoutServer = this.isLayoutServer;
        clone.isLogUnitServer = this.isLogUnitServer;
        clone.isPrimarySequencer = this.isPrimarySequencer;
        clone.status = this.status;
        return clone;
    }

    /**
     * Computes the count of clients that see layout servers, log unit servers and primary sequencers, and also
     * the amount of servers in each possible node status
     *
     * @return Map with a key for each count computed in the method
     */
    public static LinkedHashMap<String, Object> getStatsForCollection(List<ClientServerState> collection) {
        int layoutServers = 0;
        int logUnitServers = 0;
        int primarySequencers = 0;
        for(ClientServerState css: collection){
            if(css.isLayoutServer)
                layoutServers++;
            if(css.isLogUnitServer)
                logUnitServers++;
            if(css.isPrimarySequencer)
                primarySequencers++;
        }
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        sortedAggregations.put(Stats.LAYOUT_SERVERS, layoutServers);
        sortedAggregations.put(Stats.LOG_UNIT_SERVERS, logUnitServers);
        sortedAggregations.put(Stats.PRIMARY_SEQUENCERS, primarySequencers);
        //Stats for cluster status
        Map<ClusterStatusReport.NodeStatus, Long> aggregation = collection.stream().collect(
                Collectors.groupingBy(ClientServerState::getStatus, Collectors.counting()));
        if(!aggregation.containsKey(ClusterStatusReport.NodeStatus.DB_SYNCING))
            sortedAggregations.put(Stats.DB_SYNCING_SERVERS, 0L);
        else
            sortedAggregations.put(Stats.DB_SYNCING_SERVERS, aggregation.get(ClusterStatusReport.NodeStatus.DB_SYNCING));
        if(!aggregation.containsKey(ClusterStatusReport.NodeStatus.DOWN))
            sortedAggregations.put(Stats.DOWN_SERVERS, 0L);
        else
            sortedAggregations.put(Stats.DOWN_SERVERS, aggregation.get(ClusterStatusReport.NodeStatus.DOWN));
        if(!aggregation.containsKey(ClusterStatusReport.NodeStatus.UP))
            sortedAggregations.put(Stats.UP_SERVERS, 0L);
        else
            sortedAggregations.put(Stats.UP_SERVERS, aggregation.get(ClusterStatusReport.NodeStatus.UP));
        return sortedAggregations;
    }
}
