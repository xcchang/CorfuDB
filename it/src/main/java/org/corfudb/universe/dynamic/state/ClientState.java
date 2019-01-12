package org.corfudb.universe.dynamic.state;

import com.spotify.docker.client.DockerClient;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.corfudb.runtime.view.ClusterStatusReport;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Represents the state of a node client in a point of time.
 *
 * Created by edilmo on 11/06/18.
 */
@ToString
@Data
@EqualsAndHashCode(callSuper = false)
public class ClientState extends NodeState {

    /**
     * Name of the node client
     */
    private final String name;

    /**
     * Name of the server on the host
     */
    private final String hostName;

    /**
     * Cluster health status.
     */
    private ClusterStatusReport.ClusterStatus clusterStatus = ClusterStatusReport.ClusterStatus.STABLE;

    /**
     * A map with the state that the client see for each server in the universe.
     */
    private final Map<String, ClientServerState> servers = new HashMap<>();

    /**
     * A map which keep track of the throughput stats per table stream.
     */
    private final Map<String, ClientTableAccessStats> tableAccessStats = new HashMap<>();

    /**
     * This function returns a set of unresponsive servers that the client is seing.
     *
     * @return A set of unresponsive servers.
     */
    public Set<String> getUnresponsiveServers() {
        return this.servers.values().stream().filter(css -> css.getStatus() == ClusterStatusReport.NodeStatus.DOWN).
                map(ClientServerState::getServerName).collect(Collectors.toSet());
    }

    /**
     * Update the status of the given server.
     *
     * @param nodeName  Name of the corfu server node.
     * @param status    New status of the corfu server node.
     */
    public void updateServerStatus(String nodeName, ClusterStatusReport.NodeStatus status) {
        this.servers.get(nodeName).setStatus(status);
        int unresponsiveServersCount = getUnresponsiveServers().size();
        if(unresponsiveServersCount == servers.size()){
            setClusterStatus(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
        }else if(unresponsiveServersCount == 0){
            setClusterStatus(ClusterStatusReport.ClusterStatus.STABLE);
        }else{
            setClusterStatus(ClusterStatusReport.ClusterStatus.DEGRADED);
        }
    }

    public Object clone(){
        ClientState clone = new ClientState(this.name, this.hostName, this.getHostController(), this.getMonitorPeriod(),
                this.getMonitorPeriodUnit());
        updateClone(clone);
        clone.clusterStatus = this.clusterStatus;
        for(Map.Entry<String, ClientServerState> entry: this.servers.entrySet()){
            clone.servers.put(entry.getKey(), (ClientServerState)entry.getValue().clone());
        }
        for(Map.Entry<String, ClientTableAccessStats> entry: this.tableAccessStats.entrySet()){
            clone.tableAccessStats.put(entry.getKey(), (ClientTableAccessStats)entry.getValue().clone());
        }
        return clone;
    }

    public LinkedHashMap<String, Object> getStats(){
        //Stats for server status across servers for this same client
        LinkedHashMap<String, Object> sortedAggregations = ClientServerState.getStatsForCollection(this.servers.values().
                stream().collect(Collectors.toList()));
        //Stats for table access across tables for this same client
        for(Map.Entry<String, Object> e: ClientTableAccessStats.getStatsForCollection(this.tableAccessStats.values().
                stream().collect(Collectors.toList())).entrySet()){
            sortedAggregations.put(e.getKey(), e.getValue());
        }
        return sortedAggregations;
    }

    /**
     * Computes the count of clients that see a possible cluster status, and aggregates the stats for
     * servers and tables.
     *
     * @return Map with {@link ClusterStatusReport.ClusterStatus} as key and the count of clients as value
     * and a key-value pair for each client stats.
     */
    public static LinkedHashMap<String, Object> getStatsForCollection(List<ClientState> collection) {
        //Stats for cluster status
        Map<ClusterStatusReport.ClusterStatus, Long> aggregation = collection.stream().collect(
                Collectors.groupingBy(ClientState::getClusterStatus, Collectors.counting()));
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        if(!aggregation.containsKey(ClusterStatusReport.ClusterStatus.DEGRADED))
            sortedAggregations.put(DEGRADED_CLUSTER_STATUS, 0L);
        else
            sortedAggregations.put(DEGRADED_CLUSTER_STATUS, aggregation.get(ClusterStatusReport.ClusterStatus.DEGRADED));
        if(!aggregation.containsKey(ClusterStatusReport.ClusterStatus.STABLE))
            sortedAggregations.put(STABLE_CLUSTER_STATUS, 0L);
        else
            sortedAggregations.put(STABLE_CLUSTER_STATUS, aggregation.get(ClusterStatusReport.ClusterStatus.STABLE));
        if(!aggregation.containsKey(ClusterStatusReport.ClusterStatus.UNAVAILABLE))
            sortedAggregations.put(UNAVAILABLE_CLUSTER_STATUS, 0L);
        else
            sortedAggregations.put(UNAVAILABLE_CLUSTER_STATUS, aggregation.get(ClusterStatusReport.ClusterStatus.UNAVAILABLE));
        //Stats for server status across clients for a same server
        Map<String, List<ClientServerState>> clientServerStatesPerServer = collection.stream().flatMap(cs ->
                cs.getServers().values().stream()).collect(Collectors.groupingBy(ClientServerState::getServerName));
        Map<String, LinkedHashMap<String, Object>> clientServerStatsPerServer = clientServerStatesPerServer.
                entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> ClientServerState.getStatsForCollection(e.getValue())));
        for(Map.Entry<String, LinkedHashMap<String, Object>> e: clientServerStatsPerServer.entrySet()){
            sortedAggregations.put(CLIENT_SERVER_PREFIX + e.getKey(), e.getValue());
        }
        //Stats for server status across servers for this same client and
        //Stats for table access across tables for this same client
        for(ClientState cs: collection.stream().sorted(Comparator.comparing(ClientState::getName)).
                collect(Collectors.toList())){
            sortedAggregations.put(CLIENT_PREFIX + cs.name, cs.getStats());
        }
        //Stats for table access across clients for a same table
        Map<String, List<ClientTableAccessStats>> clientTableAccessStatsPerTable = collection.stream().flatMap(cs ->
                cs.getTableAccessStats().values().stream()).
                collect(Collectors.groupingBy(ClientTableAccessStats::getTableStreamName));
        Map<String, LinkedHashMap<String, Object>> tableAccessStatsAgregatedPerTable = clientTableAccessStatsPerTable.
                entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> ClientTableAccessStats.getStatsForCollection(e.getValue())));
        for(Map.Entry<String, LinkedHashMap<String, Object>> e: tableAccessStatsAgregatedPerTable.entrySet()){
            sortedAggregations.put(TABLE_STATS_PREFIX + e.getKey(), e.getValue());
        }
        return sortedAggregations;
    }

    public ClientState(String name, String hostName, DockerClient hostController, long monitorPeriod,
                       TimeUnit monitorPeriodUnit) {
        super(hostController, monitorPeriod, monitorPeriodUnit);
        this.name = name;
        this.hostName = hostName;
    }
}
