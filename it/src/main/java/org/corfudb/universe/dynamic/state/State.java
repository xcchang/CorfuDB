package org.corfudb.universe.dynamic.state;

import lombok.Data;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.universe.dynamic.Stats;
import org.corfudb.universe.dynamic.events.CorfuTableDataGenerationFunction;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents the state of the universe in a point of time.
 *
 * The phase state is the abstraction that encapsulate all the parameters that describe
 * the evolution of the universe as a whole.
 * This abstraction does not need to consider any parameter used for the creation of
 * the universe that does not change during its life, e.g.
 *      - name of the network for the nodes.
 *      - type of nodes (VMs or Containers).
 *      - etc.
 * Parameters of the universe that change during its life but that are irrelevant for the
 * evaluation of the behaviour or performance of the system as whole, must be keep it
 * out of this abstraction.
 * The only parameters that we need to track in the state are those that can be compared
 * against themselves in order to produce a metric that allow us to evaluate the behaviour
 * or performance of the system as whole.
 *
 * Created by edilmo on 11/06/18.
 */
@Data
public class State implements Cloneable, Stats {

    /**
     * A list of servers in the universe.
     */
    private final Map<String, ServerState> servers = new HashMap<>();

    /**
     * A list of clients in the universe.
     */
    private final Map<String, ClientState> clients = new HashMap<>();

    /**
     * A map which keep track of the data put in each stream.
     * In order to have a efficient representation of the data that allow to test
     * with a payload arbitrary large, no data at all is saved, just
     * a instance of a class that describe how to produce the data.
     *
     * The key of the map is the name of the stream in corfu.
     * The value of the map is a special state class that includes
     * the a status of the stream as whole and a sequence of values put in the stream
     * during the time. The idea of this is to give support to the
     * snapshot feature of corfu.
     */
    private final Map<String, TableState> data = new HashMap<>();

    public Object clone() {
        State clone = new State();
        for(Map.Entry<String, ServerState> entry: this.servers.entrySet()){
            clone.servers.put(entry.getKey(), (ServerState)entry.getValue().clone());
        }
        for(Map.Entry<String, ClientState> entry: this.clients.entrySet()){
            clone.clients.put(entry.getKey(), (ClientState)entry.getValue().clone());
        }
        for(Map.Entry<String, TableState> entry: this.data.entrySet()){
            clone.data.put(entry.getKey(), (TableState)entry.getValue().clone());
        }
        return clone;
    }

    /**
     * Update the status of the given server.
     *
     * @param name  Name of the corfu server node.
     * @param status    New status of the corfu server node.
     */
    public void updateServerStatus(String name, NodeStatus status) {
        for(ClientState clientServerState: this.clients.values()){
            clientServerState.updateServerStatus(name, status);
        }
    }

    /**
     * Put the generator of a new data in the phase state of the table stream.
     *
     * @param tableStreamName           Name of the table stream used.
     * @param corfuTableDataGenerationFunction    Data generator used.
     * @throws CloneNotSupportedException
     */
    public synchronized void putDataToTableStream(String tableStreamName, CorfuTableDataGenerationFunction corfuTableDataGenerationFunction)
            throws CloneNotSupportedException {
        if(this.data.containsKey(tableStreamName)){
            this.data.get(tableStreamName).getDataTableStream().add(corfuTableDataGenerationFunction.getSnapshot());
        }
        else{
            TableState tableState = new TableState(tableStreamName);
            tableState.getDataTableStream().add(corfuTableDataGenerationFunction.getSnapshot());
            this.data.put(tableStreamName, tableState);
        }
    }

    /**
     * Whether data has been put in the specified table stream or not.
     *
     * @param tableStreamName   Name of the table stream.
     * @return                  Whether data has been put in the specified table stream or not.
     */
    public boolean hasDataInTableStream(String tableStreamName) {
        return this.data.containsKey(tableStreamName) && !this.data.get(tableStreamName).dataTableStream.isEmpty();
    }

    /**
     * Update the status of a corfu table stream.
     *
     * @param tableStreamName   Name of the corfu table stream.
     * @param status            Status of the corfu table stream.
     */
    public synchronized void updateStatusOfTableStream(String tableStreamName, TableStreamStatus status) {
        if(this.data.containsKey(tableStreamName)){
            this.data.get(tableStreamName).setStatus(status);
        }
    }

    /**
     * Update the put throughput for a given client.
     *
     * @param clientName name of the client to update.
     * @param throughput throughput value to update.
     */
    public synchronized void updateClientPutThroughput(String clientName, String tableStreamName, double throughput) {
        this.clients.get(clientName).getTableAccessStats().get(tableStreamName).setPutThroughput(throughput);
    }

    /**
     * Update the put throughput for a given client.
     *
     * @param clientName name of the client to update.
     * @param throughput throughput value to update.
     */
    public synchronized void updateClientGetThroughput(String clientName, String tableStreamName, double throughput) {
        this.clients.get(clientName).getTableAccessStats().get(tableStreamName).setGetThroughput(throughput);
    }

    /**
     * This function returns a set of unresponsive servers in the universe.
     * In corfu, the list of unresponsive servers is computed as the complement of the list of responsive server
     * which is computed as the interception of the responsive servers for the client and the responsive nodes.
     * Here we compute the unresponsive servers as those that are DOWN for any client deployed in the universe.
     *
     * @return A set of unresponsive servers in the universe.
     */
    public Set<String> getUnresponsiveServers() {
        return this.clients.values().stream().flatMap(cs -> cs.getServers().values().stream()).
                collect(Collectors.groupingBy(ClientServerState::getServerName,
                        Collectors.mapping(ClientServerState::getStatus, Collectors.toList()))).entrySet().stream().
                filter(e -> e.getValue().contains(NodeStatus.DOWN)).
                map(e -> e.getKey()).collect(Collectors.toSet());
    }

    public boolean isUnresponsiveServer(String serverName){
        return getUnresponsiveServers().contains(serverName);
    }

    /**
     * This function returns a set of responsive servers in the universe.
     * Here we compute the responsive servers as the complement of the list of unresponsive server.
     *
     * @return A set of unresponsive servers in the universe.
     */
    public List<String> getResponsiveServers() {
        Set<String> unresponsiveServers = this.getUnresponsiveServers();
        List<String> servers = this.servers.values().stream().filter(ss -> !unresponsiveServers.contains(ss.getName())).
                map(ServerState::getName).collect(Collectors.toList());
        return servers;
    }

    /**
     * Aggregates all metrics for servers, clients and tables in a single map. Some metrics are groups
     * of metrics in a map.
     *
     * @return Map with a key for each metric or group of metrics. A group of metrics is another map.
     */
    public LinkedHashMap<String, Object> getStats() {
        //Stats for servers
        LinkedHashMap<String, Object> sortedAggregations = ServerState.getStatsForCollection(this.servers.values().
                stream().collect(Collectors.toList()));
        //Stats for clients
        for(Map.Entry<String, Object> e: ClientState.getStatsForCollection(this.clients.values().
                stream().collect(Collectors.toList())).entrySet()){
            sortedAggregations.put(e.getKey(), e.getValue());
        }
        //Stats for tables
        for(Map.Entry<String, Object> e: TableState.getStatsForCollection(this.data.values().
                stream().collect(Collectors.toList())).entrySet()){
            sortedAggregations.put(e.getKey(), e.getValue());
        }
        return sortedAggregations;
    }
}
