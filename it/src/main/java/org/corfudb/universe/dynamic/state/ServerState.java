package org.corfudb.universe.dynamic.state;

import com.spotify.docker.client.DockerClient;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Represents the state of a node server in a point of time.
 *
 * Created by edilmo on 11/06/18.
 */
@Data
@ToString
@EqualsAndHashCode(callSuper = false)
public class ServerState extends NodeState {

    /**
     * Name of the server
     */
    private final String name;

    /**
     * Endpoint of the server
     */
    private final String nodeEndpoint;

    public Object clone(){
        ServerState clone = new ServerState(this.name, this.nodeEndpoint,
                this.getHostController(), this.getMonitorPeriod(), this.getMonitorPeriodUnit());
        updateClone(clone);
        return clone;
    }

    /**
     * Aggregates every metric of each server in one single map
     *
     * @return Map with a key for each metric
     */
    public static LinkedHashMap<String, Object> getStatsForCollection(List<ServerState> collection){
        return NodeState.getStatsForNodeStateCollection(collection.stream().map(ss -> (NodeState)ss).
                collect(Collectors.toList()));
    }

    public ServerState(String name, String nodeEndpoint, DockerClient hostController,
                       long monitorPeriod, TimeUnit monitorPeriodUnit) {
        super(hostController, monitorPeriod, monitorPeriodUnit);
        this.name = name;
        this.nodeEndpoint = nodeEndpoint;
    }
}
