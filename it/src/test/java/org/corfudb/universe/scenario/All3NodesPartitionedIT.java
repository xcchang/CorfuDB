package org.corfudb.universe.scenario;

import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class All3NodesPartitionedIT extends AllNodesBaseIT {

    @Override
    protected FailureType getFailureType(int corfuServerIndex){
        return FailureType.DISCONNECT_NODE;
    }

    @Override
    protected boolean useOneUniversePerTest(){
        return true;
    }

    @Override
    protected int getAmountOfNodes(){
        return 3;
    }

    /**
     * Test cluster behavior after all nodes are disconnected and just one is reconnected afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Reconnect just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is UNAVAILABLE and the data path
     * operations failed with UnreachableClusterException
     */
    //@Ignore("Fix iptables for travis")
    @Test(timeout = (36 * 300 * 1000))
    public void allNodesDisconnectedOneNodeReconnectedTest() {
        testAllNodesAllRecoverCombinations(true, 1);
    }

    /**
     * Test cluster behavior after all nodes are disconnected and just two is reconnected afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Reconnect two servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    //@Ignore("Fix iptables for travis")
    @Test(timeout = (36 * 300 * 1000))
    public void allNodesDisconnectedQuorumNodesReconnectedSequentiallyTest() {
        testAllNodesAllRecoverCombinations(true, getQuorumAmountOfNodes());
    }

    /**
     * Test cluster behavior after all nodes are disconnected and just two is reconnected afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Reconnect two servers concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    //@Ignore("Fix iptables for travis")
    @Test(timeout = (36 * 300 * 1000))
    public void allNodesDisconnectedQuorumNodesReconnectedConcurrentlyTest() {
        testAllNodesAllRecoverCombinations(false, getQuorumAmountOfNodes());
    }

    /**
     * Test cluster behavior after all nodes are disconnected and all is reconnected afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Reconnect all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    //@Ignore("Fix iptables for travis")
    @Test(timeout = (36 * 300 * 1000))
    public void allNodesDisconnectedAllNodesReconnectedSequentiallyTest() {
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after all nodes are disconnected and all is reconnected afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Reconnect all servers concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    //@Ignore("Fix iptables for travis")
    @Test(timeout = (48 * 300 * 1000))
    public void allNodesDisconnectedAllNodesReconnectedConcurrentlyTest() {
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }
}
