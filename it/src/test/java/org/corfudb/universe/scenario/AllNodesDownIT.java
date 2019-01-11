package org.corfudb.universe.scenario;

import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AllNodesDownIT extends AllNodesBaseIT {

    @Override
    protected FailureType getFailureType(int corfuServerIndex){
        return FailureType.STOP_NODE;
    }

    @Override
    protected boolean useOneUniversePerTest(){
        return true;
    }

    /**
     * Test cluster behavior after all nodes are are down and just one is up afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is UNAVAILABLE and the data path
     * operations failed with UnreachableClusterException
     */
    @Test(timeout = (3 * 300 * 1000))
    public void allNodesDownOneNodeUpTest() {
        testAllNodesAllRecoverCombinations(true, 1);
    }

    /**
     * Test cluster behavior after all nodes are are down and just two is up afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart two servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (3 * 300 * 1000))
    public void allNodesDownQuorumNodesUpSequentiallyTest() {
        testAllNodesAllRecoverCombinations(true, QUORUM_AMOUNT_OF_NODES);
    }

    /**
     * Test cluster behavior after all nodes are are down and just two is up afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart two servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (3 * 300 * 1000))
    public void allNodesDownQuorumNodesUpConcurrentlyTest() {
        testAllNodesAllRecoverCombinations(false, QUORUM_AMOUNT_OF_NODES);
    }

    /**
     * Test cluster behavior after all nodes are are down and all is up afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (3 * 300 * 1000))
    public void allNodesDownAllNodesUpSequentiallyTest() {
        testAllNodesAllRecoverCombinations(true, DEFAULT_AMOUNT_OF_NODES);
    }

    /**
     * Test cluster behavior after all nodes are are down and all is up afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart all servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (3 * 300 * 1000))
    public void allNodesDownAllNodesUpConcurrentlyTest() {
        testAllNodesAllRecoverCombinations(false, DEFAULT_AMOUNT_OF_NODES);
    }
}
