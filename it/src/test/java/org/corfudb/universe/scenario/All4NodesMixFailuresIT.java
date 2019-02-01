package org.corfudb.universe.scenario;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class All4NodesMixFailuresIT extends AllNodesBaseIT {

    private List<FailureType> currentFailuresByCorfuServerIndex = new ArrayList<>();

    @Override
    protected FailureType getFailureType(int corfuServerIndex){
        return currentFailuresByCorfuServerIndex.get(corfuServerIndex);
    }

    @Override
    protected boolean useOneUniversePerTest(){
        return true;
    }

    @Override
    protected int getAmountOfNodes(){
        return currentFailuresByCorfuServerIndex.size();
    }

    /**
     * Test cluster behavior after one node disconnected and one down.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: one node disconnected and one down
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopOnePartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after one node disconnected and one down.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: one node disconnected and one down
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopOnePartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes disconnected and one down.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes disconnected and one down
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopTwoPartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes disconnected and one down.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes disconnected and one down
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopTwoPartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and one disconnected.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and one disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void twoStopOnePartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and one disconnected.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and one disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void twoStopOnePartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.NONE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and two disconnected.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and two disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void twoStopTwoPartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and two disconnected.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and two disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void twoStopTwoPartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after three nodes down and one disconnected.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: three nodes down and one disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void threeStopOnePartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after three nodes down and one disconnected.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: three nodes down and one disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void threeStopOnePartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and three disconnected.
     * The healing is executed sequentially.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and three disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers sequentially
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopThreePartionedNodesSequentiallyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        testAllNodesAllRecoverCombinations(true, getAmountOfNodes());
    }

    /**
     * Test cluster behavior after two nodes down and three disconnected.
     * The healing is executed concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially execute the failures: two nodes down and three disconnected
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Heal all servers Concurrently
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    @Test(timeout = (48 * 300 * 1000))
    public void oneStopThreePartionedNodesConcurrentlyTest() {
        currentFailuresByCorfuServerIndex.clear();
        currentFailuresByCorfuServerIndex.add(FailureType.STOP_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        currentFailuresByCorfuServerIndex.add(FailureType.DISCONNECT_NODE);
        testAllNodesAllRecoverCombinations(false, getAmountOfNodes());
    }
}
