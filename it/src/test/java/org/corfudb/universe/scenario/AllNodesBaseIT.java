package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.Sleep;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;
import static org.junit.Assert.fail;

/**
 * Base class of all combinatorial tests intended to test the link failure module.
 * A test for this class is a set of failures, one per node, and and set of combination-permutations of these
 * failures. A example of one test:
 * - Set of failures: Stop, Discconect, None.
 * - Set of combinations-permutations 2 on 3: 0-1, 1-0, 0-2, 2-0, 1-2, 2-1
 * The test can be performed in two ways:
 * - One cluster per combination-permutation of failures.
 * - One cluster for all combinations-permutations of failures.
 * This behaviour is specified by the sub-class through the method {@link AllNodesBaseIT#useOneUniversePerTest()}.
 *
 * The class offers a general behaviour that goes as follow:
 * 1) Deploy and bootstrap a three nodes cluster
 * 2) Sequentially execute the specified failures for each node. The sub-class must
 * implement the method {@link AllNodesBaseIT#getFailureType(int)}. It possible to return None to
 * inidicate no failure for a specific node, but just one of the nodes could have this "failure type".
 * 3) Verify cluster status is unavailable, node status are down and data path is not available
 * 4) Heal the nodes accordingly to the failure associated to it through {@link AllNodesBaseIT#getFailureType(int)}.
 * 5) Wait for the new layout is available
 * 6) Verify the amount of active servers, the cluster status and data path
 * operations. The expected result of these verifications depends on the amount and type of failures.
 *
 * The class also offers the possibility to be greedy or eager in the test process:
 * - Eager: the hole test must fail as soon as a verification in the step 6 failed in one of
 * the combinations-permutations.
 * - Greedy: the hole test must run across all the combinations-permutations and just fail at the end.
 * This behaviour can be controlled by the sub-classes through a implementation of the
 * method {@link AllNodesBaseIT#shouldFailEager()}.
 */
@Slf4j
public abstract class AllNodesBaseIT extends GenericIntegrationTest {

    /**
     * Amount of retries made before fail because a unexpected status in the cluster report.
     */
    static final int DEFAULT_CLUSTER_REPORT_POLL_ITER = 10;
    /**
     * Delay between the failures to apply.
     */
    static final Duration DELAY_BETWEEN_SEQUENTIAL_FAILURES = Duration.ofMillis(1200);
    /**
     * Maximum number of unexpected exceptions to handle before failing for a data path operation.
     */
    static final int MAXIMUN_UNEXPECTED_EXCEPTIONS = 20;
    /**
     * Max amount of times that a failure or heal operation is attempted.
     */
    static final int MAX_FAILURES_RECOVER_RETRIES = 5;
    /**
     * Indicates if the criteria to wait for new layout (step 5) should include a change in the epoch
     */
    static final boolean WAIT_FOR_EPOCH_CHANGE = true;
    /**
     * Delay between retries in the process of waiting for a layout chage (step 5).
     */
    static final Duration SLEEP_BEFORE_GET_CLUSTER_STATUS = Duration.ofMillis(2000);
    /**
     * Maximum number of times that a combination-permutation in a test case is retried
     * if it fails because a problem with a container or vm
     */
    static final int MAX_NODES_FAILURES_RETRIES = 3;
    /**
     * Delay between two creations of universe sceneries (dockers or vms).
     */
    static final Duration SLEEP_BEFORE_CREATE_NEW_UNIVERSE = Duration.ofMillis(2000);


    /**
     * Get the failure type that should be used for the specified corfu server.
     *
     * @param corfuServerIndex Index of the ser in the corfu server list.
     * @return Type of failure to force in the server node specified.
     */
    protected abstract FailureType getFailureType(int corfuServerIndex);

    /**
     * Indicates whether one cluster must be used for all combinations-permutations.
     *
     * @return Whether one cluster must be used for all combinations-permutations.
     */
    protected abstract boolean useOneUniversePerTest();

    /**
     * Get the amount of nodes that must be used for the current test.
     *
     * @return amount of nodes for the current test.
     */
    protected abstract int getAmountOfNodes();

    /**
     * Get the amount of quorum nodes as a function of {@link AllNodesBaseIT#getAmountOfNodes}.
     *
     * @return amount of quorum nodes.
     */
    protected int getQuorumAmountOfNodes(){
        return (getAmountOfNodes() / 2) + 1;
    }

    /**
     * Indicated whether the test must fail as soon as a verification fails or
     * the hole test must run across all the combinations-permutations and just fail at the end.
     *
     * @return whether the test must fail as soon as a verification fails or not.
     */
    protected boolean shouldFailEager() {
        return false;
    }

    protected void testAllNodesAllRecoverCombinations(boolean startServersSequentially, int amountUp){
        testAllNodesAllRecoverCombinations(startServersSequentially, amountUp, true);
    }

    protected void testAllNodesAllRecoverCombinations(boolean startServersSequentially, int amountUp,
                                                      boolean permuteCombinations){
        if(useOneUniversePerTest()){
            testAllNodesAllRecoverCombinationsInOneUniverse(startServersSequentially, amountUp, permuteCombinations);
        }else{
            testAllNodesAllRecoverCombinationsInDifferentUniverses(startServersSequentially, amountUp, permuteCombinations);
        }
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified amount is fix afterwards.
     * One universe is created for each test case. A test case is a specific combination-permutation
     * of recovered servers.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private void testAllNodesAllRecoverCombinationsInDifferentUniverses(boolean startServersSequentially, int amountUp,
                                                                        boolean permuteCombinations) {
        ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected = new ArrayList<>();
        if (amountUp >= getQuorumAmountOfNodes()) {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.STABLE);
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.DEGRADED);
        } else {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
        }

        ArrayList<CombinationResult> testResult = new ArrayList();
        ArrayList<ArrayList<Integer>> combinationsAndPermutations = combine(getAmountOfNodes(), amountUp, permuteCombinations);

        ClientParams clientParams = ClientParams.builder()
                .systemDownHandlerTriggerLimit(10)
                .requestTimeout(Duration.ofSeconds(5))
                .idleConnectionTimeout(30)
                .connectionTimeout(Duration.ofMillis(500))
                .connectionRetryRate(Duration.ofMillis(1000))
                .build();

        for (ArrayList<Integer> combination : combinationsAndPermutations) {
            getScenario(getAmountOfNodes()).describe((fixture, testCase) -> {
                CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

                CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(clientParams);

                CorfuTable<String, String> table = initializeCorfuTable(corfuClient);

                ArrayList<CorfuServer> nodes = new ArrayList<>(corfuCluster.<CorfuServer>nodes().values());

                String combinationName = combination.stream().map(i -> i.toString()).
                        collect(Collectors.joining(" - "));
                testCase.it(getTestCaseDescription(startServersSequentially, combinationName), data -> {
                    CombinationResult result = testAllNodesWithOneRecoverCombination(combination, combinationName, clusterStatusesExpected,
                            (amountUp < getQuorumAmountOfNodes()), startServersSequentially, nodes, corfuClient, table);
                    testResult.add(result);
                });
            });
            universe.shutdown();
        }
        executeFinalAssertations(testResult, combinationsAndPermutations);
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified amount is fix afterwards.
     * One single universe is re-used for all test cases. A test case is a specific combination-permutation
     * of recovered servers.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private void testAllNodesAllRecoverCombinationsInOneUniverse(boolean startServersSequentially, int amountUp, boolean permuteCombinations) {
        ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected = new ArrayList<>();
        if (amountUp >= getQuorumAmountOfNodes()) {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.STABLE);
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.DEGRADED);
        } else {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
        }

        ArrayList<CombinationResult> testResult = new ArrayList();
        ArrayList<ArrayList<Integer>> combinationsAndPermutations = combine(getAmountOfNodes(), amountUp, permuteCombinations);

        ArrayList<CorfuServer> nodes = null;
        CorfuClient corfuClient = null;
        CorfuTable<String, String> table = null;

        for (ArrayList<Integer> combination : combinationsAndPermutations) {
            String combinationName = combination.stream().map(i -> i.toString()).
                    collect(Collectors.joining(" - "));
            for (int i = 0; i < MAX_NODES_FAILURES_RETRIES; i++) {
                log.info("Execute test: {}. Attempt {}", getTestCaseDescription(startServersSequentially, combinationName), i);
                try {
                    if (currentCorfuCluster == null) {
                        if(docker == null)
                            docker = DefaultDockerClient.fromEnv().build();
                        initializeCorfuCluster();
                        Sleep.sleepUninterruptibly(Duration.ofMillis((i+1)*250));
                        nodes = new ArrayList<>(currentCorfuCluster.<CorfuServer>nodes().values());
                        corfuClient = getCorfuClient();
                        table = initializeCorfuTable(corfuClient);
                    }
                    CombinationResult result = testAllNodesWithOneRecoverCombination(combination, combinationName, clusterStatusesExpected,
                            (amountUp < getQuorumAmountOfNodes()), startServersSequentially, nodes, corfuClient, table);
                    testResult.add(result);
                    if (!result.imposibleToExecuteFailure && !result.imposibleToExecuteFix) {
                        break;
                    } else if (i < (MAX_NODES_FAILURES_RETRIES - 1)) {
                        log.info("Restarting universe...");
                        shutdownUniverse();
                    }
                }catch (Exception ex){
                    log.error(String.format("Error executing test combination", combinationName), ex);
                    CombinationResult result = new CombinationResult(getTestName(), getTimestamp(), combinationName, combination,
                            clusterStatusesExpected, nodes.stream().map(CorfuServer::getEndpoint).collect(Collectors.toList()),
                            table, -1, startServersSequentially);
                    result.failedWithUnhandledException = true;
                    testResult.add(result);
                    shutdownUniverse();
                }
            }
        }
        executeFinalAssertations(testResult, combinationsAndPermutations);
    }

    private void shutdownUniverse() {
        universe.shutdown();
        docker.close();
        universe = null;
        docker = null;
        currentCorfuCluster = null;
        Sleep.sleepUninterruptibly(SLEEP_BEFORE_CREATE_NEW_UNIVERSE);
    }

    private CorfuClient getCorfuClient() {
        ClientParams clientParams = ClientParams.builder()
                .systemDownHandlerTriggerLimit(10)
                .requestTimeout(Duration.ofSeconds(5))
                .idleConnectionTimeout(30)
                .connectionTimeout(Duration.ofMillis(500))
                .connectionRetryRate(Duration.ofMillis(1000))
                .build();
        return currentCorfuCluster.getLocalCorfuClient(clientParams);
    }

    private CorfuCluster currentCorfuCluster = null;

    private void setCurrentCorfuCluster(CorfuCluster corfuCluster){
        currentCorfuCluster = corfuCluster;
    }

    private void initializeCorfuCluster() {
        getScenario(getAmountOfNodes()).describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            setCurrentCorfuCluster(corfuCluster);
        });
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified combination is fix afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private CombinationResult testAllNodesWithOneRecoverCombination(ArrayList<Integer> combination, String combinationName,
                                                                    ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected,
                                                                    boolean dataPathExceptionExpected, boolean startServersSequentially,
                                                                    ArrayList<CorfuServer> nodes, CorfuClient corfuClient,
                                                                    CorfuTable<String, String> table) {
        long currentEpoch = corfuClient.getLayout().getEpoch();
        CombinationResult result = new CombinationResult(getTestName(), getTimestamp(), combinationName, combination,
                clusterStatusesExpected, nodes.stream().map(CorfuServer::getEndpoint).collect(Collectors.toList()),
                table, currentEpoch, startServersSequentially);
        for (int j= 0; j < nodes.size(); j++){
            result.failures.add(getFailureType(j));
        }
        // Force failure in all nodes sequentially without wait to much for it
        if(executeFailureInAllNodes(nodes)) {
            // Verify cluster status is UNAVAILABLE with all nodes NA and UNRESPONSIVE
            Sleep.sleepUninterruptibly(SLEEP_BEFORE_GET_CLUSTER_STATUS);
            log.info(String.format("Verify cluster status after failure in all nodes for test-combination %s", combination));
            ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            for (int i = 0; i < DEFAULT_CLUSTER_REPORT_POLL_ITER; i++) {
                result.addAfterFailureLayouts(clusterStatusReport.getLayout());
                if (clusterStatusReport.getClusterStatus() != ClusterStatusReport.ClusterStatus.UNAVAILABLE) {
                    log.info(String.format("Cluster status after failure in all nodes: %s", clusterStatusReport.getClusterStatus()));
                    Sleep.sleepUninterruptibly(SLEEP_BEFORE_GET_CLUSTER_STATUS);
                    clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                } else
                    break;
            }
            Map<String, ClusterStatusReport.ConnectivityStatus> connectionMap = clusterStatusReport.getClientServerConnectivityStatusMap();
            Map<String, ClusterStatusReport.NodeStatus> statusMap = clusterStatusReport.getClusterNodeStatusMap();
            log.info(String.format("Connection map after failure in all nodes: %s", connectionMap));
            log.info(String.format("Status map after failure in all nodes: %s", statusMap));
            int nodesStoped = 0;
            for (int i = 0; i < nodes.size(); i++) {
                CorfuServer s = nodes.get(i);
                switch (getFailureType(i)){
                    case STOP_NODE:
                        assertThat(connectionMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE);
                        assertThat(statusMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.NodeStatus.NA);
                        nodesStoped++;
                        break;
                    case DISCONNECT_NODE:
                        assertThat(connectionMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.ConnectivityStatus.RESPONSIVE);
                        //In this case is not clear which is the expected Node Status
                        break;
                    case NONE:
                        break;
                }
            }
            if(nodesStoped >= getQuorumAmountOfNodes()){
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
            }
            Layout afterStopLayout = clusterStatusReport.getLayout();
            log.info(String.format("Layout after all servers down: %s", afterStopLayout));

            //Verify data path
            // At this point, if the cluster is completely partitioned, the local corfu client is still capable
            // to communicate with every node.
            // It's not clear if under this conditions the data path operation should fail or should works.
            // That's why the implementation is as follow:
            // check if we are able to write, otherwise do not check any thing
            executeIntermediateCorfuTableWrites(result);

            // Start the combination of nodes
            log.info(String.format("Fix nodes for combination %s", combination));
            ExecutorService executor;
            if (startServersSequentially) {
                executor = null;
                combination.forEach(i -> {
                    CorfuServer corfuServer = nodes.get(i);
                    boolean recoverRes = recoverServer(corfuServer, i, "Error executing fix for failure (%s)",
                            "Imposible to execute fix");
                    result.setImposibleToExecuteFixWithOr(!recoverRes);
                });
            } else {
                executor = Executors.newFixedThreadPool(combination.size());
                combination.forEach(i -> executor.submit(() -> {
                    CorfuServer corfuServer = nodes.get(i);
                    boolean recoverRes = recoverServer(corfuServer, i, "Error executing fix for failure (%s)",
                            "Imposible to execute fix");
                    result.setImposibleToExecuteFixWithOr(!recoverRes);
                }));
            }

            // Verify cluster status is the expected
            log.info(String.format("Verify cluster status after fix the nodes combination %s", combination));
            for (int i = 0; i < DEFAULT_CLUSTER_REPORT_POLL_ITER; i++) {
                Sleep.sleepUninterruptibly(SLEEP_BEFORE_GET_CLUSTER_STATUS);
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                result.addAfterHealingLayouts(clusterStatusReport.getLayout());
                if (!clusterStatusesExpected.contains(clusterStatusReport.getClusterStatus())) {
                    log.info(String.format("Cluster status: %s", clusterStatusReport.getClusterStatus()));
                } else {
                    if(WAIT_FOR_EPOCH_CHANGE){
                        long expectedEpoch = (!clusterStatusesExpected.contains(ClusterStatusReport.ClusterStatus.UNAVAILABLE)) ?
                                currentEpoch + 1 : currentEpoch;
                        corfuClient.invalidateLayout();
                        Layout refreshedLayout = corfuClient.getLayout();
                        if(refreshedLayout != null && refreshedLayout.getEpoch() >= expectedEpoch)
                            break;
                        else
                            log.info(String.format("Waiting for epoch change after fix the nodes combination %s", combination));
                    }else {
                        break;
                    }
                }
            }

            log.info(String.format("Status for combination %s", combination));
            Layout afterStartLayout = clusterStatusReport.getLayout();
            log.info(String.format("Layout after servers up: %s", afterStartLayout));
            if (afterStartLayout != null) {
                result.activeServers = afterStartLayout.getAllActiveServers().size();
                result.unresposiveServers = afterStartLayout.getUnresponsiveServers().size();
                result.finalEpoch = afterStartLayout.getEpoch();
                log.info(String.format("Nodes active: %s", result.activeServers));
                log.info(String.format("Unresponsive Nodes: %s", result.unresposiveServers));
                log.info(String.format("Epochs: %s - %s", result.initialEpoch, result.finalEpoch));
            } else {
                log.info("Nodes active: None (layout is null)");
                result.activeServers = 0;
                result.unresposiveServers = 0;
                result.finalEpoch = -1;
            }
            result.connectionMap = clusterStatusReport.getClientServerConnectivityStatusMap();
            log.info(String.format("Connection map after fix the nodes: %s", result.connectionMap));
            result.statusMap = clusterStatusReport.getClusterNodeStatusMap();
            log.info(String.format("Status map after fix the nodes: %s", result.statusMap));
            result.clusterStatus = clusterStatusReport.getClusterStatus();
            log.info(String.format("Cluster status: %s", result.clusterStatus));
            if (shouldFailEager()) {
                result.assertResultStatus();
            }

            // Verify data path is working fine
            testDataPathGetAfterRecovering(combination, dataPathExceptionExpected, result);
            if (executor != null)
                executor.shutdownNow();
            // Recover nodes not present in the combination
            IntStream.range(0, nodes.size() - 1).filter(i -> !combination.contains(i)).forEach(j -> {
                recoverServer(nodes.get(j), j, "Error executing recovering from (%s)", "Imposible to execute recovering");
            });
        }else{
            result.imposibleToExecuteFailure = true;
        }
        return result;
    }

    private void executeFinalAssertations(ArrayList<CombinationResult> testResult, ArrayList<ArrayList<Integer>> combinationsAndPermutations) {
        List<String> summary = new ArrayList<>();
        summary.add("Summary of the test:");
        summary.add(CombinationResult.getDescriptionColumns());
        summary.addAll(testResult.stream().map(CombinationResult::getDescription).collect(Collectors.toList()));
        log.info(summary.stream().collect(Collectors.joining("\n")));
        for (CombinationResult r : testResult) {
            r.saveLayouts();
        }
        int imposibleToExecuteFailureCount = 0;
        int imposibleToExecuteFixCount = 0;
        for (CombinationResult r : testResult) {
            if(!r.imposibleToExecuteFailure && !r.imposibleToExecuteFix) {
                if (!shouldFailEager()) {
                    r.assertResults();
                }
            }else if(r.imposibleToExecuteFailure){
                imposibleToExecuteFailureCount++;
            }else{
                imposibleToExecuteFixCount++;
            }
        }
        float untestedCombinations = (float) (imposibleToExecuteFailureCount + imposibleToExecuteFixCount);
        float maxUntestedCombinationsAllowed = ((float)combinationsAndPermutations.size()) / 2;
        if(untestedCombinations > 0)
            log.warn(String.format("%s combination has not been tested", untestedCombinations));
        assertThat(untestedCombinations).isLessThan(maxUntestedCombinationsAllowed);
    }

    private String getTestCaseDescription(boolean startServersSequentially, String combinationName){
        String orderType = startServersSequentially ? "sequentially" : "concurrently";
        String description = String.format("Should put a failure in some nodes, %s heal the nodes %s and recover", orderType,
                combinationName);
        return description;
    }

    private boolean executeFailureInAllNodes(ArrayList<CorfuServer> nodes) {
        boolean res = false;
        List<Integer> stoppedNodes = new ArrayList<>();
        for (int j= 0; j < nodes.size(); j++) {
            CorfuServer s = nodes.get(j);
            for (int i = 0; i < MAX_FAILURES_RECOVER_RETRIES; i++) {
                Sleep.sleepUninterruptibly(DELAY_BETWEEN_SEQUENTIAL_FAILURES);
                try {
                    switch (getFailureType(j)) {
                        case STOP_NODE:
                            s.stop(Duration.ofNanos(1));
                            stoppedNodes.add(j);
                            break;
                        case DISCONNECT_NODE:
                            List<CorfuServer> noStoppedNodes = new ArrayList<>();
                            for (int k = 0; k < nodes.size(); k++) {
                                if(!stoppedNodes.contains(k))
                                    noStoppedNodes.add(nodes.get(k));
                            }
                            s.disconnect(noStoppedNodes);
                            break;
                        case NONE:
                            break;
                    }
                    res = true;
                    break;
                }catch (Exception ex){
                    log.error(String.format("Error executing failure (%s)", getFailureType(j)), ex);
                    res = false;
                }
            }
            if(!res)
                break;
        }
        return res;
    }

    private CorfuTable<String, String> initializeCorfuTable(CorfuClient corfuClient) {
        CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME);
        for (int i = 0; i < Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
            table.put(String.valueOf(i), String.valueOf(i));
        }
        return table;
    }

    private void executeIntermediateCorfuTableWrites(CombinationResult result) {
        for (int i = 0; i < 5; i++) {
            try {
                result.table.put(getIntermediateKey(result, i), getIntermediateValue(i));
                result.lastIntermediateWrite = i;
            }catch (Exception ex){
                log.warn(String.format("Exception writing intermediate data value %s: %s", i,
                        ex));
                break;
            }
        }
    }

    private String getIntermediateValue(int index) {
        return String.format("IntermediateValue%s", index);
    }

    private String getIntermediateKey(CombinationResult result, int index) {
        return String.format("%s-IntermediateKey-%s", result.combinationName, index);
    }

    private void testDataPathGetAfterRecovering(ArrayList<Integer> combination, boolean dataPathExceptionExpected, CombinationResult result) {
        log.info(String.format("Verify data path for combination %s", combination));
        int unexpectedExceptions = 0;
        for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            try {
                if (dataPathExceptionExpected) {
                    String unexpectedValue = result.table.get(String.valueOf(i));
                    String mess = String.format("Expected an UnreachableClusterException to be thrown but %s obtained",
                            unexpectedValue);
                    if (shouldFailEager()) {
                        fail(mess);
                    } else {
                        log.info(mess);
                        result.dataValidationResult.add(DataValidationResultType.EXPECTED_EXCEPTION_NO_RECEIVED_CHECKING_ORIGINAL_VALUE);
                    }
                } else {
                    if (shouldFailEager()) {
                        assertThat(result.table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                    } else {
                        boolean dataValidationResult = result.table.get(String.valueOf(i)).equals(String.valueOf(i));
                        if (!dataValidationResult) {
                            result.dataValidationResult.add(DataValidationResultType.INCORRECT_ORIGINAL_VALUE);
                            log.info(String.format("Incorrect value for %s", i));
                        }else
                            result.dataValidationResult.add(DataValidationResultType.SUCCESS);
                    }
                }

            } catch (Exception ex) {
                if (dataPathExceptionExpected) {
                    log.info(String.format("Exception checking data value %s: %s", i,
                            ex));
                    if (shouldFailEager()) {
                        assertThat(ex.getMessage()).startsWith("Cluster is unavailable");
                    } else {
                        if(ex.getMessage().startsWith("Cluster is unavailable")){
                            result.dataValidationResult.add(DataValidationResultType.SUCCESS);
                        }else {
                            result.dataValidationResult.add(DataValidationResultType.INCORRECT_EXCEPTION_CHECKING_ORIGINAL_VALUE);
                        }
                        break;
                    }
                } else {
                    unexpectedExceptions++;
                    log.info(String.format("Unexpected exception checking data value %s: %s", i,
                            ex));
                    result.dataValidationResult.add(DataValidationResultType.EXCEPTION_CHECKING_ORIGINAL_VALUE);
                    if(unexpectedExceptions >= MAXIMUN_UNEXPECTED_EXCEPTIONS)
                        break;
                }
            }
        }
        if(!dataPathExceptionExpected && result.lastIntermediateWrite >= 0){
            for (int i = 0; i < result.lastIntermediateWrite; i++) {
                try{
                    boolean dataValidationResult = result.table.get(getIntermediateKey(result, i)).
                            equals(getIntermediateValue(i));
                    if (!dataValidationResult) {
                        result.dataValidationResult.add(DataValidationResultType.INCORRECT_INTERMEDIATE_VALUE);
                        String mess = String.format("Incorrect intermediate value for %s", i);
                        if (shouldFailEager()){
                            fail();
                        }else{
                            log.info(mess);
                        }
                    }else{
                        result.dataValidationResult.add(DataValidationResultType.SUCCESS);
                    }
                }catch (Exception ex){
                    unexpectedExceptions++;
                    log.info(String.format("Unexpected exception checking intermediate data value %s: %s", i,
                            ex));
                    result.dataValidationResult.add(DataValidationResultType.EXCEPTION_CHECKING_INTERMEDIATE_VALUE);
                    if(unexpectedExceptions >= MAXIMUN_UNEXPECTED_EXCEPTIONS)
                        break;
                }
            }
        }
    }

    private boolean recoverServer(CorfuServer corfuServer, int corfuServerIndex, String s, String s2) {
        boolean res = false;
        for (int j = 0; j < MAX_FAILURES_RECOVER_RETRIES; j++) {
            Sleep.sleepUninterruptibly(DELAY_BETWEEN_SEQUENTIAL_FAILURES);
            try {
                switch (getFailureType(corfuServerIndex)) {
                    case STOP_NODE:
                        corfuServer.start();
                        break;
                    case DISCONNECT_NODE:
                        corfuServer.reconnect();
                        break;
                    case NONE:
                        break;
                }
                res = true;
                break;
            } catch (Exception ex) {
                log.error(String.format(s, getFailureType(corfuServerIndex)), ex);
                res = false;
            }
        }
        return res;
    }

    /**
     * Generate all the combinations  of k numbers with its respective permutations, of n numbers.
     *
     * @param n numbers tu combine
     * @param k amount of numbers per combination
     * @return n in k combinations with its respective permutations
     */
    private ArrayList<ArrayList<Integer>> combine(int n, int k, boolean permuteCombinations) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
        if (n <= 0 || n < k)
            return result;
        ArrayList<Integer> item = new ArrayList<Integer>();
        dfs(n, k, 0, item, result, permuteCombinations);
        return result;
    }

    /**
     * Recursively generates all combinations of k numbers with its respective permutations, of n numbers.
     *
     * @param n     numbers tu combine
     * @param k     amount of numbers per combination
     * @param start number used to start the recursion
     * @param item  current combination to fill
     * @param res   n in k combinations with its respective permutations
     */
    private void dfs(int n, int k, int start, ArrayList<Integer> item, ArrayList<ArrayList<Integer>> res,
                     boolean permuteCombinations) {
        if (item.size() == k) {
            if(permuteCombinations)
                permute(0, item, res);
            else
                res.add(new ArrayList<Integer>(item));
            return;
        }
        for (int i = start; i < n; i++) {
            item.add(i);
            dfs(n, k, i + 1, item, res, permuteCombinations);
            item.remove(item.size() - 1);
        }
    }

    /**
     * Permute the number indicate with the others in the list.
     *
     * @param i      position of the number to permute
     * @param nums   list of numbers
     * @param result the list were all permutations are added
     */
    private void permute(int i, ArrayList<Integer> nums, ArrayList<ArrayList<Integer>> result) {
        if (i == nums.size() - 1) {
            result.add(new ArrayList<Integer>(nums));
            return;
        }

        for (int j = i; j < nums.size(); j++) {
            swap(nums, i, j);
            permute(i + 1, nums, result);
            swap(nums, i, j);
        }
    }

    /**
     * Swap the numbers in the positions specified.
     *
     * @param nums list of numbers
     * @param i    position of the first number to swap
     * @param j    position of the second number to swap
     */
    private void swap(ArrayList<Integer> nums, int i, int j) {
        int t = nums.get(i);
        nums.set(i, nums.get(j));
        nums.set(j, t);
    }

    /**
     * Structure that holds parameters and results of a combination of server
     * to heal after servers failures
     */
    @Data
    private static class CombinationResult {
        //Description column names
        public final static String COMBINATION_COLUMN           = "Combination          ";
        public final static String CLUSTER_STATUS_COLUMN        = "Cluster Status       ";
        public final static String ACTIVE_NODES_COLUMN          = "Actives              ";
        public final static String UNRESPONSIVE_NODES_COLUMN    = "Unresponsives        ";
        public final static String UNREACHABLE_NODES_COLUMN     = "Unreachables         ";
        public final static String NODE_STATUS_COLUMN           = "Node Status          ";
        public final static String EPOCHS_COLUMN                = "Epochs               ";
        public final static String DATA_TESTS_FAILED_COLUMN     = "Failed Data Tests    ";
        public final static String COLUMN_SEPARATOR             = " | ";
        public final static String TEST_FAILED_CELL             = "Failed";
        public final static String TEST_EXCEPTION_CELL          = "Exception";

        //Parameters
        public final String testName;
        public final String timestamp;
        public final String combinationName;
        public final ArrayList<Integer> combination;
        public final ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected;
        public final List<String> endpoints;
        public final CorfuTable<String, String> table;
        public final long initialEpoch;
        public final boolean startServersSequentially;
        public int lastIntermediateWrite = -1;
        public List<FailureType> failures = new ArrayList<>();

        //Results
        private List<String> afterFailureLayouts = new ArrayList<>();
        private List<String> afterHealingLayouts = new ArrayList<>();
        public boolean failedWithUnhandledException = false;
        public long finalEpoch = 0;
        public boolean imposibleToExecuteFailure = false;
        public boolean imposibleToExecuteFix = false;
        public int activeServers = 0;
        public int unresposiveServers = 0;
        public ClusterStatusReport.ClusterStatus clusterStatus = ClusterStatusReport.ClusterStatus.UNAVAILABLE;
        public Map<String, ClusterStatusReport.ConnectivityStatus> connectionMap = null;
        public Map<String, ClusterStatusReport.NodeStatus> statusMap = null;
        public ArrayList<DataValidationResultType> dataValidationResult = new ArrayList<>();

        public void addAfterFailureLayouts(Layout afterFailureLayout){
            String jsonLayout = afterFailureLayout.asJSONString();
            if(afterFailureLayouts.isEmpty() ||
                    !jsonLayout.equals(afterFailureLayouts.get(afterFailureLayouts.size()-1))){
                afterFailureLayouts.add(jsonLayout);
            }
        }

        public void addAfterHealingLayouts(Layout afterHealingLayout){
            String jsonLayout = afterHealingLayout.asJSONString();
            if(afterHealingLayouts.isEmpty() ||
                    !jsonLayout.equals(afterHealingLayouts.get(afterHealingLayouts.size()-1))){
                afterHealingLayouts.add(jsonLayout);
            }
        }

        public static String getDescriptionColumns(){
            ArrayList<String> columnNames = new ArrayList<>();
            columnNames.add(COMBINATION_COLUMN);
            columnNames.add(CLUSTER_STATUS_COLUMN);
            columnNames.add(ACTIVE_NODES_COLUMN);
            columnNames.add(UNRESPONSIVE_NODES_COLUMN);
            columnNames.add(UNREACHABLE_NODES_COLUMN);
            columnNames.add(NODE_STATUS_COLUMN);
            columnNames.add(EPOCHS_COLUMN);
            columnNames.add(DATA_TESTS_FAILED_COLUMN);
            String header = columnNames.stream().collect(Collectors.joining(COLUMN_SEPARATOR));
            return header;
        }

        public String getDescription(){
            ArrayList<String> columns = new ArrayList<>();
            columns.add(String.format("%" + COMBINATION_COLUMN.length() + "s", combinationName));
            if(!imposibleToExecuteFailure && !imposibleToExecuteFix && !failedWithUnhandledException) {
                columns.add(String.format("%" + CLUSTER_STATUS_COLUMN.length() + "s", clusterStatus));
                columns.add(String.format("%" + ACTIVE_NODES_COLUMN.length() + "s", activeServers));
                columns.add(String.format("%" + UNRESPONSIVE_NODES_COLUMN.length() + "s", unresposiveServers));
                long unreachableNodes = connectionMap.values().stream().
                        filter(s -> s.equals(ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE)).count();
                columns.add(String.format("%" + UNREACHABLE_NODES_COLUMN.length() + "s", unreachableNodes));
                String nodeStatus = statusMap.values().stream().map(s -> String.format("%4s", s)).
                        collect(Collectors.joining("|"));
                columns.add(String.format("%" + NODE_STATUS_COLUMN.length() + "s", nodeStatus));
                String epochs = String.format("%s - %s", initialEpoch, finalEpoch);
                columns.add(String.format("%" + EPOCHS_COLUMN.length() + "s", epochs));
                long dataTestsFailed = dataValidationResult.stream().filter(r -> !r.equals(DataValidationResultType.SUCCESS)).count();
                columns.add(String.format("%" + DATA_TESTS_FAILED_COLUMN.length() + "s", dataTestsFailed));
            }
            else{
                String mess = failedWithUnhandledException ? TEST_EXCEPTION_CELL : TEST_FAILED_CELL;
                columns.add(String.format("%" + CLUSTER_STATUS_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + ACTIVE_NODES_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + UNRESPONSIVE_NODES_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + UNREACHABLE_NODES_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + NODE_STATUS_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + EPOCHS_COLUMN.length() + "s", mess));
                columns.add(String.format("%" + DATA_TESTS_FAILED_COLUMN.length() + "s", mess));
            }
            String row = columns.stream().collect(Collectors.joining(COLUMN_SEPARATOR));
            return row;
        }

        public synchronized void setImposibleToExecuteFixWithOr(boolean valueToOr){
            imposibleToExecuteFix = (valueToOr || imposibleToExecuteFix);
        }

        public void assertResultStatus() {
            assertThat(clusterStatus).isIn(clusterStatusesExpected);
            combination.forEach(i -> assertThat(connectionMap.get(endpoints.get(i))).isEqualTo(ClusterStatusReport.ConnectivityStatus.RESPONSIVE));
            //The validation of node status and active servers can be performed only if the cluster is in a
            //status different than unavailable
            if(!clusterStatusesExpected.contains(ClusterStatusReport.ClusterStatus.UNAVAILABLE)) {
                combination.forEach(i -> assertThat(statusMap.get(endpoints.get(i))).isEqualTo(ClusterStatusReport.NodeStatus.UP));
                assertThat(activeServers).isEqualTo(combination.size());
                //The layout should change or at least stay the same
                assertThat(initialEpoch).isLessThanOrEqualTo(finalEpoch);
            }
        }

        public void assertResults() {
            dataValidationResult.forEach(s -> assertThat(s).isEqualTo(DataValidationResultType.SUCCESS));
            assertResultStatus();
        }

        public void saveLayouts(){
            String orderType = startServersSequentially ? "sequentially" : "concurrently";
            String testCombinationName = failures.stream().map(f -> f.toString()).collect(Collectors.joining("-"));
            testCombinationName = testCombinationName + "-" + combinationName + "-" + orderType;
            Path logDirPath = LoggingParams.getClientLogDir(testName, timestamp);
            logDirPath = logDirPath.resolve(testCombinationName);
            File logDirFile = logDirPath.toFile();
            try {
                if (!logDirFile.exists() && logDirFile.mkdirs()) {
                    log.info("Created new client corfu log directory at {}.", logDirFile);
                }
                for (int i = 0; i < afterFailureLayouts.size(); i++) {
                    Path filePathObj = logDirPath.resolve(String.format("after-failure-layout%d.json",i));
                    String layoutStr = afterFailureLayouts.get(i);
                    Files.write(filePathObj, layoutStr.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            }catch (Exception ex){
                log.error("Error logging layouts collected after the failure.", ex);
            }
            try {
                for (int i = 0; i < afterHealingLayouts.size(); i++) {
                    Path filePathObj = logDirPath.resolve(String.format("after-healing-layout%d.json",i));
                    String layoutStr = afterHealingLayouts.get(i);
                    Files.write(filePathObj, layoutStr.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            }catch (Exception ex){
                log.error("Error logging layouts collected after the healing.", ex);
            }
        }
    }

    protected enum FailureType {
        STOP_NODE,
        DISCONNECT_NODE,
        NONE
    }

    protected enum DataValidationResultType {
        EXPECTED_EXCEPTION_NO_RECEIVED_CHECKING_ORIGINAL_VALUE,
        INCORRECT_EXCEPTION_CHECKING_ORIGINAL_VALUE,
        EXCEPTION_CHECKING_ORIGINAL_VALUE,
        EXCEPTION_CHECKING_INTERMEDIATE_VALUE,
        INCORRECT_ORIGINAL_VALUE,
        INCORRECT_INTERMEDIATE_VALUE,
        SUCCESS
    }
}
