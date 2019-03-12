package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

public class PerformanceIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node paused
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Pause one node (hang the jvm process)
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by resuming the paused node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void Tx() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            corfuClient.getRuntime().getObjectsView().TXBegin();
            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }
            corfuClient.getRuntime().getObjectsView().TXEnd();


            corfuClient.getRuntime().getObjectsView().TXBegin();
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i) + "x", String.valueOf(i));
            }
            corfuClient.getRuntime().getObjectsView().TXEnd();


            corfuClient.shutdown();
        });
    }
}
