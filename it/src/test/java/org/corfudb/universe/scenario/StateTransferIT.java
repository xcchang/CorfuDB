package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

@Slf4j
public class StateTransferIT extends GenericIntegrationTest {
    @Test(timeout = 300000)
    public void testLargeStateTransfer(){
        workflow(wf -> {
            wf.setupDocker(fix -> fix.getLogging().enabled(true));
            wf.deploy();
            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            server0.disconnect();
            log.info("Waiting for unresponsive servers change.");
            waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

            log.info("Layout before write: {}.", corfuClient.getLayout());
            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            log.info("Writing data.");
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            log.info("Restore connection");
            server0.reconnect();
            waitForLayoutChange(layout -> layout.getSegments().size() == 1, corfuClient);

        });
    }
}
