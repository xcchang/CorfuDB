package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HeavyLoadIT extends GenericIntegrationTest {

    public static final int SIZE = 1024 * 1024;
    private static final String DATA = generate(SIZE);

    @Test(timeout = 300_000_000)
    public void test() {

        final int iteration = 1_000 * 5;

        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < iteration; i++) {
                System.out.println("Written mb: " + (i +1));
                table.put(String.valueOf(i), DATA);
            }

            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            CorfuServer server1 = corfuCluster.getServerByIndex(1);
            CorfuServer server2 = corfuCluster.getServerByIndex(2);

            for (int i = 0; i < 3; i++) {
                // Stop one node and wait for layout's unresponsive servers to change
                System.out.println("Stop and start server");
                stopServer(corfuClient, server0);
                sleepMin(1);
                startServer(corfuClient, server0);
            }

            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            sleepMin(100);

            // Verify data path working fine
            //for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            //  assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            //}

            corfuClient.shutdown();
        });
    }

    private void startServer(CorfuClient corfuClient, CorfuServer server) {
        server.start();
        waitForUnresponsiveServersChange(size -> size == 0, corfuClient);
    }

    private void stopServer(CorfuClient corfuClient, CorfuServer server) {
        server.stop(Duration.ofSeconds(10));
        waitForUnresponsiveServersChange(size -> size == 1, corfuClient);
        Layout layout = corfuClient.getLayout();
        assertThat(layout.getUnresponsiveServers()).containsExactly(server.getEndpoint());
    }

    private void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            throw new IllegalStateException("yay");
        }
    }

    private void sleepMin(int min) {
        try {
            TimeUnit.MINUTES.sleep(min);
        } catch (InterruptedException e) {
            throw new IllegalStateException("yay");
        }
    }

    private static String generate(int size) {
        Random rnd = new Random();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < size; j++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }

        return sb.toString();
    }
}
