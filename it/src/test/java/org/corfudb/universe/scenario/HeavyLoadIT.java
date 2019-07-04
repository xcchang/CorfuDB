package org.corfudb.universe.scenario;

import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class HeavyLoadIT extends GenericIntegrationTest {

    public static final int SIZE = 1024 * 512;
    private static final String DATA = generate(SIZE);
    private static final Random RND = new SecureRandom();

    @Test(timeout = 300_000_000)
    public void test() {

        final int iteration = 1_000 * 3;

        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);

            CompletableFuture<Void> asyncFail = CompletableFuture.completedFuture(null);

            for (int i = 0; i < iteration; i++) {
                System.out.println("Written mb: " + ((i + 1) / 2) +
                        ", epoch: " + corfuClient.getLayout().getEpoch() +
                        ", " + corfuClient.getLayout().getUnresponsiveServers()
                );
                table.put(String.valueOf(i), DATA);

                if (i > 0 && i % 200 == 0) {
                    asyncFail = asyncFail.thenCompose(v -> {
                        return CompletableFuture.supplyAsync(() -> {
                            sleep(RND.nextInt(5));
                            CorfuServer server = corfuCluster.getServerByIndex(Math.max(1, RND.nextInt(3)));
                            System.out.println("!!!Stop and start server: " + server.getEndpoint());
                            stopServer(corfuClient, server);
                            sleep(RND.nextInt(15));
                            System.out.println("!!!New epoch: " + corfuClient.getLayout().getEpoch() +
                                    ", unresponsive: " + corfuClient.getLayout().getUnresponsiveServers());
                            startServer(corfuClient, server);
                            return null;
                        });
                    });
                }
            }

            //for cycle - read all the data from the cluster. I suspect data loss.
            // State transfer superfast, should't work so fast.

            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            while (true) {
                sleep(10);
                System.out.println("Async failures done: " + asyncFail.isDone());
                corfuClient.invalidateLayout();
                final Layout layout = corfuClient.getLayout();

                if (asyncFail.isDone() && layout.getUnresponsiveServers().isEmpty() && layout.getSegments().size() == 1) {
                    for (int i = 0; i < iteration; i++) {
                        try {
                            table.get(String.valueOf(i));
                        } catch (Exception ex) {
                            throw new IllegalStateException("DATA LOSS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! addr: " + i);
                        }
                    }

                    System.out.println("DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    sleepMin(1000000);
                }

                System.out.println("LAYOUT!!! " + layout.getEpoch() + ", unresponsive: " + layout.getUnresponsiveServers());
            }

            // Verify data path working fine
            //for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            //  assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            //}

            //corfuClient.shutdown();
        });
    }

    private void startServer(CorfuClient corfuClient, CorfuServer server) {
        server.start();
        waitForUnresponsiveServersChange(size -> size == 0, corfuClient);
    }

    private void stopServer(CorfuClient corfuClient, CorfuServer server) {
        server.stop(Duration.ofSeconds(10));
        //waitForUnresponsiveServersChange(size -> size == 1, corfuClient);
        //Layout layout = corfuClient.getLayout();
        //assertThat(layout.getUnresponsiveServers()).containsExactly(server.getEndpoint());
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
