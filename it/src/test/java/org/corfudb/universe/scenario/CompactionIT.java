package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

@Slf4j
public class CompactionIT extends GenericIntegrationTest {

    public static UniverseWorkflow<Fixture<UniverseParams>> wf;
    public static CorfuCluster corfuCluster;
    private final ThreadLocal threadLocal = new ThreadLocal();
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(50);

    private final long TABLE_NUM = 10;
    private final int MAP_SIZE = 10;

    private void worker() {
        final BigInteger count = (BigInteger) threadLocal.get();
        final int VALUE_LEN = 2;
        final BigInteger nextCount = count.add(BigInteger.valueOf(VALUE_LEN));
        if (count.longValue() % 50 == 0) {
            log.warn("Current counter == {}", count);
        }

        final long threadId = Thread.currentThread().getId();
        final CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

        corfuClient.getRuntime().getObjectsView().executeTx(() -> {
                    for (int idx = 0; idx < TABLE_NUM; idx++) {
                        if (idx == 0) continue;
                        final CorfuTable<String, String> table = corfuClient
                                .createDefaultCorfuTable(DEFAULT_STREAM_NAME + idx);
                        for (int key = 0; key < MAP_SIZE; key++) {
                            String currentKey = threadId + "_" + key;
                            BigInteger prev = new BigInteger(table.getOrDefault(currentKey, "0"));
                            Assertions.assertEquals(prev, count);
                            table.put(currentKey, nextCount.toString());
                        }
                    }
                });
        threadLocal.set(nextCount);
        corfuClient.shutdown();
    }

    private void trim(Token address) {
        try {
            final CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            corfuClient.getRuntime().getAddressSpaceView().prefixTrim(address);

            corfuClient.shutdown();
        } finally {
            executorService.schedule(this::checkpoint, 30, TimeUnit.SECONDS);

        }
    }

    private void checkpoint() {
        try {
            final CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();

            for (int idx = 0; idx < TABLE_NUM; idx++) {
                final CorfuTable<String, String> table = corfuClient
                        .createDefaultCorfuTable(DEFAULT_STREAM_NAME + idx);
                mcw.addMap(table);
            }

            Token address = mcw.appendCheckpoints(corfuClient.getRuntime(), "A1");
            executorService.schedule(() -> trim(address), 30, TimeUnit.SECONDS);
            corfuClient.shutdown();
        } catch (Throwable err) {
            log.error("Received an exception during check-pointing.", err);
            err.printStackTrace();
            executorService.schedule(this::checkpoint, 30, TimeUnit.SECONDS);
        }

    }

    public void induceFailure() {
        //Should fail two links and then heal
        List<CorfuServer> servers = Arrays.asList(corfuCluster.getServerByIndex(0),
                corfuCluster.getServerByIndex(1),
                corfuCluster.getServerByIndex(2));

        Collections.shuffle(servers);
        List<CorfuServer> tail = servers.subList(1, servers.size());
        CorfuServer head = servers.get(0);

        try {
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            // Disconnect server0 with server1 and server2
            head.disconnect(tail);
            log.warn("Inducing a faiure.");
            waitForLayoutChange(layout -> layout.getUnresponsiveServers()
                    .equals(Collections.singletonList(head.getEndpoint())), corfuClient);

            // Cluster status should be DEGRADED after one node is marked unresponsive
            ClusterStatusReport clusterStatusReport = corfuClient
                    .getManagementView()
                    .getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatusReport.ClusterStatus.DEGRADED);

            // Repair the link failure between server0 and others
            head.reconnect(tail);
            waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

            log.error("The cluster if fully healed.");
            corfuClient.shutdown();
        } catch (Throwable err) {
            log.error("Received an exception during failure injection/recovery.", err);
            err.printStackTrace();
        } finally {
            while (true) {
                try {
                    head.reconnect(tail);
                    break;
                } catch (Throwable err) {
                    log.error("Trying to reconnect.", err);
                }
            }
            executorService.schedule(this::induceFailure, 10, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 3600000L)
    public void oneNodeDownTest() {
        workflow(wf -> {
            wf.deploy();
            corfuCluster = wf.getUniverse().getGroup(
                    wf.getFixture().data().getGroupParamByIndex(0).getName());

            final int WORKER_THREADS = 10;
            List<Thread> workerThreads = new ArrayList<>();
            for (int i = 0; i < WORKER_THREADS; i++) {
               Thread thread = new Thread(() -> {
                   threadLocal.set(BigInteger.valueOf(0));
                   while (true) {
                        try {
                            worker();
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            log.error("Thread got interrupted.", e);
                        } catch (Throwable err) {
                            log.error("Received exception.", err);
                        }
                    }
                });
               thread.start();
               workerThreads.add(thread);
            }

            executorService.schedule(this::checkpoint, 30, TimeUnit.SECONDS);
            executorService.schedule(this::checkpoint, 20, TimeUnit.SECONDS);
            executorService.schedule(this::induceFailure, 10, TimeUnit.SECONDS);
            workerThreads.forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
