package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusteringIT extends AbstractIT {

    private static String singleNodeHost;

    @Before
    public void loadProperties() {
        singleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
    }

    private String getEndpoint(int port) {
        return singleNodeHost + ":" + port;
    }

    @Test
    public void testClusterIdChangeForConnectedRuntimeDoesNotThrowWCE() throws Exception {

        Optional<CorfuRuntime> runtime = Optional.empty();
        try {
            final int port0 = 9000;
            Process corfuServer1 = runPersistentServer(singleNodeHost, port0, true);
            final int port1 = 9001;
            Process corfuServer2 = runPersistentServer(singleNodeHost, port1, true);
            final int port2 = 9002;
            Process corfuServer3 = runPersistentServer(singleNodeHost, port2, true);
            runtime = Optional.of(createRuntime(getEndpoint(port0)));
            boolean secondNodeReset =
                    runtime.get().getLayoutView()
                            .getRuntimeLayout()
                            .getBaseClient(getEndpoint(port1))
                            .reset()
                            .join();


            assertThat(secondNodeReset).isTrue();

            Duration timeout1 = Duration.ofSeconds(5);
            Duration timeout2 = Duration.ofSeconds(1);
            int retries = 3;
            try {
//                Sleep.sleepUninterruptibly(Duration.ofSeconds(8));
//
//                runtime.get().getManagementView().addNode(getEndpoint(port1), retries, timeout1, timeout2);
//                Layout layout1 = runtime.get().getLayoutView().getLayout();
//                System.out.println("Added layout: " + layout1);
            }
            catch (Exception e) {
                System.out.println("Threw " + e);
            }
            Sleep.sleepUninterruptibly(Duration.ofSeconds(60));
        }
        finally {
            runtime.ifPresent(CorfuRuntime::shutdown);
        }





    }
}
