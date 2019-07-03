package org.corfudb.universe.scenario;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.ServerUtil;
import org.corfudb.universe.node.server.docker.DockerCorfuServer;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.docker.DockerUniverse;
import org.corfudb.universe.universe.docker.DockerUniverse.DockerNetwork;
import org.corfudb.universe.universe.docker.FakeDns;
import org.corfudb.universe.util.DockerManager;
import org.corfudb.util.JsonUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class JoinClusterIT extends GenericIntegrationTest {
    /**
     * Test cluster join test
     * <p>
     * 1) Bootstrap a cluster with three nodes.
     * 2) Create a node in another cluster.
     * 3) Reset a node in the second cluster.
     * 4) Reinitialize that node.
     * 5) Add that node to cluster.
     */

    @Test(timeout = 300000)
    public void joinClusterTest() throws InterruptedException {

        UniverseParams universeParams = UniverseParams.universeBuilder().build();

        CorfuServerParams params = CorfuServerParams.serverParamsBuilder()
                .port(ServerUtil.getRandomOpenPort()).clusterName("a").build();
        CorfuServerParams params1 = CorfuServerParams.serverParamsBuilder()
                .port(ServerUtil.getRandomOpenPort()).clusterName("a").build();
        CorfuServerParams params2 = CorfuServerParams.serverParamsBuilder()
                .port(ServerUtil.getRandomOpenPort()).clusterName("a").build();
        CorfuServerParams params3 = CorfuServerParams.serverParamsBuilder()
                .port(ServerUtil.getRandomOpenPort()).clusterName("b").build();

        DockerCorfuServer server = DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(null)
                .params(params)
                .loggingParams(getDockerLoggingParams())
                .docker(docker)
                .dockerManager(DockerManager.builder().docker(docker).build())
                .build();

        DockerCorfuServer server1 = DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(null)
                .params(params1)
                .loggingParams(getDockerLoggingParams())
                .docker(docker)
                .dockerManager(DockerManager.builder().docker(docker).build())
                .build();

        DockerCorfuServer server2 = DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(null)
                .params(params2)
                .loggingParams(getDockerLoggingParams())
                .docker(docker)
                .dockerManager(DockerManager.builder().docker(docker).build())
                .build();

        DockerCorfuServer server3 = DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(null)
                .params(params3)
                .loggingParams(getDockerLoggingParams())
                .docker(docker)
                .dockerManager(DockerManager.builder().docker(docker).build())
                .build();

        FakeDns FAKE_DNS = FakeDns.getInstance().install();

        FAKE_DNS.addForwardResolution(params.getName(), InetAddress.getLoopbackAddress());
        FAKE_DNS.addForwardResolution(params1.getName(), InetAddress.getLoopbackAddress());
        FAKE_DNS.addForwardResolution(params2.getName(), InetAddress.getLoopbackAddress());
        FAKE_DNS.addForwardResolution(params3.getName(), InetAddress.getLoopbackAddress());

        DockerNetwork network = new DockerNetwork(universeParams.getNetworkName(), docker);

        network.setup();

//        server.deploy();
//        server1.deploy();
//        server2.deploy();
        server3.deploy();

//        Layout l = getLayout(Arrays.asList(params.getFullName(), params1.getFullName(), params2.getFullName()));
//        bootstrap(l);

        Layout l2 = getLayout(Collections.singletonList(params3.getFullName()));

        bootstrap(l2);

        List<String> servers = Stream.of(server3)
                .map(CorfuServer::getEndpoint)
                .collect(Collectors.toList());

        ImmutableSortedSet<String> serverSet = ImmutableSortedSet.copyOf(servers);

        LocalCorfuClient localClient = LocalCorfuClient.builder()
                .serverEndpoints(serverSet)
                .build()
                .deploy();

        Thread.sleep(8000);

        System.out.println("Resetting");

        localClient.resetNode(params3.getFullName());

        Thread.sleep(60000);
        network.shutdown();

    }

    public void bootstrap(Layout layout) {

        BootstrapUtil.bootstrap(layout, 20, Duration.ofSeconds(3));
    }

    private Layout getLayout(List<String> servers) {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();

        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(servers))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }

}
