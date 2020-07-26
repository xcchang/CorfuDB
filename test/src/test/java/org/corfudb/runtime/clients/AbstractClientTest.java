package org.corfudb.runtime.clients;

import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.server.CorfuServerStateMachine;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 12/13/15.
 */
public abstract class AbstractClientTest extends AbstractCorfuTest {

    protected final CorfuServerStateMachine serverSm = Mockito.mock(CorfuServerStateMachine.class);

    /**
     * Initialize the AbstractClientTest.
     */
    public AbstractClientTest() {
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
    }

    @Getter
    protected TestClientRouter router;

    @Getter
    protected TestServerRouter serverRouter;

    @Before
    public void resetTest() {
        serverRouter = new TestServerRouter();
        router = new TestClientRouter(serverRouter);
        getServersForTest().forEach(serverRouter::addServer);
        getClientsForTest().forEach(router::addClient);
    }

    @Override
    public void close() {
        serverRouter.close();
    }

    /**
     * A map of maps to endpoint->routers, mapped for each runtime instance captured
     */
    protected final Map<CorfuRuntime, Map<String, TestClientRouter>> runtimeRouterMap =
            new ConcurrentHashMap<>();

    /**
     * Function for obtaining a router, given a runtime and an endpoint.
     *
     * @param runtime  The CorfuRuntime to obtain a router for.
     * @param endpoint An endpoint string for the router.
     * @return
     */
    private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:")) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
                x -> {
                    TestClientRouter tcn = new TestClientRouter(serverRouter);
                    tcn.addClient(new BaseHandler())
                            .addClient(new SequencerHandler())
                            .addClient(new LayoutHandler())
                            .addClient(new LogUnitHandler())
                            .addClient(new ManagementHandler());
                    return tcn;
                }
        );
    }

    abstract Set<AbstractServer> getServersForTest();

    abstract Set<IClient> getClientsForTest();

    public ServerContext defaultServerContext() {
        return new ServerContextBuilder()
                .setMemory(true)
                .setSingle(false)
                .setServerRouter(serverRouter)
                .build();
    }
}
