package org.corfudb.infrastructure;

// Start HTTP server and process backup and restore command.

import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;

@Slf4j
public class BackupRestoreCommandTest {
    @Test
    public void testBackupRestoreCommand() throws InterruptedException {
        /*Map<String, Object> opts = new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);*/
        Map<String, Object> opts = new HashMap<>();
        opts.put("--metrics", true);
        opts.put("--metrics-port", 9999);
        PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(opts);
        MetricsServer server = new PrometheusMetricsServer(config, ServerContext.getMetrics());
        server.start();
        log.info("test started");
        sleep(120000);
        log.info("test exit");
    }
}
