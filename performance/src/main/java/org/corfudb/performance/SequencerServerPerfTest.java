package org.corfudb.performance;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Test Sequencer Server performance to generate AddressSpaceView tokens.
 */
@Slf4j
public class SequencerServerPerfTest {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final int DEFAULT_RUNTIME_NUM = 1;
    private static final int DEFAULT_THREAD_NUM = 1;
    private static final long DEFAULT_TOKEN_NUM = 10_000L;

    private static final String USAGE = String.format("Usage: SequenceServerPerfTest "
                    + "[-c <conf>] [-r <runtime num>] [-t <thread num>] [-T <token num>]\n"
                    + "Options:\n"
                    + " -c <conf>     Set the Sequencer Server host and port  [default: %s]\n"
                    + " -r <runtime num>    Set the number of runtime to drive the Sequencer Server  [default: %d]\n"
                    + " -t <thread num>     Set the number of threads per runtime  [default: %d]\n"
                    + " -T <token num>      Set the total number of tokens from Sequencer Server  [default: %d]\n",
            DEFAULT_ENDPOINT,
            DEFAULT_RUNTIME_NUM,
            DEFAULT_THREAD_NUM,
            DEFAULT_TOKEN_NUM);

    private AtomicLong tokenCount = new AtomicLong(0);
    private AtomicLong getFutureExceptionCount = new AtomicLong(0);

    private final String sequencerServerEndpoint;
    private final int runtimeNum;
    private final int threadNum;
    private final long tokenNum;



    public SequencerServerPerfTest(@NonNull String sequencerServerEndpoint, int runtimeNum, int threadNum,
                                 long tokenNum) {
        this.sequencerServerEndpoint = sequencerServerEndpoint;
        this.runtimeNum = runtimeNum;
        this.threadNum = threadNum;
        this.tokenNum = tokenNum;
    }

    public static void main(String[] args) {
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);

        String sequencerServerEndPoint = (String) opts.getOrDefault("-c", DEFAULT_ENDPOINT);
        int runtimeNum = Integer.parseInt((String) opts.getOrDefault("-r", String.valueOf(DEFAULT_RUNTIME_NUM)));
        int threadNum = Integer.parseInt((String) opts.getOrDefault("-t", String.valueOf(DEFAULT_THREAD_NUM)));
        long tokenNum = Integer.parseInt((String) opts.getOrDefault("-T", String.valueOf(DEFAULT_TOKEN_NUM)));

        SequencerServerPerfTest sequencerServerPerfTest =
                new SequencerServerPerfTest(sequencerServerEndPoint, runtimeNum, threadNum, tokenNum);

        sequencerServerPerfTest.run();
    }

    @SuppressWarnings("checkstyle:printLine")
    private void run() {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < runtimeNum; ++i) {
            threads.addAll(prepareDriverThreads(sequencerServerEndpoint, UUID.randomUUID()));
        }

        threads.forEach(thread -> {
            thread.start();
        });

        long startTime = System.currentTimeMillis();
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                System.out.println("Main thread is interrupted.\n");
                return;
            }
        });
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Good throughput: %d token/s",
                (tokenNum * runtimeNum * threadNum - getFutureExceptionCount.get())* 1000 / (endTime - startTime)));
        System.out.println(String.format("Rotten throughput: %d token/s",
                getFutureExceptionCount.get() * 1000 / (endTime - startTime)));
    }

    private List<Thread> prepareDriverThreads(String sequencerServerEndpoint, UUID clientId) {
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .clientId(clientId)
                .build();
        CorfuRuntime runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(sequencerServerEndpoint);
        runtime.connect();

        List<Thread> driverThreads = new ArrayList<>();

        SequencerClient sequencerClient = runtime.getLayoutView().getRuntimeLayout()
                .getSequencerClient(sequencerServerEndpoint);

        Driver driver = new Driver(sequencerClient, clientId);
        for (int i = 0; i < threadNum; ++i) {
            driverThreads.add(new Thread(driver));
        }
        return driverThreads;
    }

    class Driver implements Runnable {
        private SequencerClient sequencerClient;
        private UUID clientId;

        public Driver(@NonNull SequencerClient sequencerClient, @NonNull UUID clientId) {
            this.sequencerClient = sequencerClient;
            this.clientId = clientId;
        }

        @Override
        public void run() {
            int count = 0;
            List<CompletableFuture<TokenResponse>> futures = new ArrayList<>();
            for (count = 0; count < tokenNum; ++count) {
                CompletableFuture<TokenResponse> future = sequencerClient.nextToken(Arrays.asList(clientId), 1);
                futures.add(future);
            }

            futures.forEach((future) -> {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException ex) {
                    getFutureExceptionCount.getAndIncrement();
                }
            });
        }
    }
}
