package org.corfudb.perf.streaming;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.perf.SimulatorArguments;
import org.corfudb.perf.Utils;
import org.corfudb.runtime.CorfuRuntime;

@Slf4j
public class StreamingSimulator {

    static class Arguments extends SimulatorArguments {
        @Parameter(names = {"--endpoint"}, description = "Cluster endpoint", required = true)
        private List<String> connectionString = new ArrayList<>();

        @Parameter(names = {"--num-runtime"}, description = "Number of corfu runtimes to use",
                required = true)
        private int numRuntime;

        @Parameter(names = {"--num-consumers"}, description = "Number of consumers", required = true)
        int numConsumers;

        @Parameter(names = {"--poll-period"}, description = "Consumer poll period", required = true)
        int pollPeriod = 10;

        @Parameter(names = {"--num-threads"}, description = "Number of producers", required = true)
        int numProducers;

        @Parameter(names = {"--num-tasks"}, description = "Total number of tasks to create" +
                " per producer", required = true)
        int numTasks;

        @Parameter(names = {"--update-interval"}, description = "Progress update interval in ms",
                required = true)
        long statusUpdateMs;

        @Parameter(names = {"--payload-size"}, description = "Size of payload in bytes",
                required = true)
        int payloadSize;

        @Parameter(names = {"--duration"}, description = "Duration of time to run the simulation " +
                "for in minutes", required = true)
        int duration;
    }

    /**
     * Create a list of unique producers
     * @param arguments arguments to use
     * @param rts an array of CorfuRuntime that can be used to create the streams
     * @return A list of producers
     */
    private static List<Producer> createProducers(final Arguments arguments,
                                                  final CorfuRuntime[] rts) {
        if (arguments.numProducers <= 0) {
            throw new IllegalArgumentException("Not enough producers!");
        }

        final byte[] payload = new byte[arguments.payloadSize];
        ThreadLocalRandom.current().nextBytes(payload);
        final List<Producer> producers = new ArrayList<>(arguments.numProducers);
        IntStream.range(0, arguments.numProducers)
                .forEach(idx -> {
                    final UUID id = CorfuRuntime.getStreamID("producer" + idx);
                    final CorfuRuntime runtime = rts[idx % rts.length];
                    producers.add(new Producer(id, runtime, arguments.statusUpdateMs,
                            arguments.numTasks, payload));
                });
        return Collections.unmodifiableList(producers);
    }

    /**
     * Create a list of unique consumers
     * @param arguments arguments to use
     * @param rts an array of CorfuRuntime that can be used to create the streams
     * @return a list of consumers
     */
    private static List<Consumer> createConsumers(final Arguments arguments,
                                                  final CorfuRuntime[] rts) {
        if (arguments.numConsumers == 0) {
            return Collections.emptyList();
        }

        if (arguments.numConsumers < arguments.numProducers) {
            throw new IllegalArgumentException("Not enough consumers!");
        }

        final List<Consumer> consumers = new ArrayList<>(arguments.numConsumers);
        IntStream.range(0, arguments.numConsumers)
                .forEach(idx -> {
                    String name = "consumer" + idx % arguments.numProducers;
                    final UUID id = CorfuRuntime.getStreamID(name);
                    final CorfuRuntime runtime = rts[idx % rts.length];
                    consumers.add(new Consumer(id, runtime, arguments.statusUpdateMs,
                            arguments.numTasks, arguments.pollPeriod));
                });
        return Collections.unmodifiableList(consumers);
    }

    /**
     * Logs uncaught exceptions.
     *
     * @param thread    The thread which terminated.
     * @param throwable The throwable which caused the thread to terminate.
     */
    private static void handleUncaughtException(@Nonnull Thread thread, @Nonnull Throwable throwable) {
        log.error("handleUncaughtThread: {} terminated with throwable of type {}",
                    thread.getName(),
                    throwable.getClass().getSimpleName(),
                    throwable);
    }

    public static void main(String[] stringArgs) throws Exception {
        final Arguments arguments = new Arguments();
        Utils.parse(arguments, stringArgs);

        //TODO(Maithem): enable multiple producers to create tasks for the same underlying stream

        final List<Consumer> consumers = createConsumers(arguments, null);
        final List<Producer> producers = createProducers(arguments, null);

        final ExecutorService producersPool = Executors.newFixedThreadPool(
                arguments.numProducers, new ThreadFactoryBuilder()
                        .setNameFormat("producer-%d")
                        .setUncaughtExceptionHandler(StreamingSimulator::handleUncaughtException)
                        .setDaemon(true)
                        .build());
        final ExecutorService consumersPool = Executors.newFixedThreadPool(
                arguments.numConsumers, new ThreadFactoryBuilder()
                        .setNameFormat("producer-%d")
                        .setUncaughtExceptionHandler(StreamingSimulator::handleUncaughtException)
                        .setDaemon(true)
                        .build());

        // Start the producers before the consumers and wait till they consumers are done
        producers.forEach(producersPool::submit);
        consumers.forEach(consumersPool::submit);

        producersPool.shutdown();
        consumersPool.shutdown();

        final boolean consumersComplete = consumersPool.awaitTermination(arguments.duration,
                TimeUnit.MINUTES);
        log.info("Consumers completed: {}" , consumersComplete);
        log.info("Producers completed: {}" , producersPool.isTerminated());

    }
}
