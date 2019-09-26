package org.corfudb.benchmarks.runtime.object;

import static org.corfudb.benchmarks.runtime.object.VloBenchmark.VloState.getSmrStream;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.object.VersionLockedObject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

public class VloBenchmark {

    public static void main(String[] args) throws RunnerException {
        VersionLockedObject<Map<Long, String>> vlo;
        vlo = new VersionLockedObject<>(
                HashMap::new,
                getSmrStream(),
                new DelayedHashMap<>(Duration.ofMillis(100), "1"),
                new DelayedHashMap<>(Duration.ofMillis(100), "2"),
                new DelayedHashMap<>(Duration.ofMillis(100), "3"),
                new HashSet<>()
        );

        vlo.access()
    }

    public static void mainnn(String[] args) throws RunnerException {

        String benchmarkName = VloBenchmark.class.getSimpleName();

        int warmUpIterations = 1;
        TimeValue warmUpTime = TimeValue.seconds(3);

        int measurementIterations = 3;
        TimeValue measurementTime = TimeValue.seconds(10);

        int threads = 1;
        int forks = 1;

        Options opt = new OptionsBuilder()
                .include(benchmarkName)

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)

                .warmupIterations(warmUpIterations)
                .warmupTime(warmUpTime)

                .measurementIterations(measurementIterations)
                .measurementTime(measurementTime)

                .threads(threads)
                .forks(forks)

                .shouldFailOnError(true)

                .resultFormat(ResultFormatType.CSV)
                .result("target/" + benchmarkName + ".csv")

                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    @Slf4j
    public static class VloState {
        VersionLockedObject<Map<Long, String>> vlo;

        @Setup
        public void init() {
            vlo = new VersionLockedObject<>(
                    HashMap::new,
                    getSmrStream(),
                    new DelayedHashMap<>(Duration.ofMillis(100), "1"),
                    new DelayedHashMap<>(Duration.ofMillis(100), "2"),
                    new DelayedHashMap<>(Duration.ofMillis(100), "3"),
                    new HashSet<>()
            );
        }

        public static ISMRStream getSmrStream() {
            return new ISMRStream(){
                private final TreeSet<SMREntry> queue = new TreeSet<>();

                @Override
                public List<SMREntry> remainingUpTo(long maxGlobal) {
                    queue.higher()
                    return new ArrayList<>();
                }

                @Override
                public List<SMREntry> current() {
                    return Collections.singletonList(queue.last());
                }

                @Override
                public List<SMREntry> previous() {
                    queue.pollLast();
                }

                @Override
                public long pos() {
                    return 0;
                }

                @Override
                public void reset() {

                }

                @Override
                public void seek(long globalAddress) {

                }

                @Override
                public void gc(long trimMark) {

                }

                @Override
                public Stream<SMREntry> stream() {
                    return Stream.of();
                }

                @Override
                public Stream<SMREntry> streamUpTo(long maxGlobal) {
                    return Stream.of();
                }

                @Override
                public long append(
                        SMREntry entry,
                        Function<TokenResponse, Boolean> acquisitionCallback,
                        Function<TokenResponse, Boolean> deacquisitionCallback
                ) {
                    return 0;
                }
            };
        }
    }

    @Benchmark
    public void abc(VloState state, Blackhole blackhole) {
        long timestamp = 0;
        final ICorfuSMRAccess<String, Map<Long, String>> accessMethod = obj -> null;

        String result = state.vlo.access(
                o -> o.getVersionUnsafe() >= timestamp && !o.isOptimisticallyModifiedUnsafe(),
                o -> o.syncObjectUnsafe(timestamp),
                accessMethod::access
        );

        blackhole.consume(result);
    }

    @AllArgsConstructor
    @ToString
    public static class DelayedHashMap<K, V> extends HashMap<K, V> {
        private final Duration delay;
        private final String id;

        @Override
        public V get(Object key) {
            delay();
            return super.get(key);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        private void delay() {
            try {
                TimeUnit.MILLISECONDS.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted", e);
            }
        }
    }
}
