package org.corfudb.infrastructure.performance.log.index;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.ehcache.sizeof.SizeOf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class HashMapIndexing {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RandomReadHashMapBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                //.addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class RandomReadHashMapBenchmark {
        public final Random rnd = new Random();
        private ConcurrentMap<Long, IndexValue> index = new ConcurrentHashMap<>();
        private ConcurrentMap<Long, IndexValue> writeIndex = new ConcurrentHashMap<>();

        @Param({"10000", "100000"})
        private int indexSize;

        private volatile long counter = 0;

        @Setup
        public void setUp() {
            for (long i = 0; i < indexSize; i++) {
                index.put(i, new IndexValue(i, i, 256));
            }
        }

        @TearDown
        public void tearDown() {
            SizeOf sizeOf = SizeOf.newInstance();
            counter = 0;
            //System.out.println("index size: " + sizeOf.deepSizeOf(index));
            index = new ConcurrentHashMap<>();
            writeIndex = new ConcurrentHashMap<>();
            System.gc();
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        //@Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 1, time = 1)
        @Measurement(iterations = 1, time = 1)
        public void randomRead(Blackhole blackhole) {
            IndexValue v = index.get((long) rnd.nextInt(100_000));
            blackhole.consume(v);
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 1, time = 3)
        @Measurement(iterations = 3, time = 1)
        public void write(Blackhole blackhole) {
            counter++;
            IndexValue v = writeIndex.put(counter, new IndexValue(counter, counter, 256));
            blackhole.consume(v);
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class IndexValue {
        @Getter
        private final long globalAddress;
        @Getter
        private final long offset;
        @Getter
        private final int length;
    }
}
