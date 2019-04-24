package org.corfudb.infrastructure.performance.log;

import org.corfudb.infrastructure.log.segment.SegmentManager;
import org.corfudb.infrastructure.log.segment.SegmentMetaData;
import org.corfudb.infrastructure.log.segment.WriteSegment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 */
public class SequentialWrite {

    public static void main(String[] args) throws RunnerException {
        SequentialWriteBenchmark.dbDir.toFile().mkdirs();

        Options opt = new OptionsBuilder()
                .include(SequentialWriteBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class SequentialWriteBenchmark {
        private WriteSegment segment;

        @Param({"128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072", "262144", "524288", "768000", "1048576"})
        private int dataSize;

        public byte[] data;
        public static Path dbDir = Paths.get("db_dir");
        private AtomicLong globalAddress = new AtomicLong();

        @Setup
        public void setUp() throws IOException {
            data = DataGenerator.generateData(dataSize);
            segment = SegmentManager.getWriteSegment(new SegmentMetaData(0, 10_000, dbDir));
        }

        @TearDown
        public void tearDown() throws IOException {
            segment.close();
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 3, time = 1)
        @Measurement(iterations = 3, time = 1)
        public void benchmark(Blackhole blackhole) throws IOException {

            long addr = globalAddress.getAndIncrement();
            segment.write(addr, data);
            //segment.fsync();
        }
    }
}
