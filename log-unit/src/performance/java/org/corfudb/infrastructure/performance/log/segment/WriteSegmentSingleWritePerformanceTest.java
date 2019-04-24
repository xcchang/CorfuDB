package org.corfudb.infrastructure.performance.log.segment;

import org.corfudb.infrastructure.log.segment.SegmentManager;
import org.corfudb.infrastructure.log.segment.SegmentMetaData;
import org.corfudb.infrastructure.log.segment.WriteSegment;
import org.corfudb.infrastructure.performance.log.DataGenerator;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class WriteSegmentSingleWritePerformanceTest {

    public static void main(String[] args) throws RunnerException {

        String[] dataSizeParams = DataGenerator.getProgression(128, 1024 * 1024);

        Options opt = new OptionsBuilder()
                .include(WriteSegmentSingleWriteBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                .param("dataSize", dataSizeParams)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class WriteSegmentSingleWriteBenchmark {

        private final SegmentMetaData metaData = new SegmentMetaData(0, 10_000, Paths.get("db_dir"));
        private WriteSegment writeSegment;
        private AtomicLong addr;
        private byte[] data;
        @Param({})
        private int dataSize;

        @Setup
        public void setUp() throws IOException {
            Files.deleteIfExists(metaData.getSegmentPath());

            addr = new AtomicLong();
            data = DataGenerator.generateData(dataSize);
            writeSegment = SegmentManager.getWriteSegment(metaData);
        }

        @TearDown
        public void tearDown() throws IOException {
            writeSegment.close();
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        //@Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 3, time = 1)
        @Measurement(iterations = 3, time = 3)
        public void benchmark(Blackhole blackhole) throws IOException {
            long globalAddress = addr.getAndIncrement();
            writeSegment.write(globalAddress, data);
        }
    }
}
