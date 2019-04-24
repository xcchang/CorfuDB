package org.corfudb.infrastructure.performance.log.segment;

import org.corfudb.infrastructure.log.index.AddressIndex;
import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
import org.corfudb.infrastructure.log.segment.ReadSegment;
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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 * <p>
 * http://jmh.morethan.io/
 * <p>
 * http://app.rawgraphs.io/
 */
public class StreamAddressFromSegmentPerformanceTest {

    public static void main(String[] args) throws RunnerException {

        String[] dataSizeParams = DataGenerator.getProgression(4, 128);

        Options opt = new OptionsBuilder()
                .include(StreamAddressFromSegmentBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                .param("dataSize", dataSizeParams)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class StreamAddressFromSegmentBenchmark {
        public final Random rnd = new Random();
        public ReadSegment segment;
        FileChannel readChannel;
        SegmentMetaData metaData = new SegmentMetaData(0, 10_000, Paths.get("db_dir"));

        @Param({})
        private int dataSize;

        @Setup
        public void setUp() throws IOException {
            writeData(metaData);
            readChannel = SegmentManager.getReadChannel(metaData);
        }

        @TearDown
        public void tearDown() throws IOException {
            segment.close();
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        //@Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 3, time = 1)
        @Measurement(iterations = 3, time = 3)
        public void benchmark(Blackhole blackhole) throws IOException {
            AddressIndex index = NonAddressIndex.NON_ADDRESS;

            segment = new ReadSegment(metaData, index, readChannel);
            segment.updateIndex();
            //blackhole.consume(segment);
        }

        public void writeData(SegmentMetaData metaData) throws IOException {
            Files.deleteIfExists(metaData.getSegmentPath());

            WriteSegment writeSegment = SegmentManager.getWriteSegment(metaData);
            for (int addr = 0; addr < metaData.getLatestAddress(); addr++) {
                byte[] data = DataGenerator.generateData(128);
                writeSegment.write(addr, data);
            }
        }
    }
}
