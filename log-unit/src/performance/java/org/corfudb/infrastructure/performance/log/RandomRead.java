package org.corfudb.infrastructure.performance.log;

import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
import org.corfudb.infrastructure.log.segment.ReadSegment;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
public class RandomRead {

    public static void main(String[] args) throws RunnerException {
        //generateTestData();

        int power = 4;
        int startFrom = 4;
        String[] dataSizeParams = new String[power];

        int counter = 0;
        for (int i = 0; i < power; i++) {
            dataSizeParams[counter] = String.valueOf(1 << i+startFrom);
            counter++;
        }

        Options opt = new OptionsBuilder()
                .include(RandomReadBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                //.addProfiler(GCProfiler.class)
                .param("dataSize", dataSizeParams)
                .build();

        new Runner(opt).run();
    }

    public static void generateTestData() throws IOException {
        WriteSegment writeSegment = SegmentManager.getWriteSegment(
                new SegmentMetaData(0, 10_000, RandomReadBenchmark.dbDir)
        );

        if (writeSegment.getPosition() < 1) {
            throw new IllegalStateException("Position!!!");
        }

        for (int i = 0; i < 5_000; i++) {
            final byte[] data = DataGenerator.generateData(1024 * 1024);
            writeSegment.write(i, data);
            writeSegment.fsync();
        }
        writeSegment.close();

        System.exit(0);
    }

    @State(Scope.Benchmark)
    public static class RandomReadBenchmark {

        public static final Path dbDir = Paths.get("db_dir");
        public final Random rnd = new Random();
        public ReadSegment segment;

        @Param({})
        private int dataSize;

        private ReadSegment openReadSegment() {
            try {
                return SegmentManager.openForRead(new SegmentMetaData(0, 10_000, dbDir), NonAddressIndex.NON_ADDRESS);
            } catch (IOException e) {
                throw new IllegalStateException("");
            }
        }

        @Setup
        public void setUp() {
            segment = openReadSegment();
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
            ByteBuffer buf = segment.read(rnd.nextInt(100_000), dataSize);
            blackhole.consume(buf);
        }

        /**
         * Batch read!!!!!!!!!!!
         * @Benchmark
         @BenchmarkMode(Mode.Throughput)
         @Fork(1)
         //@Threads(Threads.MAX)
         @OutputTimeUnit(TimeUnit.SECONDS)
         @Warmup(iterations = 3, time = 1)
         @Measurement(iterations = 3, time = 3)
         @OperationsPerInvocation(50) public void batchRead(Blackhole blackhole) throws IOException {

         for (int i = 0; i < 50; i++) {
         ByteBuffer buf = segment.read(dataSize, rnd.nextInt(100_000));
         blackhole.consume(buf);
         }
         }*/
    }
}
