package org.corfudb.infrastructure.performance.log;

import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
import org.corfudb.infrastructure.log.segment.ReadSegment;
import org.corfudb.infrastructure.log.segment.SegmentMetaData;
import org.corfudb.infrastructure.log.segment.WriteSegment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MixedReadWrite {

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(MixedReadWriteBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                //.addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class MixedReadWriteBenchmark {

        public static final Path dbDir = Paths.get("db_dir");
        public final Random rnd = new Random();
        public ReadSegment readSegment;
        public WriteSegment writeSegment;
        public byte[] data;

        //@Param({"128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072", "262144", "524288", "768000", "1048576"})
        @Param({"128", "256", "512", "1024"})
        private int dataSize = 128;

        public FileChannel getFileChannel() throws IOException {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE, StandardOpenOption.SPARSE
            );

            Path segmentFile = Paths.get(dbDir.toString(), String.valueOf(0));
            return FileChannel.open(segmentFile, options);
        }

        @Setup
        public void setUp() throws IOException {
            data = DataGenerator.generateData(dataSize);
            FileChannel fileChannel = getFileChannel();

            final SegmentMetaData metaData = new SegmentMetaData(0, 10_000, dbDir);
            writeSegment = WriteSegment.builder()
                    .channel(fileChannel)
                    .info(metaData)
                    .build();

            readSegment = new ReadSegment(metaData, NonAddressIndex.NON_ADDRESS, fileChannel);
        }

        @TearDown
        public void tearDown() throws IOException {
            writeSegment.close();
            readSegment.close();
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 5, time = 1)
        @Measurement(iterations = 1, time = 3)
        @Group("mix")
        //@GroupThreads(3)
        public void benchmarkRandomRead(Blackhole blackhole) throws IOException {
            ByteBuffer buf = readSegment.read(rnd.nextInt(100_000), dataSize);
            blackhole.consume(buf);
        }

        private volatile long globalAddress;

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 5, time = 1)
        @Measurement(iterations = 1, time = 3)
        @Group("mix")
        //@GroupThreads(3)
        public void benchmarkRandomWrite(Blackhole blackhole) throws IOException {
            if (data.length != dataSize){
                //System.out.println("ERRROR!!! " + data.length);
                System.exit(0);
            }

            globalAddress++;
            writeSegment.write(globalAddress, data);
            //writeSegment.fsync();
            //blackhole.consume(a);
        }
    }
}
