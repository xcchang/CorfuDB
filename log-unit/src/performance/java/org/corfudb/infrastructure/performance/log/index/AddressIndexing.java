package org.corfudb.infrastructure.performance.log.index;

import org.corfudb.infrastructure.log.Types.StreamLogAddress;
import org.corfudb.infrastructure.log.index.AddressIndex;
import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AddressIndexing {

    //dynamic batching
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(AddressIndexRandomGetBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                //.addProfiler(GCProfiler.class)
                //.addProfiler(CompilerProfiler.class)
                //.addProfiler(HotspotMemoryProfiler.class)
                //.addProfiler(HotspotRuntimeProfiler.class)
                //.addProfiler(HotspotThreadProfiler.class)
                //.addProfiler(SafepointsProfiler.class)
                //.addProfiler(StackProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class AddressIndexRandomGetBenchmark {
        public final Random rnd = new Random();
        private AddressIndex index = NonAddressIndex.NON_ADDRESS;
        private volatile AddressIndex writeIndex = NonAddressIndex.NON_ADDRESS;
        private volatile int counter = 0;

        @Param({"100", "1000", "10000"})
        private int size;

        @Setup
        public void setUp() {
            for (long i = 1; i < size; i++) {
                StreamLogAddress addr = StreamLogAddress.newBuilder()
                        .setAddress(i)
                        .setOffset(i * 100)
                        .setLength(100)
                        .build();
                index = index.next(addr);
            }
        }

        /*@TearDown
        public void tearDown(){
            SizeOf sizeOf = SizeOf.newInstance();
            counter = 1;
            //System.out.println("index size: " + sizeOf.deepSizeOf(index));
            index = NonAddressIndex.NON_ADDRESS;
            writeIndex = NonAddressIndex.NON_ADDRESS;
            System.gc();
        }*/

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        //@Threads(32)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 1, time = 1)
        @Measurement(iterations = 1, time = 1)
        public void randomRead(Blackhole blackhole) {
            AddressIndex v = index.get(Math.max(1, rnd.nextInt(size - 1)));
            blackhole.consume(v);
        }

        /*@Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Threads(Threads.MAX)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 1, time = 3)
        @Measurement(iterations = 3, time = 1)
        public void write() {
            counter++;
            StreamLogAddress addr = StreamLogAddress.newBuilder()
                    .setAddress(counter)
                    .setOffset(counter)
                    .setLength(256)
                    .build();
            writeIndex = writeIndex.next(addr);
            //blackhole.consume(writeIndex);
        }*/
    }
}
