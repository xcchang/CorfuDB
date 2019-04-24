package org.corfudb.infrastructure.performance.log.index;

import org.corfudb.infrastructure.log.Types.StreamLogAddress;
import org.corfudb.infrastructure.log.index.AddressIndex;
import org.corfudb.infrastructure.log.index.AddressIndex.NonAddressIndex;
import org.ehcache.sizeof.SizeOf;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamLogAddressPerformanceTest {

    //dynamic batching
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StreamLogAddressBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.CSV)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class StreamLogAddressBenchmark {
        public final Random rnd = new Random();

        @TearDown
        public void tearDown(){
            StreamLogAddress addr = StreamLogAddress.newBuilder()
                    .setAddress(100L)
                    .setOffset(100L)
                    .setLength(256)
                    .build();
            int size = addr.getSerializedSize();
            System.out.println("Stream log address object size: " + size);
        }

        @Benchmark
        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Warmup(iterations = 1, time = 3)
        @Measurement(iterations = 3, time = 3)
        public void randomStreamLogAddress(Blackhole blackhole) {
            long globalAddr = rnd.nextLong();
            StreamLogAddress addr = StreamLogAddress.newBuilder()
                    .setAddress(globalAddr)
                    .setOffset(globalAddr + 100)
                    .setLength(256)
                    .build();
            int size = addr.getSerializedSize();
            blackhole.consume(size);
            blackhole.consume(addr);
        }
    }
}
