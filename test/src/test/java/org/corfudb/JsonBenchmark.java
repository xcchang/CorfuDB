package org.corfudb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.corfudb.util.serializer.FastJsonSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.JavaSerializer;
import org.corfudb.util.serializer.JsonSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Data generator benchmark measures latency created by a string generation code and other utility
 * classes/methods
 */
public class JsonBenchmark {

    /**
     * Data generator benchmark
     *
     * @param args args
     * @throws RunnerException jmh runner exception
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JsonBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    @Getter
    public static class JsonState {
        ISerializer primitiveSerializer = new JavaSerializer((byte) 5);
        ISerializer jsonSerializer = new JsonSerializer((byte) 6);
        ISerializer fastJsonSerializer = new FastJsonSerializer((byte) 7);
        private byte[] payload;

        /**
         * Init benchmark state
         */
        @Setup
        public void init() {
            payload = DataGenerator.generateDataBytes(1024 * 4096);
        }
    }

    /**
     * Java random generator benchmark
     *
     * @param blackhole jmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    public void googleGson(Blackhole blackhole, JsonState state) {
        ByteBuf buf = Unpooled.buffer();
        state.jsonSerializer.serialize(state.payload, buf);
        byte[] data = (byte[]) state.jsonSerializer.deserialize(buf, null);
        blackhole.consume(data);
    }


    /**
     * Java secure random generator benchmark
     *
     * @param blackhole kmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    public void primitiveSerializer(Blackhole blackhole, JsonState state) {
        ByteBuf buf = Unpooled.buffer();
        state.primitiveSerializer.serialize(state.payload, buf);
        byte[] data = (byte[]) state.primitiveSerializer.deserialize(buf, null);
        blackhole.consume(data);
    }

    /**
     * Java secure random generator benchmark
     *
     * @param blackhole kmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    public void fastJsonSerializer(Blackhole blackhole, JsonState state) {
        ByteBuf buf = Unpooled.buffer();
        state.fastJsonSerializer.serialize(state.payload, buf);
        byte[] data = (byte[]) state.fastJsonSerializer.deserialize(buf, null);
        blackhole.consume(data);
    }
}