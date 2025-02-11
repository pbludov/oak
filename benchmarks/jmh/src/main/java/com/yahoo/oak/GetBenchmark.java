/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class GetBenchmark {

    public static final int KEY_SIZE_BYTES = 64;
    public static final int VALUE_SIZE_BYTES = 64;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private OakMap<String, String> oakMap;

        @Param({"500000"})
        private int numRows;
        private ArrayList<String> keys;

        @Setup()
        public void setup() {
            OakMapBuilder<String, String> builder = OakCommonBuildersFactory.getDefaultStringBuilder();
            oakMap = builder.buildOrderedMap();

            keys = new ArrayList<>(numRows);
            for (int i = 0; i < numRows; ++i) {
                String key = String.format("%1$" + KEY_SIZE_BYTES / Character.BYTES + "s",
                        String.valueOf(i));

                String val = String.format("%1$-" + VALUE_SIZE_BYTES / Character.BYTES + "s",
                        String.valueOf(i));

                oakMap.zc().put(key, val);
                keys.add(key);
            }
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        int i = 0;
    }


    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Threads(8)
    @Benchmark
    public void get(Blackhole blackhole, BenchmarkState state, ThreadState threadState) {
        String key = state.keys.get(threadState.i++ % state.numRows);
        String val = state.oakMap.get(key);
        blackhole.consume(val);
    }

    //java -jar -Xmx8g -XX:MaxDirectMemorySize=8g ./benchmarks/target/benchmarks.jar get -p numRows=500000 -prof stack
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GetBenchmark.class.getSimpleName())
                .forks(0)
                .threads(8)
                .build();

        new Runner(opt).run();
    }

}
