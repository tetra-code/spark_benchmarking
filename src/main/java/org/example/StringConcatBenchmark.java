package org.example;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StringConcatBenchmark {
    @Benchmark
    public void simpleConcatenation() {
        String s = StringConcatenation.simpleAppend("Hello World! ", 1000);
    }

    @Benchmark
    public void builder() {
        String s = StringConcatenation.appendWithBuilder("Hello World! ", 1000);
    }
}
