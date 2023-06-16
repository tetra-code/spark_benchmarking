package org.benchmark;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FibonacciBenchmark {
    @Benchmark
    public void recursion(){
        int value = Fibonacci.fibonacciRecursion(40);
    }
    @Benchmark
    public void iterative(){
        int value = Fibonacci.fibonacciIterative(40);
    }
}