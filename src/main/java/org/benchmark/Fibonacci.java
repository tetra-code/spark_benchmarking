package org.benchmark;

public class Fibonacci {
    public static int fibonacciRecursion(int n) {
        if (n == 0)
            return 0;
        else if (n == 1)
            return 1;
        return fibonacciRecursion(n - 1) +
                fibonacciRecursion(n - 2);
    }
    public static int fibonacciIterative(int n) {
        if (n <= 1)
            return n;
        int fib = 1;
        int prevFib = 1;
        for (int i = 2; i < n; i++) {
            int temp = fib;
            fib += prevFib;
            prevFib = temp;
        }
        return fib;
    }
}
