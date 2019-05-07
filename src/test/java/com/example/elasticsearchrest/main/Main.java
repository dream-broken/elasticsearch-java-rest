package com.example.elasticsearchrest.main;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.LongStream;

public class Main {

    public static void main(String[] args) {
        long[] longs = LongStream.rangeClosed(1, 100000000L).toArray();
        RecursiveTask<Long> recursiveTask = new ForkJoinSumCalulator(longs);
        Long invoke = new ForkJoinPool().invoke(recursiveTask);
        System.out.println(invoke);

    }
}
