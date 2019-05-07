package com.example.elasticsearchrest.main;

import java.util.concurrent.RecursiveTask;

public class ForkJoinSumCalulator extends RecursiveTask<Long> {

    private final long[] numbers;
    private final int start;
    private final int end;

    private static final long THRESHOLD = 100;

    public ForkJoinSumCalulator(long[] numbers) {
        this(numbers, 0, numbers.length);
    }

    public ForkJoinSumCalulator(long[] numbers, int start, int end) {
        this.numbers = numbers;
        this.start = start;
        this.end = end;
    }

    @Override
    protected Long compute() {
        int length = end - start;
        if (length <= THRESHOLD) {
            long sum = 0;
            for (int i = start; i < end; i++) {
                sum += numbers[i];
            }
            return sum;
        }

        ForkJoinSumCalulator leftTask = new ForkJoinSumCalulator(numbers, start, start + length / 2);
        leftTask.fork();

        ForkJoinSumCalulator rightTask = new ForkJoinSumCalulator(numbers, start  + length / 2, end);

        Long compute = rightTask.compute();
        Long join = leftTask.join();
        return compute + join;
    }

}
