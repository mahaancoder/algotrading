package com.satyam.trading2.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dedicated bounded pool for momentum BUY submissions.
 *
 * The pool is intentionally isolated from Spring's scheduler and WebSocket
 * executors so slow broker responses cannot starve market-data processing.
 */
@Configuration
public class MomentumExecutionConfiguration {

    @Bean(name = "momentumOrderExecutor", destroyMethod = "shutdown")
    public ExecutorService momentumOrderExecutor(
            @Value("${momentum.execution.pool-size:8}") int poolSize,
            @Value("${momentum.execution.queue-capacity:256}") int queueCapacity) {

        final int workers = Math.max(1, poolSize);
        final int capacity = Math.max(workers, queueCapacity);
        AtomicInteger sequence = new AtomicInteger(1);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable,
                    "momentum-order-" + sequence.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());

        executor.prestartAllCoreThreads();
        return executor;
    }
}
