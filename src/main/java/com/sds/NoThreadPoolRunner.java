package com.sds;

import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;
import io.swagger.client.api.SkiersApi;

import java.sql.Timestamp;
import java.util.concurrent.*;

public class NoThreadPoolRunner {

    public static void main(String[] args) throws InterruptedException {
        int N_QUEUE = 10000;
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        int N_REQUESTS = 200000;
        LiftRideEvent POISON = new LiftRideEvent();
        SkiersApi apiInstance = new SkiersApi();
        RequestCounter numPassedRequests = new RequestCounter();
        RequestCounter numFailedRequests = new RequestCounter();
        int N_CONSUMERS = Runtime.getRuntime().availableProcessors();
        int PHASE_ONE_THREADS = 32;

        int PHASE_TWO_THREADS = 168;

        BlockingQueue<LiftRideEvent> blockingQueue = new LinkedBlockingDeque<>();


        //start a single dedicated thread to create lift ride event
        Runnable producer = new PostProducer(blockingQueue,N_CONSUMERS,POISON,N_REQUESTS);
        ExecutorService producerExecutor = Executors.newSingleThreadExecutor();
        producerExecutor.execute(producer);

        //phase 1
        CountDownLatch overallLatch = new CountDownLatch(PHASE_ONE_THREADS + PHASE_TWO_THREADS);
        CountDownLatch latch1 = new CountDownLatch(PHASE_ONE_THREADS);

        for (int i = 0; i < PHASE_ONE_THREADS; i++) {
            // lambda runnable creation - interface only has a single method so lambda works fine
            new LiftRideThread(blockingQueue,apiInstance, latch1,overallLatch, numPassedRequests, numFailedRequests).start();
        }
        latch1.await();
        CountDownLatch latch2 = new CountDownLatch(PHASE_TWO_THREADS);
        for (int i = 0; i < PHASE_TWO_THREADS; i++) {
            // lambda runnable creation - interface only has a single method so lambda works fine
            new LiftRideThread(blockingQueue,apiInstance, latch2,overallLatch, numPassedRequests, numFailedRequests).start();
        }
        latch2.await();
        overallLatch.countDown();

        producerExecutor.shutdown();
        try {
            producerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.err.println("Thread is interrupted");
            e.printStackTrace();
        }

        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        long latency = endTime.getTime() - startTime.getTime();
        System.out.println("Finished all threads");
        System.out.println("Total number of successful requests is " + numPassedRequests.getVal());
        System.out.println("Total number of failed requests is " + numFailedRequests.getVal());
        System.out.println("Total run time (wall time) is " + latency + "ms");
        System.out.println("Total throughput in requests per second " + Math.floor((double)numPassedRequests.getVal() / ((double)latency/1000)));
    }
}

