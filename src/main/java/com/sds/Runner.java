package com.sds;

import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;
import io.swagger.client.api.SkiersApi;


import java.sql.Timestamp;
import java.util.concurrent.*;

public class Runner {

    public static void main(String[] args) throws InterruptedException {
        int N_QUEUE = 200000;
        int N_REQUESTS = 200000;
        LiftRideEvent POISON = new LiftRideEvent();

        RequestCounter numPassedRequests = new RequestCounter();
        RequestCounter numFailedRequests = new RequestCounter();
        int N_CONSUMERS = 168;
        int PHASE_ONE_THREADS = 32;
        BlockingQueue<LiftRideEvent> blockingQueue = new LinkedBlockingDeque<>(N_QUEUE);


        //start a single dedicated thread to create lift ride event
        Runnable producer = new PostProducer(blockingQueue,N_CONSUMERS,POISON,N_REQUESTS);
        ExecutorService producerExecutor = Executors.newSingleThreadExecutor();

        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        producerExecutor.execute(producer);

        //phase 1
        CountDownLatch latch1 = new CountDownLatch(PHASE_ONE_THREADS);

        for (int i = 0; i < PHASE_ONE_THREADS; i++) {
            // lambda runnable creation - interface only has a single method so lambda works fine
            new PhaseThread(blockingQueue,new SkiersApi(), latch1,numPassedRequests, numFailedRequests).start();
        }

        latch1.await();


        //use a thread pool to send post requests;
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(N_CONSUMERS);


        for (int i = 0; i < N_CONSUMERS; i++) {
            Runnable worker = new PostConsumer(blockingQueue, POISON, new SkiersApi(),numPassedRequests, numFailedRequests);
            consumerExecutor.execute(worker);
        }

        consumerExecutor.shutdown();
        producerExecutor.shutdown();
        try {
            consumerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
        System.out.println("Total throughput in requests per second " + Math.round(numPassedRequests.getVal() / (double)(latency/1000)));
    }
}

