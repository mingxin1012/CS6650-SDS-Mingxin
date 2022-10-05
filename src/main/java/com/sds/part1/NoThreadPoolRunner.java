package com.sds.part1;

import com.sds.PostConsumer;
import com.sds.PostProducer;
import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;
import io.swagger.client.api.SkiersApi;

import java.sql.Timestamp;
import java.util.concurrent.*;

public class NoThreadPoolRunner {

    private final static int N_REQUESTS = 200000;
    private final static LiftRideEvent POISON = new LiftRideEvent();
    private final static int PHASE_TWO_THREADS = 168;
    private final static int PHASE_ONE_THREADS = 32;
    private final static String BASE_PATH = "http://54.245.60.78:8080/skiers_Web/skier";

    public static void main(String[] args) throws InterruptedException {

        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        SkiersApi apiInstance = new SkiersApi();
        RequestCounter numPassedRequests = new RequestCounter();
        RequestCounter numFailedRequests = new RequestCounter();

        BlockingQueue<LiftRideEvent> blockingQueue = new LinkedBlockingDeque<>();

        CountDownLatch consumerLatch = new CountDownLatch(PHASE_ONE_THREADS + PHASE_TWO_THREADS);

        for (int i = 0; i < PHASE_ONE_THREADS; i++) {
            // lambda runnable creation - interface only has a single method so lambda works fine
            new Thread(new PostConsumer(BASE_PATH,blockingQueue, POISON, apiInstance,numPassedRequests, numFailedRequests,consumerLatch)).start();
        }

        for (int i = 0; i < PHASE_TWO_THREADS; i++) {
            // lambda runnable creation - interface only has a single method so lambda works fine
            new Thread(new PostConsumer(BASE_PATH,blockingQueue, POISON, apiInstance,numPassedRequests, numFailedRequests,consumerLatch)).start();
        }

        //start a single dedicated thread to create lift ride event
        CountDownLatch producerLatch = new CountDownLatch(1);
        Runnable producer = new PostProducer(blockingQueue,PHASE_TWO_THREADS +PHASE_ONE_THREADS ,POISON,N_REQUESTS,producerLatch);
        new Thread(producer).start();

        producerLatch.await();
        consumerLatch.await();

        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        long latency = endTime.getTime() - startTime.getTime();
        System.out.println("Finished all threads");
        System.out.println("Total number of successful requests is " + numPassedRequests.getVal());
        System.out.println("Total number of failed requests is " + numFailedRequests.getVal());
        System.out.println("Total run time (wall time) is " + latency + "ms");
        System.out.println("Total throughput in requests per second " + Math.floor((double)numPassedRequests.getVal() / ((double)latency/1000)));
    }
}

