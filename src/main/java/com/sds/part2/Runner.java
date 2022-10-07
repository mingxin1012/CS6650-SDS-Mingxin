package com.sds.part2;

import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;

import io.swagger.client.api.SkiersApi;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class Runner {

    private final static int N_REQUESTS = 200000;
    private final static LiftRideEvent POISON = new LiftRideEvent();
    private final static int PHASE_TWO_THREADS = 168;
    private final static int PHASE_ONE_THREADS = 32;
    private final static int MAX_REQUEST_ACCEPT_PHASE_ONE = 1000;
    private final static int MAX_REQUEST_ACCEPT_PHASE_TWO = 1200;
    private final static String BASE_PATH = "http://35.90.159.76:8080/skiers_Web/skier";

    public static void main(String[] args) throws InterruptedException{
        final SharedFileWriter sharedFileWriter = new SharedFileWriter();
        final Queue<Metric> metrics = new ConcurrentLinkedQueue<>();
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        RequestCounter numPassedRequests = new RequestCounter();
        RequestCounter numFailedRequests = new RequestCounter();

        BlockingQueue<LiftRideEvent> blockingQueue = new LinkedBlockingDeque<>();

        //start a single dedicated producer thread to create lift ride event
        CountDownLatch producerLatch = new CountDownLatch(1);
        Runnable producer = new PostProducer(blockingQueue,PHASE_TWO_THREADS +PHASE_ONE_THREADS ,POISON,N_REQUESTS,producerLatch);
        new Thread(producer).start();

        producerLatch.await();
        // start consumer threads
        CountDownLatch consumerLatch = new CountDownLatch(PHASE_ONE_THREADS + PHASE_TWO_THREADS);
        for (int i = 0; i < PHASE_ONE_THREADS; i++) {
            new Thread(new PostConsumer(BASE_PATH,blockingQueue, POISON, MAX_REQUEST_ACCEPT_PHASE_ONE, numPassedRequests, numFailedRequests,consumerLatch,sharedFileWriter,metrics)).start();
        }

        for (int i = 0; i < PHASE_TWO_THREADS; i++) {
            new Thread(new PostConsumer(BASE_PATH,blockingQueue, POISON, MAX_REQUEST_ACCEPT_PHASE_TWO, numPassedRequests, numFailedRequests,consumerLatch,sharedFileWriter,metrics)).start();
        }
        consumerLatch.await();
        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        long latency = endTime.getTime() - startTime.getTime();

        //write csv files
        try{
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("results.csv"));
            for(String line : sharedFileWriter.getFileContent()){
                bufferedWriter.write(line);
            }
        }catch(IOException e){
            System.out.println("failed to write file");
        }

        printPerformance(metrics,latency);
    }

    public static  void printPerformance(Queue<Metric> metrics, long latency){
        List<Metric> metricList = new ArrayList<>();
        long sum = 0;
        while(!metrics.isEmpty()){
            Metric metric = metrics.poll();
            metricList.add(metric);
            sum += metric.getLatency();
        }

        metricList.sort(((o1, o2) -> (int) (o1.getLatency() - o2.getLatency())));
        int size = metricList.size();
        long medianResTime = metricList.get(size / 2).getLatency();
        long p99 = metricList.get((int)(size * 0.99)).getLatency();
        double meanResTime = (double)(sum) / size;
        long minResTime = metricList.get(0).getLatency();
        long maxResTime = metricList.get(size - 1).getLatency();
        int throughput = (int) ((N_REQUESTS * 1000) / latency);

        System.out.println();
        System.out.println("******* Performance Calculation for Part Two *******");
        System.out.println("mean response time: " + meanResTime +"ms");
        System.out.println("median response time: " + medianResTime+"ms");
        System.out.println("throughput: " + throughput+ " requests per second");
        System.out.println("p99 (99th percentile) response time: " + p99+"ms");
        System.out.println("minimum response time: "+minResTime+"ms");
        System.out.println("maximum response time: "+maxResTime+"ms");
        System.out.println("***************************************************");
    }

}

