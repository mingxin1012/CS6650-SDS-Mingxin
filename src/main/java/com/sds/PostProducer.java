package com.sds;

import com.sds.model.LiftRideEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class PostProducer implements Runnable {
    private final BlockingQueue<LiftRideEvent> inputQueue;
    private final int numPoisonPills;
    private final LiftRideEvent poison;
    private final int numPostRequests;
    private final CountDownLatch latch;


    public PostProducer(BlockingQueue<LiftRideEvent> inputQueue, int numPoisonPills, LiftRideEvent poison, int numPostRequests, CountDownLatch latch) {
        this.inputQueue = inputQueue;
        this.numPoisonPills = numPoisonPills;
        this.poison = poison;
        this.numPostRequests = numPostRequests;
        this.latch = latch;

    }

    public void run() {
        try {
            System.out.println("Producer " + Thread.currentThread().getName() + " START");
            generateNumbers();
            System.out.println("Producer " + Thread.currentThread().getName() + " END");
        } catch (InterruptedException e) {
            System.err.println("Thread is interrupted");
            e.printStackTrace();
        }
        this.latch.countDown();
    }

    private void generateNumbers() throws InterruptedException {
        for (int i = 0; i < numPostRequests; i++) {
            inputQueue.put(new LiftRideEvent());
        }
        for (int j = 0; j < numPoisonPills; j++) {
            inputQueue.put(poison);
        }
    }
}
