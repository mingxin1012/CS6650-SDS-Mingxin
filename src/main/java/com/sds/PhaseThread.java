package com.sds;

import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class PhaseThread extends Thread{

    private final BlockingQueue<LiftRideEvent> inputQueue;
    private final SkiersApi apiInstance;
    private final CountDownLatch latch;
    private final RequestCounter numPassedRequests;
    private final RequestCounter numFailedRequests;

    private int counter;

    private final static String BASE_PATH = "http://35.92.186.195:8080/skiers_Web/skier";
    private final static Integer MAX_TRIES = 5;

    public PhaseThread(BlockingQueue<LiftRideEvent> inputQueue, SkiersApi apiInstance, CountDownLatch latch, RequestCounter numPassedRequests, RequestCounter numFailedRequests) {
        this.inputQueue = inputQueue;
        this.apiInstance = apiInstance;
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(BASE_PATH);
        apiInstance.setApiClient(apiClient);
        this.counter = 0;
        this.latch = latch;
        this.numFailedRequests = numFailedRequests;
        this.numPassedRequests = numPassedRequests;
    }
    @Override
    public void run() {
        //worker loop keeps taking en element from the queue as long as the producer is still running or as
        //long as the queue is not empty:
        while (counter<1000) {

            System.out.println("Phase1 post request " + Thread.currentThread().getName() + " START");
            try {
                LiftRideEvent event = inputQueue.take();

                //process queueElement
                this.retryRequests(event,apiInstance);
                counter++;
                this.numPassedRequests.inc();
            } catch (InterruptedException e) {
                System.err.println("Thread is interrupted");
                e.printStackTrace();
            } catch (ApiException e){
                this.numFailedRequests.inc();
                System.err.println("Exception when calling SkierApi#writeNewLiftRide");
                e.printStackTrace();
            } catch (Exception e){
                System.err.println("Unknown Errors");
                e.printStackTrace();
            }

            System.out.println("Phase1 post request " + Thread.currentThread().getName() + " END");
        }

        try {
            this.latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void retryRequests(LiftRideEvent event, SkiersApi apiInstance) throws ApiException{
        int count = 0;
        int maxTries = MAX_TRIES;
        while(true) {
            try {
                apiInstance.writeNewLiftRide(event.getBody(), event.getResortID(), event.getSeasonID(), event.getDayID(), event.getSkierID());
                break;
            } catch (ApiException e) {
                if (++count == maxTries) throw e;
            }
        }
    }
}
