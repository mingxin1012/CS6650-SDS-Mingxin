package com.sds;
import com.sds.model.LiftRideEvent;
import com.sds.model.RequestCounter;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;

import java.io.PrintWriter;
import java.util.concurrent.*;

public class PostConsumer implements Runnable {

    private final BlockingQueue<LiftRideEvent> inputQueue;
    private final LiftRideEvent poison;
    private final SkiersApi apiInstance;
    private final int numOfRequests;
    private final RequestCounter numPassedRequests;
    private final RequestCounter numFailedRequests;

    private final static Integer MAX_TRIES = 5;
    private final CountDownLatch latch;


    public PostConsumer(String base_path, BlockingQueue<LiftRideEvent> inputQueue, LiftRideEvent poison,
                         int numOfRequests, RequestCounter numPassedRequests, RequestCounter numFailedRequests, CountDownLatch latch) {
        this.inputQueue = inputQueue;
        this.poison = poison;
        this.numOfRequests = numOfRequests;
        this.apiInstance = new SkiersApi();
        ApiClient client = apiInstance.getApiClient();
        client.setBasePath(base_path);
        this.numFailedRequests = numFailedRequests;
        this.numPassedRequests = numPassedRequests;
        this.latch = latch;

    }

    @Override
    public void run() {
        //worker loop keeps taking en element from the queue as long as the producer is still running or as 
        //long as the queue is not empty:
        System.out.println("Consumer Thread " + Thread.currentThread().getName() + " START");
        int counter = 0;
        try {
            for(int i = 0; i < numOfRequests; i++) {
                //System.out.println("Post Request " + Thread.currentThread().getName() + " START");
                LiftRideEvent event = inputQueue.take();

                if (event == poison ) {
                    break;
                }
                //process queueElement
                this.retryRequests(event,apiInstance);
                counter++;
                //System.out.println("Post Request " + Thread.currentThread().getName() + " END");
            }
        } catch (InterruptedException e) {
            System.err.println("Thread is interrupted");
            e.printStackTrace();
        } catch (ApiException e){
            this.numFailedRequests.inc(1);
            System.err.println("Exception when calling SkierApi#writeNewLiftRide");
            e.printStackTrace();
        }
        this.numPassedRequests.inc(counter);
        System.out.println("Consumer Thread " + Thread.currentThread().getName() + " END");
        this.latch.countDown();
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