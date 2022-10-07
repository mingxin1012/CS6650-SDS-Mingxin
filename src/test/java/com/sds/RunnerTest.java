package com.sds;

import com.sds.model.LiftRideEvent;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.ResortIDSeasonsBody;
import junit.framework.TestCase;
import org.junit.Test;

import java.sql.Timestamp;

public class RunnerTest extends TestCase {
    private final static String BASE_PATH = "http://localhost:8080/skiers_Web/skier";

    @Test
    public void testLatency() throws ApiException {
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        for(int i = 0; i < 10000; i++){
            SkiersApi skiersApi = new SkiersApi();
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_PATH);
            skiersApi.setApiClient(apiClient);
            retryRequests(new LiftRideEvent(),skiersApi );
        }
        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        long overallLatency = endTime.getTime() - startTime.getTime();

        System.out.println("latency is " + overallLatency / 10000.0);

    }

    private void retryRequests(LiftRideEvent event, SkiersApi apiInstance) throws ApiException {
        int count = 0;
        int maxTries = 5;
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