package com.sds.model;

public class RequestCounter {
    private int count;

    public RequestCounter() {
        this.count = 0;
    }

    synchronized public void inc() {
        count++;
    }

    public int getVal() {
        return this.count;
    }
}
