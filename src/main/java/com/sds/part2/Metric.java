package com.sds.part2;

public class Metric {
    private final String startTimeViewer;
    private final String reqType;
    private final long latency;
    private final int resCode;

    public Metric(String startTimeViewer, String reqType, long latency, int resCode) {
        this.startTimeViewer = startTimeViewer;
        this.reqType = reqType;
        this.latency = latency;
        this.resCode = resCode;
    }

    public String getStartTimeViewer() {
        return startTimeViewer;
    }

    public String getReqType() {
        return reqType;
    }

    public long getLatency() {
        return latency;
    }

    public int getResCode() {
        return resCode;
    }

    @Override
    public String toString() {
        return startTimeViewer +
                "," + reqType +
                "," + latency +
                "," + resCode +
                "\n";
    }
}
