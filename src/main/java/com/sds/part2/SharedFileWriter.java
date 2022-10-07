package com.sds.part2;

import java.util.ArrayList;
import java.util.List;

public class SharedFileWriter {
    private final List<String> content;

    public SharedFileWriter() {
        content = new ArrayList<>();
        String title = "start_time" +
                "," + "request_type" +
                "," + "latency" +
                "," + "response_code" +
                "\n";
        content.add(title);
    }

    public synchronized void addData(List<String> data){
        content.addAll(data);
    }

    public List<String> getFileContent(){
        return this.content;
    }
}
