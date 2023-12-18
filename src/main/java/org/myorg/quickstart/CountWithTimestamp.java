package org.myorg.quickstart;


import org.apache.flink.runtime.state.StateObject;

public class CountWithTimestamp implements StateObject {
    public String key;
    public long count;
    public long lastModified;

    @Override
    public String toString() {
        return "CountWithTimestamp{" +
                "key='" + key + '\'' +
                ", count=" + count +
                ", lastModified=" + lastModified +
                '}';
    }

    @Override
    public void discardState() throws Exception {

    }

    @Override
    public long getStateSize() {
        return 0;
    }
}