package com.gprofiler.spark;

public class ThreadInfoUpdate {
    private final long threadId;
    private final String threadName;

    public ThreadInfoUpdate(long threadId, String threadName) {
        this.threadId = threadId;
        this.threadName = threadName;
    }

    public long getThreadId() {
        return threadId;
    }

    public String getThreadName() {
        return threadName;
    }
}
