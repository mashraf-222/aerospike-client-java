package com.aerospike.client.metrics;



public interface Metric {
    public void increment();
    public void increment(String namespace);
    public void increment(String namespace, int count);
}
