package com.aerospike.client.metrics;



public interface Metric {
    public void increment(MetricType type, int value);
    public void increment(MetricType type, String namespace, int value);
}
