package com.aerospike.client.metrics;

public class Histogram implements Metric{
    public void increment(String type, int value) {

    }

    @Override
    public void increment(MetricType type, int value) {

    }

    @Override
    public void increment(MetricType type, String namespace, int value) {

    }
}
