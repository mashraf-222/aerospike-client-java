package com.aerospike.client.metrics;

import java.util.concurrent.atomic.*;

public class Counter implements Metric{

    private final AtomicLong value = new AtomicLong();

    private String name;

    private MetricType type;

    public Counter() {
    }

    @Override
    public void increment(MetricType type, int value) {
        this.value.getAndIncrement();
    }

    @Override
    public void increment(MetricType type, String namespace, int value) {

    }
}
