package com.aerospike.client.metrics;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.ConcurrentHashMap;

public class Counter implements Metric{

    private final ConcurrentHashMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    private final MetricType type;
    private AtomicLong total = new AtomicLong(0);

    public Counter(MetricType type) {
        this.type = type;
    }

    @Override
    public void increment() {
        String namespace = "_";
        map.compute(namespace, (k, v) -> {
            if (v == null) {
                return new AtomicLong(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        });
    }

    @Override
    public void increment(String ns) {
        String namespace = (ns == null)? "_" : ns;
        map.compute(namespace, (k, v) -> {
            if (v == null) {
                return new AtomicLong(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        });
        total.incrementAndGet();
    }

    @Override
    public void increment(String ns, int count) {
        String namespace = (ns == null)? "_" : ns;
        map.compute(namespace, (k, v) -> {
            if (v == null) {
                return new AtomicLong(count);
            } else {
                v.getAndAdd(count);
                return v;
            }
        });
        total.incrementAndGet();
    }


    public long getTotal() {
        return total.get();
    }

    public ConcurrentHashMap<String, AtomicLong> getNSmap() {
        return map;
    }
}
