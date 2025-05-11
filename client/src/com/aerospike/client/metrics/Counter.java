package com.aerospike.client.metrics;

import java.util.concurrent.atomic.*;
import java.util.concurrent.ConcurrentHashMap;

public class Counter {

    private final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();

    private AtomicLong total = new AtomicLong(0);

    public Counter() {
    }

    public void increment(String ns) {
        String namespace = (ns == null)? "_" : ns;
        counterMap.compute(namespace, (k, v) -> {
            if (v == null) {
                return new AtomicLong(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        });
        total.incrementAndGet();
    }

    public void increment(String ns, long count) {
        String namespace = (ns == null)? "_" : ns;
        counterMap.compute(namespace, (k, v) -> {
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

    public long getCountByNS(String namespace) {
        AtomicLong count = counterMap.get(namespace);
        if (count == null) {
            return 0;
        }
        return counterMap.get(namespace).longValue();
    }

    public ConcurrentHashMap<String, AtomicLong> getNSmap() {
        return counterMap;
    }
}
