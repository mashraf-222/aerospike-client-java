package com.aerospike.client.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

public class Counter {

    private final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();
    private final AtomicLong total = new AtomicLong(0);
    private final static String noNSLabel = " ";

    /**
     * A Counter is a container for a namespace-aggregate map of AtomicLong counters
     */
    public Counter() {
    }

    /**
     * Increment the counter by 1 for the given namespace
     * @param ns - the namespace for the counter
     */
    public void increment(String ns) {
        String namespace = (ns == null)? noNSLabel : ns;
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

    /**
     * Increment the counter by the provided count amount for the given namespace
     * @param ns - the namespace for the counter
     * @param count - the amount by which to increment the counter
     */
    public void increment(String ns, long count) {
        String namespace = (ns == null)? noNSLabel : ns;
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

    /**
     * Get the counter's total, which is the sum of the counter across all namespaces
     * @return the total
     */
    public long getTotal() {
        return total.get();
    }

    /**
     * Get the counter's count for the provided namespace
     * @param namespace     the namespace for which we want the count
     * @return the count for the namespace
     */
    public long getCountByNS(String namespace) {
        AtomicLong count = counterMap.get(namespace);
        if (count == null) {
            return 0;
        }
        return counterMap.get(namespace).longValue();
    }
}
