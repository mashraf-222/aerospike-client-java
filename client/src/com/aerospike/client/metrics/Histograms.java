package com.aerospike.client.metrics;

import java.util.concurrent.ConcurrentHashMap;

public class Histograms {
    private static final long NS_TO_MS = 1000000;
    private final ConcurrentHashMap<String, LatencyBuckets[]> histoMap = new ConcurrentHashMap<>();
    private final int histoShift;
    private final int columnCount;
    private final static String noNSLabel = " ";
    private final int max;

    /**
     * In our context, a Histogram is a map of namespaces to LatencyBuckets
     *
     * @param columnCount	number of histogram columns or "buckets"
     * @param shift		power of 2 multiple between each range bucket in histogram starting at bucket 3.
     * 							The first 2 buckets are "&lt;=1ms" and "&gt;1ms".
     */
    public Histograms(int columnCount, int shift) {
        this.histoShift = shift;
        this.columnCount = columnCount;
        max = LatencyType.getMax();
    }

    private LatencyBuckets[] createBuckets() {
        LatencyBuckets[] buckets = new LatencyBuckets[max];

        for (int i = 0; i < max; i++) {
            buckets[i] = new LatencyBuckets(columnCount, histoShift);
        }
        return buckets;
    }

    /**
     * Increment count of bucket corresponding to the namespace & elapsed time in nanoseconds.
     */
    public void addLatency(String namespace, LatencyType type, long elapsed) {
        if (namespace == null) {
            namespace = noNSLabel;
        }
        LatencyBuckets[] buckets = getBuckets(namespace);
        if (buckets == null) {
            buckets = createBuckets();
            LatencyBuckets[] finalBuckets = buckets;
            histoMap.computeIfAbsent(namespace, k -> finalBuckets);
        }
        buckets[type.ordinal()].add(elapsed);
    }

    /**
     * Return number of buckets.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * Return the LatencyBuckets for a given namespace and LatencyType ordinal
     */
    public LatencyBuckets[] getBuckets(String namespace) {
        return histoMap.get(namespace);
    }

    /**
     * Return the histogram map
     */
    public ConcurrentHashMap<String, LatencyBuckets[]> getMap () {
        return histoMap;
    }
}
