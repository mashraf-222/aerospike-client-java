/*
 * Copyright 2012-2023 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.metrics;


/**
 * Optional extended node metrics. Used when extended metrics is enabled
 * (See {@link com.aerospike.client.AerospikeClient#enableMetrics(MetricsPolicy)}).
 */
public final class NodeMetrics {
	private final Histograms histograms;
	public final Counter errorCounter;
	public final Counter timeoutCounter;
	public final Counter keyBusyCounter;
	public final Counter bytesInCounter;
	public final Counter bytesOutCounter;

	/**
	 * Initialize extended node metrics.
	 */
	public NodeMetrics(MetricsPolicy policy) {
		int latencyColumns = policy.latencyColumns;
		int latencyShift = policy.latencyShift;
		this.errorCounter = new Counter();
		this.timeoutCounter = new Counter();
		this.keyBusyCounter = new Counter();
		this.bytesInCounter = new Counter();
		this.bytesOutCounter = new Counter();

		histograms = new Histograms(latencyColumns, latencyShift);
	}

	/**
	 * Add elapsed time in nanoseconds to histogram map buckets corresponding to namespace & latency type.
	 */
	public void addLatency(String namespace, LatencyType type, long elapsed) {
		histograms.addLatency(namespace, type, elapsed);
	}

	public Histograms getHistograms() {
		return histograms;
	}
}
