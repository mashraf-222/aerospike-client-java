/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import com.aerospike.client.*;
import com.aerospike.client.configuration.serializers.*;
import com.aerospike.client.configuration.serializers.dynamicconfig.*;
import com.aerospike.client.policy.*;

import java.util.*;

/**
 * Client periodic metrics configuration.
 */
public final class MetricsPolicy {
	/**
	 * Listener that handles metrics notification events. The default listener implementation
	 * writes the metrics snapshot to a file which will later be read and forwarded to
	 * OpenTelemetry by a separate offline application.
	 * <p>
	 * The listener could be overridden to send the metrics snapshot directly to OpenTelemetry.
	 */
	public MetricsListener listener;

	/**
	 * Directory path to write metrics log files for listeners that write logs.
	 * <p>
	 * Default: <current directory>
	 */
	public String reportDir = ".";

	/**
	 * Metrics file size soft limit in bytes for listeners that write logs.
	 * <p>
	 * When reportSizeLimit is reached or exceeded, the current metrics file is closed and a new
	 * metrics file is created with a new timestamp. If reportSizeLimit is zero, the metrics file
	 * size is unbounded and the file will only be closed when
	 * {@link com.aerospike.client.AerospikeClient#disableMetrics()} or
	 * {@link com.aerospike.client.AerospikeClient#close()} is called.
	 * <p>
	 * Default: 0
	 */
	public long reportSizeLimit = 0;

	/**
	 * Number of cluster tend iterations between metrics notification events. One tend iteration
	 * is defined as {@link ClientPolicy#tendInterval} (default 1 second) plus the time to tend all
	 * nodes.
	 * <p>
	 * Default: 30
	 */
	public int interval = 30;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 * <p>
	 * Default: 7
	 */
	public int latencyColumns = 7;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "&lt;=1ms" and "&gt;1ms". Examples:
	 * <pre>{@code
	 * // latencyColumns=7 latencyShift=1
	 * <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
	 *
	 * // latencyColumns=5 latencyShift=3
	 * <=1ms >1ms >8ms >64ms >512ms
	 * }</pre>
	 * Default: 1
	 */
	public int latencyShift = 1;

	/**
	 * Application ID.  Metrics are loosely tied to this.  Changing the app_ID will not reset the metric counters, as
	 * specified by PM team
	 */
	public String app_ID;

	/**
	 * Labels that can be sent to the metrics output
	 */
	public List<Map<String, String>> labels;

	private boolean metricsRestartRequired = false;

	/**
	 * Copy batch policy from another batch policy AND override certain policy attributes if they exist in the
	 * configProvider.
	 */
	public MetricsPolicy(MetricsPolicy other, Configuration config) {
		this(other);
		if ( config == null) {
			return;
		}
		DynamicMetricsConfig dynMC = config.dynamicConfiguration.dynamicMetricsConfig;
		if (dynMC.app_id != null) this.app_ID = dynMC.app_id.value;
		if (dynMC.labels != null) this.labels = dynMC.labels;
		if (dynMC.latencyShift != null)  {
			if (dynMC.latencyShift.value != this.latencyShift) {
				metricsRestartRequired = true;
			}
			this.latencyShift = dynMC.latencyShift.value;
		}
		if (dynMC.latencyColumns != null) {
			if (dynMC.latencyColumns.value != this.latencyColumns) {
				metricsRestartRequired = true;
			}
			this.latencyColumns = dynMC.latencyColumns.value;
		}
		if (latencyColumns < 1) {
			Log.error("An invalid # of latency columns was provided. Setting latency columns to default (7).");
			latencyColumns = 7;
		}
	}

	/**
	 * Copy constructor.
	 */
	public MetricsPolicy(MetricsPolicy other) {
		this.listener = other.listener;
		this.reportDir = other.reportDir;
		this.reportSizeLimit = other.reportSizeLimit;
		this.interval = other.interval;
		this.latencyColumns = other.latencyColumns;
		this.latencyShift = other.latencyShift;
		this.app_ID = other.app_ID;
		this.labels = other.labels;
	}

	/**
	 * Default constructor.
	 */
	public MetricsPolicy() {
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setListener(MetricsListener listener) {
		this.listener = listener;
	}

	public void setReportDir(String reportDir) {
		this.reportDir = reportDir;
	}

	public void setReportSizeLimit(long reportSizeLimit) {
		this.reportSizeLimit = reportSizeLimit;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public void setLatencyColumns(int latencyColumns) {
		this.latencyColumns = latencyColumns;
	}

	public void setLatencyShift(int latencyShift) { this.latencyShift = latencyShift; }

	public void setApp_ID(String app_ID) { this.app_ID = app_ID; }

	public void setLabels(List<Map<String, String>> labels) { this.labels = labels; }

	public boolean isMetricsRestartRequired() {
		return metricsRestartRequired;
	}

	public void setMetricsRestartRequired(boolean metricsRestartRequired) {
		this.metricsRestartRequired = metricsRestartRequired;
	}
}
