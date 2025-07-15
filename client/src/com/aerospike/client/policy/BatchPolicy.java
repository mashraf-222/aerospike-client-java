/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.policy;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchReadConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchWriteConfig;

/**
 * Batch parent policy.
 */
public class BatchPolicy extends Policy {
	/**
	 * This field is ignored and deprecated. Sync batch node commands are now always issued using
	 * virtual threads in parallel. Async batch node commands always ignored this field. This field
	 * only exists to maintain api compatibility when switching between aerospike-client-jdk21 and
	 * aerospike-client-jdk8 packages.
	 */
	@Deprecated
	public int maxConcurrentThreads = 1;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for in-memory
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * <p>
	 * For batch commands with smaller sized records (&lt;= 1K per record), inline
	 * processing will be significantly faster on in-memory namespaces.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: true
	 */
	public boolean allowInline = true;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for SSD
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * Server versions &lt; 6.0 ignore this field.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: false
	 */
	public boolean allowInlineSSD = false;

	/**
	 * Should all batch keys be attempted regardless of errors. This field is used on both
	 * the client and server. The client handles node specific errors and the server handles
	 * key specific errors.
	 * <p>
	 * If true, every batch key is attempted regardless of previous key specific errors.
	 * Node specific errors such as timeouts stop keys to that node, but keys directed at
	 * other nodes will continue to be processed.
	 * <p>
	 * If false, the server will stop the batch to its node on most key specific errors.
	 * The exceptions are {@link com.aerospike.client.ResultCode#KEY_NOT_FOUND_ERROR} and
	 * {@link com.aerospike.client.ResultCode#FILTERED_OUT} which never stop the batch.
	 * <p>
	 * Server versions &lt; 6.0 do not support this field and treat this value as false
	 * for key specific errors.
	 * <p>
	 * Default: true
	 */
	public boolean respondAllKeys = true;

	/**
	 * This method is deprecated and will eventually be removed.
	 * The set name is now always sent for every distinct namespace/set in the batch.
	 * <p>
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is necessary for batch writes and batch reads when authentication is enabled and
	 * security roles are defined on a per set basis.
	 * <p>
	 * Default: false
	 */
	@Deprecated
	public boolean sendSetName;

	/**
	 * Copy batch policy from another batch policy AND override certain policy attributes if they exist in the
	 * configProvider.
	 */
	public BatchPolicy(BatchPolicy other, ConfigurationProvider configProvider) {
		this(other);
		if (configProvider == null) {
			return;
		}
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		DynamicConfiguration dConfig = config.getDynamicConfiguration();
		if (dConfig == null) {
			return;
		}
		DynamicBatchReadConfig dynBRC = dConfig.getDynamicBatchReadConfig();
		if (dynBRC == null) {
			return;
		}

		if (dynBRC.readModeAP != null && this.readModeAP != dynBRC.readModeAP) {
			this.readModeAP = dynBRC.readModeAP;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.readModeAP = " + this.readModeAP);
			}
		}
		if (dynBRC.readModeSC != null && this.readModeSC != dynBRC.readModeSC) {
			this.readModeSC = dynBRC.readModeSC;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.readModeSC = " + this.readModeSC);
			}
		}
		if (dynBRC.connectTimeout != null && this.connectTimeout != dynBRC.connectTimeout.value) {
			this.connectTimeout = dynBRC.connectTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynBRC.replica != null && this.replica != dynBRC.replica) {
			this.replica = dynBRC.replica;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.replica = " + this.replica);
			}
		}
		if (dynBRC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynBRC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynBRC.sleepBetweenRetries.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynBRC.socketTimeout != null && this.socketTimeout != dynBRC.socketTimeout.value) {
			this.socketTimeout = dynBRC.socketTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynBRC.timeoutDelay != null && this.timeoutDelay != dynBRC.timeoutDelay.value) {
			this.timeoutDelay = dynBRC.timeoutDelay.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynBRC.totalTimeout != null && this.totalTimeout != dynBRC.totalTimeout.value) {
			this.totalTimeout = dynBRC.totalTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynBRC.maxRetries != null && this.maxRetries != dynBRC.maxRetries.value) {
			this.maxRetries = dynBRC.maxRetries.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynBRC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynBRC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynBRC.maxConcurrentThreads.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.maxConcurrentThreads = " + this.maxConcurrentThreads);
			}
		}
		if (dynBRC.allowInline != null && this.allowInline != dynBRC.allowInline.value) {
			this.allowInline = dynBRC.allowInline.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.allowInline = " + this.allowInline);
			}
		}
		if (dynBRC.allowInlineSSD != null && this.allowInlineSSD != dynBRC.allowInlineSSD.value) {
			this.allowInlineSSD = dynBRC.allowInlineSSD.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.allowInlineSSD = " + this.allowInlineSSD);
			}
		}
		if (dynBRC.respondAllKeys != null && this.respondAllKeys != dynBRC.respondAllKeys.value) {
			this.respondAllKeys = dynBRC.respondAllKeys.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.respondAllKeys = " + this.respondAllKeys);
			}
		}
	}
	/**
	 * Copy batch policy from another batch policy.
	 */
	public BatchPolicy(BatchPolicy other) {
		super(other);
		this.maxConcurrentThreads = other.maxConcurrentThreads;
		this.allowInline = other.allowInline;
		this.allowInlineSSD = other.allowInlineSSD;
		this.respondAllKeys = other.respondAllKeys;
		this.sendSetName = other.sendSetName;
	}

	/**
	 * Copy batch policy from another policy.
	 */
	public BatchPolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public BatchPolicy() {
	}

	/**
	 * Default batch read policy.
	 */
	public static BatchPolicy ReadDefault() {
		return new BatchPolicy();
	}

	/**
	 * Default batch write policy.
	 */
	public static BatchPolicy WriteDefault() {
		BatchPolicy policy = new BatchPolicy();
		policy.maxRetries = 0;
		return policy;
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setMaxConcurrentThreads(int maxConcurrentThreads) {
		this.maxConcurrentThreads = maxConcurrentThreads;
	}

	public void setAllowInline(boolean allowInline) {
		this.allowInline = allowInline;
	}

	public void setAllowInlineSSD(boolean allowInlineSSD) {
		this.allowInlineSSD = allowInlineSSD;
	}

	public void setRespondAllKeys(boolean respondAllKeys) {
		this.respondAllKeys = respondAllKeys;
	}

	/**
	 * Apply batch_write config properties if they exist in the configProvider (BatchWrite).
	 */
	public void graftBatchWriteConfig(ConfigurationProvider configProvider) {
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		DynamicBatchWriteConfig dynBWC = config.dynamicConfiguration.dynamicBatchWriteConfig;
		if (dynBWC == null) {
			return;
		}

		if (dynBWC.connectTimeout != null && this.connectTimeout != dynBWC.connectTimeout.value) {
			this.connectTimeout = dynBWC.connectTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynBWC.failOnFilteredOut != null && this.failOnFilteredOut != dynBWC.failOnFilteredOut.value) {
			this.failOnFilteredOut = dynBWC.failOnFilteredOut.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.failOnFilteredOut = " + this.failOnFilteredOut);
			}
		}
		if (dynBWC.replica != null && this.replica != dynBWC.replica) {
			this.replica = dynBWC.replica;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.replica = " + this.replica);
			}
		}
		if (dynBWC.sendKey != null && 	this.sendKey != dynBWC.sendKey.value) {
			this.sendKey = dynBWC.sendKey.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.sendKey = " + this.sendKey);
			}
		}
		if (dynBWC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynBWC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynBWC.sleepBetweenRetries.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynBWC.socketTimeout != null && this.socketTimeout != dynBWC.socketTimeout.value) {
			this.socketTimeout = dynBWC.socketTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynBWC.timeoutDelay != null && this.timeoutDelay != dynBWC.timeoutDelay.value) {
			this.timeoutDelay = dynBWC.timeoutDelay.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynBWC.totalTimeout != null && this.totalTimeout != dynBWC.totalTimeout.value) {
			this.totalTimeout = dynBWC.totalTimeout.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynBWC.maxRetries != null && this.maxRetries != dynBWC.maxRetries.value) {
			this.maxRetries = dynBWC.maxRetries.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynBWC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynBWC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynBWC.maxConcurrentThreads.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.maxConcurrentThreads = " + this.maxConcurrentThreads);
			}
		}
		if (dynBWC.allowInlineSSD != null && this.allowInlineSSD != dynBWC.allowInlineSSD.value) {
			this.allowInlineSSD = dynBWC.allowInlineSSD.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.allowInlineSSD = " + this.allowInlineSSD);
			}
		}
		if (dynBWC.respondAllKeys != null && this.respondAllKeys != dynBWC.respondAllKeys.value) {
			this.respondAllKeys = dynBWC.respondAllKeys.value;
			if (Log.infoEnabled()) {
				Log.info("Set BatchPolicy.respondAllKeys = " + this.respondAllKeys);
			}
		}
	}
}
