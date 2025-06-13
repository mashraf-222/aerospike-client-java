/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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

package com.aerospike.client.configuration.serializers;

public class Configuration {
    public StaticConfiguration staticConfiguration;
    public DynamicConfiguration dynamicConfiguration;

    public Configuration() {}

    public StaticConfiguration getStaticConfiguration() {
        return this.staticConfiguration;
    }

    public void setStaticConfiguration(StaticConfiguration staticConfiguration) {
        this.staticConfiguration = staticConfiguration;
    }

    public DynamicConfiguration getDynamicConfiguration() {
        return this.dynamicConfiguration;
    }

    public void setDynamicConfiguration(DynamicConfiguration dynamicConfiguration) {
        this.dynamicConfiguration = dynamicConfiguration;
    }

    public boolean hasMetrics() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicMetricsConfig != null;
    }

    public boolean hasDBWCsendKey() {
		return dynamicConfiguration != null &&
				dynamicConfiguration.dynamicBatchWriteConfig != null &&
				dynamicConfiguration.dynamicBatchWriteConfig.sendKey != null;
	}

    public boolean hasDBUDFCsendKey() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicBatchUDFconfig != null &&
                dynamicConfiguration.dynamicBatchUDFconfig.sendKey != null;
    }

    public boolean hasDBDCsendKey() {
        return dynamicConfiguration != null &&
                dynamicConfiguration.dynamicBatchDeleteConfig != null &&
                dynamicConfiguration.dynamicBatchDeleteConfig.sendKey != null;
    }

    public String getAppID() {
        if (dynamicConfiguration.dynamicClientConfig.getAppID() != null) {
            return dynamicConfiguration.dynamicClientConfig.appID.value;
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "\n{" +
            "\n\tstatic= " + getStaticConfiguration() +
            "\n\tdynamic= " + getDynamicConfiguration() +
            "\n}\n";
    }
}
