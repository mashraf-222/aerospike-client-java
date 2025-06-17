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

package com.aerospike.client.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.Log;


public class YamlConfigProvider implements ConfigurationProvider {
    private static final String YAML_SERIALIZERS_PATH = "com.aerospike.client.configuration.serializers.";
    private static final String DEFAULT_CONFIG_URL_PREFIX = "file://";
    private static Path configurationPath;
    private Configuration configuration;
    private long lastModified;

    public YamlConfigProvider(String configEnvValue) {
        setConfigPath(configEnvValue);
        loadConfiguration();
    }

    private void setConfigPath(String configEnvValue) {
        try {
            if (!configEnvValue.startsWith(DEFAULT_CONFIG_URL_PREFIX)) {
                configEnvValue = DEFAULT_CONFIG_URL_PREFIX + configEnvValue;
            }
            URI envURI = new URI(configEnvValue);
            URL envURL = envURI.toURL();
            configurationPath = convertURLToPath(envURL);
        } catch (Exception e) {
            Log.error("Could not parse the config env var");
        }
    }

    private static Path convertURLToPath(URL url) throws URISyntaxException {
        URI uri = url.toURI();
        return Paths.get(uri);
    }

    public Configuration fetchConfiguration() {
        return configuration;
    }

    public Configuration fetchDynamicConfiguration() {
        if (configuration.staticConfiguration != null) {
           configuration.staticConfiguration = null;
        };
        return configuration;
    }

    /**
     * Attempt to load a YAML configuration from the configurationPath
     * @return True if a YAML config file could be loaded and parsed
     */

    public boolean loadConfiguration() {
        if (configurationPath == null) {
            Log.error("The YAML config file path has not been set. Check the config env variable");
            return false;
        }
        File file = new File(configurationPath.toString());
        if (!file.exists()) {
            if (Log.warnEnabled()) {
                Log.warn("No YAML config file could be located at: " + configurationPath.toString());
                return false;
            }
        }
        long newLastModified = file.lastModified();
        if (newLastModified > lastModified) {
            if (lastModified == 0) {
                if (Log.debugEnabled()) {
                    Log.debug("Initial read of YAML config file...");
                }
            } else {
                if (Log.debugEnabled()) {
                    Log.debug("YAML config file has been modified.  Loading...");
                }
            }
            lastModified = newLastModified;
        }
        try {
            ConfigurationTypeDescription configurationTypeDescription = new ConfigurationTypeDescription();
            LoaderOptions yamlLoaderOptions = new LoaderOptions();
            Map<Class<?>, TypeDescription> typeDescriptions = configurationTypeDescription.buildTypeDescriptions(YAML_SERIALIZERS_PATH, Configuration.class);
            Constructor typeDescriptionConstructor = new Constructor(Configuration.class, yamlLoaderOptions);
            typeDescriptions.values().forEach(typeDescriptionConstructor::addTypeDescription);
            Yaml yaml = new Yaml(typeDescriptionConstructor);
            FileInputStream fileInputStream = new FileInputStream(configurationPath.toString());
            configuration = yaml.load(fileInputStream);
            if (Log.debugEnabled()) {
                Log.debug("YAML config successfully loaded.");
            }
            return true;
        } catch (IOException e) {
            throw new AerospikeException("YAML Configuration file could not be read from: " + configurationPath + ". " + e.getMessage());
        } catch (YAMLException e) {
            throw new AerospikeException("Unable to parse YAML file: " + e.getMessage());
        }
    }
}
