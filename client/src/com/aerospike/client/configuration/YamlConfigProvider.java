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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import com.aerospike.client.Log;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.aerospike.client.configuration.serializers.Configuration;
import org.yaml.snakeyaml.error.YAMLException;

public class YamlConfigProvider implements ConfigurationProvider {
    private static final String configurationPathEnv = "CONFIGURATION_PATH";
    private static final String configurationPathProp = "configuration.path";
    private static final String yamlSerializersPath = "com.aerospike.client.configuration.serializers.";
    private static String configurationPath = System.getenv().getOrDefault(configurationPathEnv, System.getProperty(configurationPathProp, System.getProperty("user.dir")));
    private Configuration configuration;
    public long lastModified;

    public YamlConfigProvider() {
        loadConfiguration();
    }

    public YamlConfigProvider(String configFilePath) {
        configurationPath = configFilePath;
        loadConfiguration();
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

    public void loadConfiguration() {
        ConfigurationTypeDescription configurationTypeDescription = new ConfigurationTypeDescription();
        LoaderOptions yamlLoaderOptions = new LoaderOptions();

        Map<Class<?>, TypeDescription> typeDescriptions = configurationTypeDescription.buildTypeDescriptions(yamlSerializersPath, Configuration.class);
        Constructor typeDescriptionConstructor = new Constructor(Configuration.class, yamlLoaderOptions);
        Yaml yaml = new Yaml(typeDescriptionConstructor);

        typeDescriptions.values().forEach(typeDescriptionConstructor::addTypeDescription);
        try(FileInputStream fileInputStream = new FileInputStream(configurationPath)) {
            File file = new File(configurationPath);
            long newLastModified = file.lastModified();
            if (newLastModified > lastModified) {
                if (lastModified == 0) {
                    Log.debug("Initial read of YAML config file...");
                } else {
                    Log.debug("YAML config file has been modified.  Loading...");
                }
                lastModified = newLastModified;
                configuration = yaml.load(fileInputStream);
            }
            else {
                Log.debug("YAML config file has NOT been modified.  NOT loading.");
            }
        } catch (FileNotFoundException e) {
            Log.error("YAML configuration file could not be found at: " + configurationPath + ". " + e.getMessage());
        } catch (IOException e) {
            Log.error("YAML Configuration file could not be read from: " + configurationPath + ". " + e.getMessage());
        } catch (YAMLException e) {
            Log.error("Unable to parse YAML file: " + e.getMessage());
        }
    }
}
