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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.Log;


public class YamlConfigProvider implements ConfigurationProvider {
    private static final String YAML_SERIALIZERS_PATH = "com.aerospike.client.configuration.serializers.";
    private static final String DEFAULT_CONFIG_URL_PREFIX = "file:///";
    private static Path configurationPath;
    private Configuration configuration;
    public long lastModified;

    public YamlConfigProvider(String configEnvValue) {
        setConfigPath(configEnvValue);
        loadConfiguration();
    }

    public void setConfigPath(String configEnvValue) {
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

    public static Path convertURLToPath(URL url) throws URISyntaxException {
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

    public boolean loadConfiguration() {
        if (configurationPath == null) {
            Log.error("The YAML config file path has not been set. Check the config env variable");
            return false;
        }
        ConfigurationTypeDescription configurationTypeDescription = new ConfigurationTypeDescription();
        LoaderOptions yamlLoaderOptions = new LoaderOptions();

        Map<Class<?>, TypeDescription> typeDescriptions = configurationTypeDescription.buildTypeDescriptions(YAML_SERIALIZERS_PATH, Configuration.class);
        Constructor typeDescriptionConstructor = new Constructor(Configuration.class, yamlLoaderOptions);
        Yaml yaml = new Yaml(typeDescriptionConstructor);

        typeDescriptions.values().forEach(typeDescriptionConstructor::addTypeDescription);
        try(FileInputStream fileInputStream = new FileInputStream(configurationPath.toString())) {
            File file = new File(configurationPath.toString());
            long newLastModified = file.lastModified();
            if (newLastModified > lastModified) {
                if (lastModified == 0) {
                    Log.debug("Initial read of YAML config file...");
                } else {
                    Log.debug("YAML config file has been modified.  Loading...");
                }
                lastModified = newLastModified;
                configuration = yaml.load(fileInputStream);
                Log.debug("Config successfully loaded.");
                return true;
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
        return false;
    }
}
