// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.PluginDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Parser for plugin descriptor metadata.
 *
 * <p>This class provides parsing utilities without performing any IO.
 * File loading will be implemented in the loader phase.</p>
 */
public final class PluginDescriptorParser {

    public static final String KEY_NAME = "name";
    public static final String KEY_VERSION = "version";
    public static final String KEY_SPI_VERSION = "spiVersion";
    public static final String KEY_FACTORY_CLASS = "factoryClass";

    private PluginDescriptorParser() {
    }

    public static PluginDescriptor fromProperties(Properties properties) {
        Objects.requireNonNull(properties, "properties");
        Map<String, String> map = new HashMap<>();
        for (String name : properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        return fromMap(map);
    }

    public static PluginDescriptor fromMap(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        String name = require(properties, KEY_NAME);
        String version = require(properties, KEY_VERSION);
        String factoryClass = require(properties, KEY_FACTORY_CLASS);
        int spiVersion = parseInt(properties.get(KEY_SPI_VERSION), 1);

        Map<String, String> extra = new HashMap<>(properties);
        extra.remove(KEY_NAME);
        extra.remove(KEY_VERSION);
        extra.remove(KEY_SPI_VERSION);
        extra.remove(KEY_FACTORY_CLASS);

        return PluginDescriptor.builder()
                .name(name)
                .version(version)
                .factoryClass(factoryClass)
                .spiVersion(spiVersion)
                .properties(extra)
                .build();
    }

    private static String require(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required plugin property: " + key);
        }
        return value;
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
