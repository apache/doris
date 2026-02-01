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

package org.apache.doris.extension.spi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable plugin metadata descriptor.
 */
public final class PluginDescriptor {

    private final String name;
    private final String version;
    private final int spiVersion;
    private final String factoryClass;
    private final Map<String, String> properties;

    private PluginDescriptor(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name is required");
        this.version = Objects.requireNonNull(builder.version, "version is required");
        this.spiVersion = builder.spiVersion;
        this.factoryClass = Objects.requireNonNull(builder.factoryClass, "factoryClass is required");
        this.properties = Collections.unmodifiableMap(new HashMap<>(builder.properties));
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public int getSpiVersion() {
        return spiVersion;
    }

    public String getFactoryClass() {
        return factoryClass;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link PluginDescriptor}.
     */
    public static final class Builder {
        private String name;
        private String version;
        private int spiVersion = 1;
        private String factoryClass;
        private Map<String, String> properties = new HashMap<>();

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder spiVersion(int spiVersion) {
            this.spiVersion = spiVersion;
            return this;
        }

        public Builder factoryClass(String factoryClass) {
            this.factoryClass = factoryClass;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties != null ? properties : new HashMap<>();
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public PluginDescriptor build() {
            return new PluginDescriptor(this);
        }
    }
}
