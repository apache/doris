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

package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Thin typed-property model for broker storage. Broker has no provider-side parsing or
 * validation — fe-core's BrokerProperties is itself a raw-map holder whose broker params are
 * the {@code broker.}-prefixed keys with the prefix stripped. This class mirrors exactly that
 * so the registry's bindPrimary/bindAll can construct every storage type uniformly.
 */
public final class BrokerFileSystemProperties implements FileSystemProperties {

    private static final String BROKER_PREFIX = "broker.";

    private final Map<String, String> rawProperties;

    BrokerFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
    }

    @Override
    public String providerName() {
        return "BROKER";
    }

    @Override
    public StorageKind kind() {
        return StorageKind.BROKER;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.BROKER;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return Collections.emptyMap();
    }

    /** The broker name, from the case-insensitive {@code broker.name} key. */
    public String getBrokerName() {
        for (Map.Entry<String, String> entry : rawProperties.entrySet()) {
            if ("broker.name".equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** fe-core parity: {@code broker.}-prefixed keys with the prefix stripped (incl. name). */
    public Map<String, String> getBrokerParams() {
        Map<String, String> params = new HashMap<>();
        for (Map.Entry<String, String> entry : rawProperties.entrySet()) {
            if (entry.getKey().startsWith(BROKER_PREFIX)) {
                params.put(entry.getKey().substring(BROKER_PREFIX.length()), entry.getValue());
            }
        }
        return params;
    }
}
