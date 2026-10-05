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

package org.apache.doris.connector.hive;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal valid catalog properties for tests that need a connector, a scan provider or a listing cache
 * but are not about properties themselves. The metastore URI is the one key {@link
 * HiveCatalogProperties#of} requires, so every such test would otherwise repeat it.
 */
public final class HiveTestProperties {

    public static final String METASTORE_URI = "thrift://localhost:9083";

    private HiveTestProperties() {
    }

    /** A mutable map carrying only the required metastore URI. */
    public static Map<String, String> minimalMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(HiveCatalogProperties.HIVE_METASTORE_URIS, METASTORE_URI);
        return m;
    }

    /** The minimal map plus the given key/value pairs, given as {@code k1, v1, k2, v2, ...}. */
    public static Map<String, String> mapWith(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("expected key/value pairs, got " + keyValuePairs.length + " strings");
        }
        Map<String, String> m = minimalMap();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            m.put(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return m;
    }

    public static HiveCatalogProperties minimal() {
        return HiveCatalogProperties.of(minimalMap());
    }

    /** The minimal properties plus one extra key, for a test that turns a single knob. */
    public static HiveCatalogProperties with(String key, String value) {
        return HiveCatalogProperties.of(mapWith(key, value));
    }
}
