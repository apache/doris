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

package org.apache.doris.connector.hudi;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal valid {@link HudiCatalogProperties} for tests that need a connector or a metadata object but
 * are not about properties. The metastore URI is the one key {@code of()} requires, so every such test
 * would otherwise repeat it.
 */
final class HudiTestProperties {

    static final String METASTORE_URI = "thrift://localhost:9083";

    private HudiTestProperties() {
    }

    /** A mutable map carrying only the required metastore URI. */
    static Map<String, String> minimalMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(HudiCatalogProperties.HIVE_METASTORE_URIS, METASTORE_URI);
        return m;
    }

    static HudiCatalogProperties minimal() {
        return HudiCatalogProperties.of(minimalMap());
    }

    /** The minimal properties plus one extra key, for a test that turns a single knob. */
    static HudiCatalogProperties with(String key, String value) {
        Map<String, String> m = minimalMap();
        m.put(key, value);
        return HudiCatalogProperties.of(m);
    }
}
