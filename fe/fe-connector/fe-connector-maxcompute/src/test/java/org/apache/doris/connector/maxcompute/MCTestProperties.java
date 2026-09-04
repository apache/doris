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

package org.apache.doris.connector.maxcompute;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The smallest property map {@link MCCatalogProperties#of} accepts, for tests that need a connector but
 * are not about properties: project, endpoint and the AK/SK pair the default auth type requires.
 */
final class MCTestProperties {

    static final String PROJECT = "my_project";
    static final String ENDPOINT = "http://service.cn-beijing.maxcompute.aliyun-inc.com/api";

    private MCTestProperties() {
    }

    /** A mutable map carrying only what of() requires. */
    static Map<String, String> minimalMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(MCCatalogProperties.PROJECT, PROJECT);
        m.put(MCCatalogProperties.ENDPOINT, ENDPOINT);
        m.put(MCCatalogProperties.ACCESS_KEY, "ak");
        m.put(MCCatalogProperties.SECRET_KEY, "sk");
        return m;
    }

    static MCCatalogProperties minimal() {
        return MCCatalogProperties.of(minimalMap());
    }

    /** The minimal properties plus one extra key, for a test that turns a single knob. */
    static MCCatalogProperties with(String key, String value) {
        Map<String, String> m = minimalMap();
        m.put(key, value);
        return MCCatalogProperties.of(m);
    }
}
