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

import java.util.Map;

/**
 * Property constants for Hudi connector configuration.
 * Mirrors keys from fe-core's Hudi/HMS property classes without taking
 * a compile-time dependency on fe-core.
 */
public final class HudiConnectorProperties {

    private HudiConnectorProperties() {
    }

    // -- HMS connection --
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    // -- HMS client pool --
    public static final String HMS_CLIENT_POOL_SIZE = "hive.metastore.client.pool.size";
    public static final int DEFAULT_HMS_CLIENT_POOL_SIZE = 8;

    // -- Hudi specific --
    public static final String HUDI_TABLE_TYPE = "hoodie.datasource.write.table.type";

    /**
     * Parse an integer property with a default value.
     */
    public static int getInt(Map<String, String> props, String key, int defaultVal) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
}
