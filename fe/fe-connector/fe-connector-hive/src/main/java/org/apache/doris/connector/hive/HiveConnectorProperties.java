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

import java.util.Map;

/**
 * Property constants for Hive connector configuration.
 * Mirrors keys from fe-core's HMS property classes without taking
 * a compile-time dependency on fe-core.
 */
public final class HiveConnectorProperties {

    private HiveConnectorProperties() {
    }

    // -- HMS connection --
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";

    // -- HMS client pool --
    public static final String HMS_CLIENT_POOL_SIZE = "hive.metastore.client.pool.size";
    public static final int DEFAULT_HMS_CLIENT_POOL_SIZE = 8;

    // -- authentication --
    public static final String AUTH_TYPE = "hive.metastore.authentication.type";
    public static final String SERVICE_PRINCIPAL = "hive.metastore.service.principal";
    public static final String CLIENT_PRINCIPAL = "hive.metastore.client.principal";
    public static final String CLIENT_KEYTAB = "hive.metastore.client.keytab";

    // -- table format detection --
    public static final String TABLE_TYPE_PARAM = "table_type";
    public static final String SPARK_TABLE_PROVIDER = "spark.sql.sources.provider";
    public static final String FLINK_CONNECTOR = "connector";

    // -- type mapping options --
    public static final String ENABLE_MAPPING_BINARY_AS_STRING = "enable_mapping_binary_as_string";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable_mapping_timestamp_tz";

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
