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

package org.apache.doris.connector.jdbc;

import java.util.Map;

/**
 * Property constants for JDBC connector configuration.
 * Mirrors keys from fe-core's {@code JdbcResource} and {@code CatalogProperty}
 * without taking a compile-time dependency on fe-core.
 */
public final class JdbcConnectorProperties {

    private JdbcConnectorProperties() {
    }

    // -- connection --
    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String DRIVER_URL = "driver_url";
    public static final String TYPE = "type";

    // -- connection pool --
    public static final String CONNECTION_POOL_MIN_SIZE = "connection_pool_min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    public static final String CONNECTION_POOL_MAX_WAIT_TIME = "connection_pool_max_wait_time";
    public static final String CONNECTION_POOL_MAX_LIFE_TIME = "connection_pool_max_life_time";
    public static final String CONNECTION_POOL_KEEP_ALIVE = "connection_pool_keep_alive";

    // -- defaults --
    public static final int DEFAULT_POOL_MIN_SIZE = 1;
    public static final int DEFAULT_POOL_MAX_SIZE = 30;
    public static final int DEFAULT_POOL_MAX_WAIT_TIME = 5000;
    public static final int DEFAULT_POOL_MAX_LIFE_TIME = 1800000;
    public static final boolean DEFAULT_POOL_KEEP_ALIVE = false;

    // -- metadata filtering --
    public static final String ONLY_SPECIFIED_DATABASE = "only_specified_database";
    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";

    // -- connectivity test --
    public static final String TEST_CONNECTION = "test_connection";

    // -- driver --
    public static final String DRIVER_CHECKSUM = "checksum";

    // -- type mapping options --
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    // -- function pushdown --
    public static final String FUNCTION_RULES = "function_rules";

    // -- identifier mapping --
    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

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
