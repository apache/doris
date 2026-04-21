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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SPI entry point for the JDBC connector plugin.
 */
public class JdbcConnectorProvider implements ConnectorProvider {

    private static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
            JdbcConnectorProperties.JDBC_URL,
            JdbcConnectorProperties.DRIVER_URL,
            JdbcConnectorProperties.DRIVER_CLASS
    );

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new JdbcDorisConnector(properties, context);
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        // 1. Required properties
        for (String required : REQUIRED_PROPERTIES) {
            String value = resolve(properties, required);
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(
                        "Required property '" + required + "' is missing");
            }
        }

        // 2. Reject deprecated lower_case_table_names
        if (properties.containsKey(JdbcConnectorProperties.LOWER_CASE_TABLE_NAMES)
                || properties.containsKey(
                        JdbcDorisConnector.JDBC_PROPERTIES_PREFIX
                                + JdbcConnectorProperties.LOWER_CASE_TABLE_NAMES)) {
            throw new IllegalArgumentException(
                    "Jdbc catalog property lower_case_table_names is not supported,"
                            + " please use lower_case_meta_names instead");
        }

        // 3. Boolean property validation
        checkBooleanProperty(properties, JdbcConnectorProperties.ONLY_SPECIFIED_DATABASE);
        checkBooleanProperty(properties, JdbcConnectorProperties.LOWER_CASE_META_NAMES);
        checkBooleanProperty(properties, JdbcConnectorProperties.CONNECTION_POOL_KEEP_ALIVE);
        checkBooleanProperty(properties, JdbcConnectorProperties.TEST_CONNECTION);

        // 4. Database list consistency: include/exclude cannot be set when only_specified_database=false
        String onlySpecified = resolve(properties, JdbcConnectorProperties.ONLY_SPECIFIED_DATABASE);
        if (onlySpecified == null || !onlySpecified.equalsIgnoreCase("true")) {
            String includeList = resolve(properties, JdbcConnectorProperties.INCLUDE_DATABASE_LIST);
            String excludeList = resolve(properties, JdbcConnectorProperties.EXCLUDE_DATABASE_LIST);
            if ((includeList != null && !includeList.isEmpty())
                    || (excludeList != null && !excludeList.isEmpty())) {
                throw new IllegalArgumentException(
                        "include_database_list and exclude_database_list "
                                + "cannot be set when only_specified_database is false");
            }
        }

        // 5. Connection pool settings
        checkConnectionPoolProperties(properties);

        // 6. Validate meta_names_mapping with actual lower_case_meta_names setting.
        // At runtime, JdbcConnectorMetadata builds the mapper with lower_case_meta_names
        // from catalog properties and lower_case_table_names from session. We validate
        // with the real lower_case_meta_names and both possible lower_case_table_names
        // states to catch mapping collisions that only appear after lowercasing.
        String metaNamesMapping = resolve(properties, JdbcConnectorProperties.META_NAMES_MAPPING);
        if (metaNamesMapping != null && !metaNamesMapping.isEmpty()) {
            boolean isLowerCaseMetaNames = Boolean.parseBoolean(
                    resolve(properties, JdbcConnectorProperties.LOWER_CASE_META_NAMES));
            try {
                // Validate with lower_case_table_names=false
                new JdbcIdentifierMapper(false, isLowerCaseMetaNames, metaNamesMapping);
                // Also validate with lower_case_table_names=true since the session
                // variable may enable it at runtime
                new JdbcIdentifierMapper(true, isLowerCaseMetaNames, metaNamesMapping);
            } catch (DorisConnectorException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }

    private static void checkBooleanProperty(Map<String, String> properties, String key) {
        String value = resolve(properties, key);
        if (value != null && !value.isEmpty()
                && !value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException(key + " must be true or false");
        }
    }

    private static void checkConnectionPoolProperties(Map<String, String> properties) {
        int minSize = resolveInt(properties, JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE);
        int maxSize = resolveInt(properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE);
        int maxWaitTime = resolveInt(properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME);
        int maxLifeTime = resolveInt(properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME);

        if (minSize < 0) {
            throw new IllegalArgumentException(
                    "connection_pool_min_size must be greater than or equal to 0");
        }
        if (maxSize < 1) {
            throw new IllegalArgumentException(
                    "connection_pool_max_size must be greater than or equal to 1");
        }
        if (maxSize < minSize) {
            throw new IllegalArgumentException(
                    "connection_pool_max_size must be greater than or equal to connection_pool_min_size");
        }
        if (maxWaitTime < 0) {
            throw new IllegalArgumentException(
                    "connection_pool_max_wait_time must be greater than or equal to 0");
        }
        if (maxWaitTime > 30000) {
            throw new IllegalArgumentException(
                    "connection_pool_max_wait_time must be less than or equal to 30000");
        }
        if (maxLifeTime < 150000) {
            throw new IllegalArgumentException(
                    "connection_pool_max_life_time must be greater than or equal to 150000");
        }
    }

    /**
     * Resolve a property value, checking both the short key and the "jdbc."-prefixed key.
     */
    private static String resolve(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null) {
            value = properties.get(JdbcDorisConnector.JDBC_PROPERTIES_PREFIX + key);
        }
        return value;
    }

    private static int resolveInt(Map<String, String> properties, String key, int defaultVal) {
        String value = resolve(properties, key);
        if (value == null || value.isEmpty()) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Property '" + key + "' must be a valid integer, got: " + value);
        }
    }
}
