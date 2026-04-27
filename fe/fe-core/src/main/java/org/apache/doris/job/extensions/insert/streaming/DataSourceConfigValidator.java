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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class DataSourceConfigValidator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // PostgreSQL unquoted identifier: lowercase letters, digits, underscores, not starting with a digit.
    private static final Pattern PG_IDENTIFIER_PATTERN = Pattern.compile("^[a-z_][a-z0-9_]*$");
    private static final int PG_MAX_IDENTIFIER_LENGTH = 63;

    private static final Set<String> ALLOW_SOURCE_KEYS = Sets.newHashSet(
            DataSourceConfigKeys.JDBC_URL,
            DataSourceConfigKeys.USER,
            DataSourceConfigKeys.PASSWORD,
            DataSourceConfigKeys.OFFSET,
            DataSourceConfigKeys.DRIVER_URL,
            DataSourceConfigKeys.DRIVER_CLASS,
            DataSourceConfigKeys.DATABASE,
            DataSourceConfigKeys.SCHEMA,
            DataSourceConfigKeys.INCLUDE_TABLES,
            DataSourceConfigKeys.EXCLUDE_TABLES,
            DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE,
            DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
            DataSourceConfigKeys.SSL_MODE,
            DataSourceConfigKeys.SSL_ROOTCERT,
            DataSourceConfigKeys.SLOT_NAME,
            DataSourceConfigKeys.PUBLICATION_NAME
    );

    // Known suffixes for per-table config keys (format: "table.<tableName>.<suffix>")
    private static final Set<String> ALLOW_TABLE_LEVEL_SUFFIXES = Sets.newHashSet(
            DataSourceConfigKeys.TABLE_TARGET_TABLE_SUFFIX,
            DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX
    );

    private static final Set<String> ALLOW_LOAD_KEYS = ImmutableSortedSet.of(
            DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.MAX_FILTER_RATIO_PROPERTY,
            DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.STRICT_MODE
    );

    private static final String TABLE_LEVEL_PREFIX = DataSourceConfigKeys.TABLE + ".";

    public static void validateSource(Map<String, String> input,
            String dataSourceType) throws IllegalArgumentException {
        for (Map.Entry<String, String> entry : input.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith(TABLE_LEVEL_PREFIX)) {
                // per-table config key must be exactly: table.<tableName>.<suffix>
                // reject malformed keys like "table.exclude_columns" (missing tableName)
                String[] parts = key.split("\\.", -1);
                if (parts.length != 3 || parts[1].isEmpty()) {
                    throw new IllegalArgumentException("Malformed per-table config key: '" + key
                            + "'. Expected format: table.<tableName>.<suffix>");
                }
                String suffix = parts[parts.length - 1];
                if (!ALLOW_TABLE_LEVEL_SUFFIXES.contains(suffix)) {
                    throw new IllegalArgumentException("Unknown per-table config key: '" + key + "'");
                }
                if (value == null || value.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Value for per-table config key '" + key + "' must not be empty");
                }
                continue;
            }

            if (!ALLOW_SOURCE_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unexpected key: '" + key + "'");
            }

            if (!isValidValue(key, value, dataSourceType)) {
                throw new IllegalArgumentException("Invalid value for key '" + key + "': " + value);
            }
        }
    }

    public static void validateTarget(Map<String, String> input) throws IllegalArgumentException {
        for (Map.Entry<String, String> entry : input.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(DataSourceConfigKeys.TABLE_PROPS_PREFIX)) {
                continue;
            }
            if (key.startsWith(DataSourceConfigKeys.LOAD_PROPERTIES)) {
                if (!ALLOW_LOAD_KEYS.contains(key)) {
                    throw new IllegalArgumentException("Unsupported load property: '" + key
                            + "'. Supported keys: " + ALLOW_LOAD_KEYS);
                }
                if (key.equals(DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.MAX_FILTER_RATIO_PROPERTY)) {
                    try {
                        Double.parseDouble(entry.getValue());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid value for key '" + key + "': " + entry.getValue());
                    }
                }
                continue;
            }
            throw new IllegalArgumentException("Not support target properties key " + key);
        }
    }

    private static boolean isValidValue(String key, String value, String dataSourceType) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (key.equals(DataSourceConfigKeys.OFFSET)) {
            return isValidOffset(value, dataSourceType);
        }

        // slot_name / publication_name are interpolated into PG DDL without quoting,
        // so enforce unquoted-identifier grammar to prevent injection and runtime errors.
        if (key.equals(DataSourceConfigKeys.SLOT_NAME)
                || key.equals(DataSourceConfigKeys.PUBLICATION_NAME)) {
            return value.length() <= PG_MAX_IDENTIFIER_LENGTH
                    && PG_IDENTIFIER_PATTERN.matcher(value).matches();
        }
        return true;
    }

    /**
     * Check if the offset value is valid for the given data source type.
     * Supported: initial, snapshot, latest, JSON binlog/lsn position.
     * earliest is only supported for MySQL.
     */
    public static boolean isValidOffset(String offset, String dataSourceType) {
        if (offset == null || offset.isEmpty()) {
            return false;
        }
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(offset)
                || DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(offset)
                || DataSourceConfigKeys.OFFSET_SNAPSHOT.equalsIgnoreCase(offset)) {
            return true;
        }
        // earliest only for MySQL
        if (DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(offset)) {
            return DataSourceType.MYSQL.name().equalsIgnoreCase(dataSourceType);
        }
        if (isJsonOffset(offset)) {
            return true;
        }
        return false;
    }

    public static boolean isJsonOffset(String offset) {
        if (offset == null || offset.trim().isEmpty()) {
            return false;
        }
        try {
            JsonNode node = OBJECT_MAPPER.readTree(offset);
            return node.isObject();
        } catch (Exception e) {
            return false;
        }
    }

}
