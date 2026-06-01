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
            DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL,
            DataSourceConfigKeys.SSL_MODE,
            DataSourceConfigKeys.SSL_ROOTCERT,
            DataSourceConfigKeys.SLOT_NAME,
            DataSourceConfigKeys.PUBLICATION_NAME,
            DataSourceConfigKeys.SERVER_ID
    );

    private static final Set<String> ALLOW_SSL_MODES = Sets.newHashSet(
            DataSourceConfigKeys.SSL_MODE_DISABLE,
            DataSourceConfigKeys.SSL_MODE_REQUIRE,
            DataSourceConfigKeys.SSL_MODE_VERIFY_CA
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

        validateSslVerifyCaPair(input);
    }

    // Cross-field: verify-ca must be paired with a CA cert; otherwise the reader will
    // silently fall back to the JVM default truststore and likely fail to connect.
    public static void validateSslVerifyCaPair(Map<String, String> input) throws IllegalArgumentException {
        if (DataSourceConfigKeys.SSL_MODE_VERIFY_CA.equals(input.get(DataSourceConfigKeys.SSL_MODE))
                && (input.get(DataSourceConfigKeys.SSL_ROOTCERT) == null
                        || input.get(DataSourceConfigKeys.SSL_ROOTCERT).trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "ssl_mode '" + DataSourceConfigKeys.SSL_MODE_VERIFY_CA
                            + "' requires ssl_rootcert to be set");
        }

        validateServerIdConfig(input);
    }

    // Shared by validateSource and the cdc_stream TVF entrypoint so both reject malformed
    // server_id at SQL-analysis time, not as a cdc_client runtime error.
    public static void validateServerIdConfig(Map<String, String> input)
            throws IllegalArgumentException {
        String serverIdValue = input.get(DataSourceConfigKeys.SERVER_ID);
        if (serverIdValue == null) {
            return;
        }
        int[] range = parseServerIdRange(serverIdValue);
        if (range == null) {
            throw new IllegalArgumentException(
                    "Invalid value for key '" + DataSourceConfigKeys.SERVER_ID + "': "
                            + serverIdValue
                            + ". Expected a single value (e.g. '5400') or range (e.g. '5400-5408')"
                            + " with start >= 1 and start <= end.");
        }
        String parallelismValue = input.getOrDefault(
                DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT);
        Integer parallelism = parsePositiveInt(parallelismValue);
        if (parallelism == null) {
            throw new IllegalArgumentException(
                    "Invalid value for key '" + DataSourceConfigKeys.SNAPSHOT_PARALLELISM
                            + "': " + parallelismValue + ". Expected a positive integer.");
        }
        int width = range[1] - range[0] + 1;
        // Range must cover every parallel SnapshotSplitReader; cdc_client throws otherwise.
        if (width < parallelism) {
            throw new IllegalArgumentException(
                    "server_id range size " + width
                            + " must be >= snapshot_parallelism " + parallelism
                            + ". Widen the range (e.g. '" + range[0] + "-"
                            + (range[0] + parallelism - 1)
                            + "') or reduce parallelism.");
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

        if (key.equals(DataSourceConfigKeys.SLOT_NAME)
                || key.equals(DataSourceConfigKeys.PUBLICATION_NAME)) {
            return isValidPgIdentifier(value);
        }
        if (key.equals(DataSourceConfigKeys.SSL_MODE)) {
            return isValidSslMode(value);
        }
        if (key.equals(DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE)
                || key.equals(DataSourceConfigKeys.SNAPSHOT_PARALLELISM)) {
            return isPositiveInt(value);
        }
        if (key.equals(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL)) {
            return isValidBoolean(value);
        }
        if (key.equals(DataSourceConfigKeys.SERVER_ID)) {
            return parseServerIdRange(value) != null;
        }
        return true;
    }

    // Strict boolean: only "true"/"false" (case-insensitive); Boolean.parseBoolean would
    // silently coerce typos like "yes" to false.
    public static boolean isValidBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
    }

    public static boolean isPositiveInt(String value) {
        if (value == null) {
            return false;
        }
        try {
            return Integer.parseInt(value) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // slot_name / publication_name are interpolated into PG DDL without quoting,
    // so enforce unquoted-identifier grammar to prevent injection and runtime errors.
    public static boolean isValidPgIdentifier(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= PG_MAX_IDENTIFIER_LENGTH
                && PG_IDENTIFIER_PATTERN.matcher(value).matches();
    }

    public static boolean isValidSslMode(String value) {
        return ALLOW_SSL_MODES.contains(value);
    }

    // Parse "5400" or "5400-5408" into {start, end} inclusive; null on any malformed input.
    // Lower bound is 1 because MySQL server_id=0 disables replication.
    static int[] parseServerIdRange(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        try {
            int start;
            int end;
            int dash = trimmed.indexOf('-');
            if (dash < 0) {
                start = end = Integer.parseInt(trimmed);
            } else {
                String left = trimmed.substring(0, dash).trim();
                String right = trimmed.substring(dash + 1).trim();
                if (left.isEmpty() || right.isEmpty()) {
                    return null;
                }
                start = Integer.parseInt(left);
                end = Integer.parseInt(right);
            }
            if (start < 1 || start > end) {
                return null;
            }
            return new int[] {start, end};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer parsePositiveInt(String value) {
        try {
            int n = Integer.parseInt(value.trim());
            return n >= 1 ? n : null;
        } catch (NumberFormatException e) {
            return null;
        }
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
