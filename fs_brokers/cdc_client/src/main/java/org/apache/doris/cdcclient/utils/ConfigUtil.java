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

package org.apache.doris.cdcclient.utils;

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.mysql.cj.conf.ConnectionUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    public static String getServerId(long jobId) {
        return String.valueOf(Math.abs(String.valueOf(jobId).hashCode()));
    }

    public static ZoneId getServerTimeZoneFromJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return ZoneId.systemDefault();
        }
        if (jdbcUrl.startsWith("jdbc:mysql://") || jdbcUrl.startsWith("jdbc:mariadb://")) {
            return getServerTimeZone(jdbcUrl);
        } else if (jdbcUrl.startsWith("jdbc:postgresql://")) {
            return getPostgresServerTimeZone(jdbcUrl);
        }
        return ZoneId.systemDefault();
    }

    private static ZoneId getServerTimeZone(String jdbcUrl) {
        Preconditions.checkNotNull(jdbcUrl, "jdbcUrl is null");
        ConnectionUrl cu = ConnectionUrl.getConnectionUrlInstance(jdbcUrl, null);
        return getTimeZoneFromProps(cu.getOriginalProperties());
    }

    public static ZoneId getTimeZoneFromProps(Map<String, String> originalProperties) {
        if (originalProperties != null && originalProperties.containsKey("serverTimezone")) {
            String timeZone = originalProperties.get("serverTimezone");
            if (StringUtils.isNotEmpty(timeZone)) {
                return ZoneId.of(timeZone);
            }
        }
        return ZoneId.systemDefault();
    }

    public static ZoneId getPostgresServerTimeZone(String jdbcUrl) {
        Preconditions.checkNotNull(jdbcUrl, "jdbcUrl is null");
        try {
            java.util.Properties props = org.postgresql.Driver.parseURL(jdbcUrl, null);
            if (props != null && props.containsKey("timezone")) {
                String timeZone = props.getProperty("timezone");
                if (StringUtils.isNotEmpty(timeZone)) {
                    return ZoneId.of(timeZone);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse Postgres JDBC URL for timezone: {}", jdbcUrl);
        }
        return ZoneId.systemDefault();
    }

    public static ZoneId getPostgresServerTimeZoneFromProps(java.util.Properties props) {
        if (props != null && props.containsKey("timezone")) {
            String timeZone = props.getProperty("timezone");
            if (StringUtils.isNotEmpty(timeZone)) {
                return ZoneId.of(timeZone);
            }
        }
        return ZoneId.systemDefault();
    }

    /** Optimized debezium parameters */
    public static Properties getDefaultDebeziumProps() {
        Properties properties = new Properties();
        return properties;
    }

    public static boolean is13Timestamp(String s) {
        return s != null && s.matches("\\d{13}");
    }

    public static boolean isJson(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        try {
            JsonNode node = objectMapper.readTree(str);
            return node.isObject();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parse the exclude-column set for a specific table from config.
     *
     * <p>Looks for key {@code "table.<tableName>.exclude_columns"} whose value is a comma-separated
     * column list, e.g. {@code "secret,internal_note"}.
     *
     * @return column name set (original case preserved); empty set when the key is absent
     */
    public static Set<String> parseExcludeColumns(Map<String, String> config, String tableName) {
        String key =
                DataSourceConfigKeys.TABLE
                        + "."
                        + tableName
                        + "."
                        + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX;
        String value = config.get(key);
        if (StringUtils.isEmpty(value)) {
            return Collections.emptySet();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Parse all per-table exclude-column sets from config at once.
     *
     * <p>Scans all keys matching {@code "table.<tableName>.exclude_columns"} and returns a map from
     * table name to its excluded column set. Intended to be called once during initialization.
     */
    public static Map<String, Set<String>> parseAllExcludeColumns(Map<String, String> config) {
        String prefix = DataSourceConfigKeys.TABLE + ".";
        String suffix = "." + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX;
        Map<String, Set<String>> result = new HashMap<>();
        for (String key : config.keySet()) {
            if (key.startsWith(prefix) && key.endsWith(suffix)) {
                String tableName = key.substring(prefix.length(), key.length() - suffix.length());
                if (!tableName.isEmpty()) {
                    result.put(tableName, parseExcludeColumns(config, tableName));
                }
            }
        }
        return result;
    }

    /**
     * Parse all target-table name mappings from config.
     *
     * <p>Scans all keys matching {@code "table.<srcTableName>.target_table"} and returns a map from
     * source table name to target (Doris) table name. Tables without a mapping are NOT included;
     * callers should use {@code getOrDefault(srcTable, srcTable)}.
     */
    public static Map<String, String> parseAllTargetTableMappings(Map<String, String> config) {
        String prefix = DataSourceConfigKeys.TABLE + ".";
        String suffix = "." + DataSourceConfigKeys.TABLE_TARGET_TABLE_SUFFIX;
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix) && key.endsWith(suffix)) {
                String srcTable = key.substring(prefix.length(), key.length() - suffix.length());
                String rawValue = entry.getValue();
                String dstTable = rawValue != null ? rawValue.trim() : "";
                if (!srcTable.isEmpty() && !dstTable.isEmpty()) {
                    result.put(srcTable, dstTable);
                }
            }
        }
        return result;
    }

    public static Map<String, String> toStringMap(String json) {
        if (!isJson(json)) {
            return null;
        }

        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
