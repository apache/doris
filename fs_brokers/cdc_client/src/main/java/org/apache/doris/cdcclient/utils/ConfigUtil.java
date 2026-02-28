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

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

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
