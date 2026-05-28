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

package org.apache.doris.jdbc;

import com.zaxxer.hikari.HikariDataSource;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Snowflake JDBC accepts OAuth credentials through the "token" connection
 * property. Doris keeps password authentication on jdbc_password, and lets
 * Snowflake OAuth callers provide a separate access token when available.
 */
public final class SnowflakeJdbcAuthUtils {

    public static final String OAUTH_ACCESS_TOKEN_PARAM = "snowflake.oauth.access_token";

    private SnowflakeJdbcAuthUtils() {
    }

    public static void configure(HikariDataSource dataSource, String jdbcUrl, String jdbcPassword) {
        configure(dataSource, jdbcUrl, jdbcPassword, "");
    }

    public static void configure(HikariDataSource dataSource, String jdbcUrl, String jdbcPassword,
            String oauthAccessToken) {
        String credential = resolveCredential(jdbcUrl, jdbcPassword, oauthAccessToken);
        if (dataSource == null || !hasText(credential) || !isSnowflakeOauthUrl(jdbcUrl)) {
            return;
        }
        dataSource.addDataSourceProperty("token", credential);
    }

    static String resolveCredential(String jdbcUrl, String jdbcPassword, String oauthAccessToken) {
        if (!isSnowflakeOauthUrl(jdbcUrl)) {
            return jdbcPassword;
        }
        if (hasText(oauthAccessToken)) {
            return oauthAccessToken;
        }
        return jdbcPassword;
    }

    static boolean isSnowflakeOauthUrl(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:snowflake:")) {
            return false;
        }
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == jdbcUrl.length() - 1) {
            return false;
        }
        String query = jdbcUrl.substring(queryStart + 1);
        for (String part : query.split("&")) {
            int equal = part.indexOf('=');
            String rawName = equal >= 0 ? part.substring(0, equal) : part;
            String rawValue = equal >= 0 ? part.substring(equal + 1) : "";
            String name = URLDecoder.decode(rawName, StandardCharsets.UTF_8).trim();
            String value = URLDecoder.decode(rawValue, StandardCharsets.UTF_8).trim();
            if ("authenticator".equalsIgnoreCase(name) && "oauth".equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
