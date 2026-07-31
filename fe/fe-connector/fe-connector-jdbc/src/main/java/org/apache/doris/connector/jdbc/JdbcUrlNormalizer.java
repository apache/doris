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

import java.util.Collections;
import java.util.Map;

/**
 * Normalizes JDBC URLs by adding required parameters for correct behavior.
 * Replicates the logic from {@code JdbcResource.handleJdbcUrl()} in fe-core,
 * ensuring the connector module produces identical behavior to the old path.
 */
public final class JdbcUrlNormalizer {

    private JdbcUrlNormalizer() {
    }

    /**
     * Normalize a JDBC URL by setting required parameters based on the database type.
     *
     * <p>For MySQL/OceanBase:
     * <ul>
     *   <li>{@code yearIsDateType=false} — prevent YEAR returning java.sql.Date</li>
     *   <li>{@code tinyInt1isBit=false} — prevent TINYINT(1) → Boolean conversion</li>
     *   <li>{@code useUnicode=true}</li>
     *   <li>{@code characterEncoding=utf-8}</li>
     *   <li>{@code rewriteBatchedStatements=true}</li>
     * </ul>
     *
     * <p>For OceanBase additionally:
     * <ul>
     *   <li>{@code useCursorFetch=true} — enables binary protocol for full precision</li>
     * </ul>
     *
     * <p>For PostgreSQL:
     * <ul>
     *   <li>{@code reWriteBatchedInserts=true}</li>
     * </ul>
     *
     * <p>For SQL Server:
     * <ul>
     *   <li>{@code useBulkCopyForBatchInsert=true}</li>
     *   <li>{@code encrypt=false} — when {@code force_sqlserver_jdbc_encrypt_false} is set</li>
     * </ul>
     */
    public static String normalize(String jdbcUrl, JdbcDbType dbType) {
        return normalize(jdbcUrl, dbType, Collections.emptyMap());
    }

    /**
     * Normalize a JDBC URL with engine environment context.
     *
     * @param jdbcUrl the raw JDBC URL
     * @param dbType  the database type
     * @param environment engine environment properties (from ConnectorContext)
     */
    public static String normalize(String jdbcUrl, JdbcDbType dbType, Map<String, String> environment) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return jdbcUrl;
        }
        String url = jdbcUrl.replaceAll(" ", "");

        switch (dbType) {
            case MYSQL:
                url = setParam(url, dbType, "yearIsDateType", "true", "false");
                url = setParam(url, dbType, "tinyInt1isBit", "true", "false");
                url = setParam(url, dbType, "useUnicode", "false", "true");
                url = setParamIfAbsent(url, dbType, "characterEncoding", "utf-8");
                url = setParam(url, dbType, "rewriteBatchedStatements", "false", "true");
                break;
            case OCEANBASE:
                url = setParam(url, dbType, "yearIsDateType", "true", "false");
                url = setParam(url, dbType, "tinyInt1isBit", "true", "false");
                url = setParam(url, dbType, "useUnicode", "false", "true");
                url = setParamIfAbsent(url, dbType, "characterEncoding", "utf-8");
                url = setParam(url, dbType, "rewriteBatchedStatements", "false", "true");
                url = setParam(url, dbType, "useCursorFetch", "false", "true");
                break;
            case POSTGRESQL:
                url = setParam(url, dbType, "reWriteBatchedInserts", "false", "true");
                break;
            case SQLSERVER:
                if ("true".equalsIgnoreCase(environment.getOrDefault(
                        "force_sqlserver_jdbc_encrypt_false", "false"))) {
                    url = setParam(url, dbType, "encrypt", "true", "false");
                }
                url = setParam(url, dbType, "useBulkCopyForBatchInsert", "false", "true");
                break;
            default:
                break;
        }
        return url;
    }

    /**
     * Force a boolean parameter to the expected value, replacing any unexpected value.
     */
    private static String setParam(String url, JdbcDbType dbType,
            String param, String unexpectedVal, String expectedVal) {
        String expected = param + "=" + expectedVal;
        String unexpected = param + "=" + unexpectedVal;

        if (url.contains(expected)) {
            return url;
        } else if (url.contains(unexpected)) {
            return url.replace(unexpected, expected);
        }
        return appendParam(url, dbType, expected);
    }

    /**
     * Set a parameter only if no parameter with the same name is already present.
     */
    private static String setParamIfAbsent(String url, JdbcDbType dbType,
            String param, String expectedVal) {
        String paramPrefix = param + "=";
        if (url.contains(paramPrefix)) {
            return url;
        }
        return appendParam(url, dbType, paramPrefix + expectedVal);
    }

    private static String appendParam(String url, JdbcDbType dbType, String paramValue) {
        String delimiter = getDelimiter(url, dbType);
        if (!url.endsWith(delimiter)) {
            url += delimiter;
        }
        return url + paramValue;
    }

    private static String getDelimiter(String url, JdbcDbType dbType) {
        if (dbType == JdbcDbType.SQLSERVER || dbType == JdbcDbType.DB2) {
            return ";";
        } else if (url.contains("?")) {
            return "&";
        } else {
            return "?";
        }
    }
}
