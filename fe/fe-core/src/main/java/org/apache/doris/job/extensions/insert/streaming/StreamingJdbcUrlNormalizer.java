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

import org.apache.doris.job.common.DataSourceType;

/**
 * Normalizes JDBC URLs before streaming ingestion uses them for metadata discovery and CDC reads.
 * Database-specific rules are kept here so every streaming entry point applies the same read-side
 * semantics while leaving unrelated JDBC Catalog write optimizations out of scope.
 */
public final class StreamingJdbcUrlNormalizer {

    private StreamingJdbcUrlNormalizer() {
    }

    public static String normalize(DataSourceType sourceType, String jdbcUrl) {
        switch (sourceType) {
            case MYSQL:
                return normalizeMysql(jdbcUrl);
            case POSTGRES:
                return jdbcUrl;
            default:
                throw new IllegalArgumentException("Unsupported data source type: " + sourceType);
        }
    }

    private static String normalizeMysql(String jdbcUrl) {
        String normalizedUrl = jdbcUrl.replace(" ", "");
        normalizedUrl = forceBooleanParam(
                normalizedUrl, "yearIsDateType", "true", "false");
        normalizedUrl = forceBooleanParam(
                normalizedUrl, "tinyInt1isBit", "true", "false");
        normalizedUrl = forceBooleanParam(
                normalizedUrl, "useUnicode", "false", "true");
        return setParam(normalizedUrl, "characterEncoding", "utf-8");
    }

    private static String forceBooleanParam(String jdbcUrl, String param,
            String unexpectedValue, String expectedValue) {
        String unexpectedParam = param + "=" + unexpectedValue;
        String expectedParam = param + "=" + expectedValue;
        if (jdbcUrl.contains(expectedParam)) {
            return jdbcUrl;
        }
        if (jdbcUrl.contains(unexpectedParam)) {
            return jdbcUrl.replace(unexpectedParam, expectedParam);
        }
        return appendParam(jdbcUrl, expectedParam);
    }

    private static String setParam(String jdbcUrl, String param, String value) {
        String expectedParam = param + "=" + value;
        if (jdbcUrl.contains(expectedParam)) {
            return jdbcUrl;
        }
        return appendParam(jdbcUrl, expectedParam);
    }

    private static String appendParam(String jdbcUrl, String param) {
        String delimiter = jdbcUrl.contains("?") ? "&" : "?";
        if (!jdbcUrl.endsWith(delimiter)) {
            jdbcUrl += delimiter;
        }
        return jdbcUrl + param;
    }
}
