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

import java.util.HashSet;
import java.util.Set;

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
        Set<String> params = getParams(normalizedUrl);
        StringBuilder result = new StringBuilder(normalizedUrl);
        setDefaultParam(result, params, "yearIsDateType", "false");
        setDefaultParam(result, params, "tinyInt1isBit", "false");
        setDefaultParam(result, params, "useUnicode", "true");
        setDefaultParam(result, params, "characterEncoding", "utf-8");
        return result.toString();
    }

    private static void setDefaultParam(StringBuilder jdbcUrl, Set<String> params, String param, String value) {
        if (params.contains(param)) {
            return;
        }
        char lastChar = jdbcUrl.charAt(jdbcUrl.length() - 1);
        if (lastChar != '?' && lastChar != '&') {
            jdbcUrl.append(jdbcUrl.indexOf("?") < 0 ? '?' : '&');
        }
        jdbcUrl.append(param).append('=').append(value);
    }

    private static Set<String> getParams(String jdbcUrl) {
        Set<String> params = new HashSet<>();
        int queryIndex = jdbcUrl.indexOf('?');
        if (queryIndex < 0) {
            return params;
        }
        for (String pair : jdbcUrl.substring(queryIndex + 1).split("&")) {
            int equalsIndex = pair.indexOf('=');
            String name = equalsIndex < 0 ? pair : pair.substring(0, equalsIndex);
            if (!name.isEmpty()) {
                params.add(name);
            }
        }
        return params;
    }
}
