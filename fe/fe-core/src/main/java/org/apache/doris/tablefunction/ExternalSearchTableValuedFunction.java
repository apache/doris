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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Common bounds for distributed external-search relations. */
public abstract class ExternalSearchTableValuedFunction extends TableValuedFunctionIf {
    public abstract long getTopK();

    public abstract long getOffset();

    protected static Map<String, String> normalizeProperties(Map<String, String> properties,
            Set<String> allowedProperties, String functionName) throws AnalysisException {
        Map<String, String> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!allowedProperties.contains(key)) {
                throw new AnalysisException("'" + entry.getKey()
                        + "' is an invalid property for " + functionName + "()");
            }
            if (normalized.put(key, entry.getValue()) != null) {
                throw new AnalysisException(
                        "Duplicate " + functionName + "() property '" + key + "'");
            }
        }
        return normalized;
    }

    protected static String required(Map<String, String> params, String key, String functionName)
            throws AnalysisException {
        String value = params.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new AnalysisException(
                    "Missing required " + functionName + "() property '" + key + "'");
        }
        return value.trim();
    }

    protected static long parseLong(String value, String property, long min, long max)
            throws AnalysisException {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < min || parsed > max) {
                throw new AnalysisException("'" + property + "' must be between "
                        + min + " and " + max);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new AnalysisException("'" + property + "' must be an integer", e);
        }
    }

    protected static boolean parseBoolean(String value, String property)
            throws AnalysisException {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new AnalysisException("'" + property + "' must be 'true' or 'false'");
    }

    protected static byte[] validateAndEncodeSqlFilter(String filter) throws AnalysisException {
        if (filter == null || filter.trim().isEmpty()) {
            throw new AnalysisException("'filter' must not be empty");
        }
        if (filter.indexOf('\0') >= 0) {
            throw new AnalysisException("'filter' must not contain an embedded NUL byte");
        }
        return filter.getBytes(StandardCharsets.UTF_8);
    }
}
