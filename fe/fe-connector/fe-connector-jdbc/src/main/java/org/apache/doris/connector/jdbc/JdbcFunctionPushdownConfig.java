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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Function pushdown configuration for JDBC connectors.
 *
 * <p>Determines which Doris functions can be pushed down to the remote
 * database and how function names should be rewritten. Each database type
 * has built-in defaults, which can be extended via the {@code function_rules}
 * catalog property (JSON format).</p>
 *
 * <p>Two models are supported:</p>
 * <ul>
 *   <li><b>Blacklist</b> (MySQL): all functions are pushable unless explicitly listed</li>
 *   <li><b>Whitelist</b> (ClickHouse, Oracle): only listed functions are pushable</li>
 * </ul>
 */
public final class JdbcFunctionPushdownConfig {

    private static final Logger LOG = LogManager.getLogger(JdbcFunctionPushdownConfig.class);

    // ---- MySQL defaults (blacklist model) ----
    private static final Set<String> MYSQL_UNSUPPORTED_FUNCTIONS;

    static {
        TreeSet<String> s = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        s.addAll(Arrays.asList("date_trunc", "money_format", "negative"));
        MYSQL_UNSUPPORTED_FUNCTIONS = Collections.unmodifiableSet(s);
    }

    private static final Map<String, String> MYSQL_REPLACEMENT_MAP;

    static {
        Map<String, String> m = new HashMap<>();
        m.put("nvl", "ifnull");
        m.put("to_date", "date");
        MYSQL_REPLACEMENT_MAP = Collections.unmodifiableMap(m);
    }

    // ---- ClickHouse defaults (whitelist model) ----
    private static final Set<String> CLICKHOUSE_SUPPORTED_FUNCTIONS;

    static {
        TreeSet<String> s = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        s.addAll(Arrays.asList("from_unixtime", "unix_timestamp"));
        CLICKHOUSE_SUPPORTED_FUNCTIONS = Collections.unmodifiableSet(s);
    }

    private static final Map<String, String> CLICKHOUSE_REPLACEMENT_MAP;

    static {
        Map<String, String> m = new HashMap<>();
        m.put("from_unixtime", "FROM_UNIXTIME");
        m.put("unix_timestamp", "toUnixTimestamp");
        CLICKHOUSE_REPLACEMENT_MAP = Collections.unmodifiableMap(m);
    }

    // ---- Oracle defaults (whitelist model) ----
    private static final Set<String> ORACLE_SUPPORTED_FUNCTIONS;

    static {
        TreeSet<String> s = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        s.addAll(Arrays.asList("nvl", "ifnull"));
        ORACLE_SUPPORTED_FUNCTIONS = Collections.unmodifiableSet(s);
    }

    private static final Map<String, String> ORACLE_REPLACEMENT_MAP;

    static {
        Map<String, String> m = new HashMap<>();
        m.put("ifnull", "nvl");
        ORACLE_REPLACEMENT_MAP = Collections.unmodifiableMap(m);
    }

    // ---- Time arithmetic patterns ----
    // Nereids generates functions like "years_add", "months_sub", etc.
    // These must be rewritten to DATE_ADD/DATE_SUB + INTERVAL syntax.
    static final Map<String, String> TIME_UNIT_SUFFIXES;

    static {
        Map<String, String> m = new HashMap<>();
        m.put("years", "YEAR");
        m.put("months", "MONTH");
        m.put("weeks", "WEEK");
        m.put("days", "DAY");
        m.put("hours", "HOUR");
        m.put("minutes", "MINUTE");
        m.put("seconds", "SECOND");
        TIME_UNIT_SUFFIXES = Collections.unmodifiableMap(m);
    }

    // ---- Instance fields ----
    private final Set<String> supportedFunctions;
    private final Set<String> unsupportedFunctions;
    private final Map<String, String> replacementMap;
    private final boolean extFuncPredPushdownEnabled;

    private JdbcFunctionPushdownConfig(Set<String> supportedFunctions,
            Set<String> unsupportedFunctions,
            Map<String, String> replacementMap,
            boolean extFuncPredPushdownEnabled) {
        this.supportedFunctions = supportedFunctions;
        this.unsupportedFunctions = unsupportedFunctions;
        this.replacementMap = replacementMap;
        this.extFuncPredPushdownEnabled = extFuncPredPushdownEnabled;
    }

    /**
     * Creates a function pushdown config for the given database type.
     *
     * @param dbType the JDBC database type
     * @param functionRulesJson optional JSON rules from the "function_rules" catalog property
     * @param extFuncPredPushdownEnabled whether external function predicate pushdown is enabled
     *                                   (session variable enable_ext_func_pred_pushdown)
     */
    public static JdbcFunctionPushdownConfig create(JdbcDbType dbType,
            String functionRulesJson, boolean extFuncPredPushdownEnabled) {
        Set<String> supported = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> unsupported = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> replacement = new HashMap<>();

        switch (dbType) {
            case MYSQL:
                unsupported.addAll(MYSQL_UNSUPPORTED_FUNCTIONS);
                replacement.putAll(MYSQL_REPLACEMENT_MAP);
                break;
            case CLICKHOUSE:
                supported.addAll(CLICKHOUSE_SUPPORTED_FUNCTIONS);
                replacement.putAll(CLICKHOUSE_REPLACEMENT_MAP);
                break;
            case ORACLE:
            case OCEANBASE_ORACLE:
                supported.addAll(ORACLE_SUPPORTED_FUNCTIONS);
                replacement.putAll(ORACLE_REPLACEMENT_MAP);
                break;
            default:
                break;
        }

        // Apply user-customizable JSON overrides
        if (functionRulesJson != null && !functionRulesJson.isEmpty()) {
            applyJsonRules(functionRulesJson, supported, unsupported, replacement);
        }

        return new JdbcFunctionPushdownConfig(
                Collections.unmodifiableSet(supported),
                Collections.unmodifiableSet(unsupported),
                Collections.unmodifiableMap(replacement),
                extFuncPredPushdownEnabled);
    }

    /**
     * Checks if the given function can be pushed down to the remote database.
     *
     * @param functionName the function name to check
     * @return true if the function can be pushed down
     */
    public boolean canPushDown(String functionName) {
        if (!extFuncPredPushdownEnabled) {
            return false;
        }

        if (supportedFunctions.isEmpty() && unsupportedFunctions.isEmpty()) {
            // No rules defined for this DB type — don't push functions
            return false;
        }

        // Whitelist model: only listed functions allowed
        if (!supportedFunctions.isEmpty()) {
            return supportedFunctions.contains(functionName);
        }

        // Blacklist model: everything allowed except listed
        return !unsupportedFunctions.contains(functionName);
    }

    /**
     * Returns the rewritten function name, or the original name if no rewrite exists.
     */
    public String rewriteFunctionName(String functionName) {
        return replacementMap.getOrDefault(functionName, functionName);
    }

    /**
     * Checks if the function name matches a Nereids time arithmetic pattern
     * (e.g. "years_add", "months_sub") and returns the parsed result.
     *
     * @param functionName the function name to check
     * @return a {@link TimeArithmeticInfo} if matched, or null if not a time arithmetic function
     */
    public static TimeArithmeticInfo parseTimeArithmetic(String functionName) {
        for (Map.Entry<String, String> entry : TIME_UNIT_SUFFIXES.entrySet()) {
            String suffix = entry.getKey();
            String timeUnit = entry.getValue();
            if (functionName.endsWith(suffix + "_add")) {
                return new TimeArithmeticInfo("date_add", timeUnit);
            } else if (functionName.endsWith(suffix + "_sub")) {
                return new TimeArithmeticInfo("date_sub", timeUnit);
            }
        }
        return null;
    }

    /** Parsed result for a time arithmetic function pattern. */
    public static final class TimeArithmeticInfo {
        private final String baseFunction;
        private final String timeUnit;

        TimeArithmeticInfo(String baseFunction, String timeUnit) {
            this.baseFunction = baseFunction;
            this.timeUnit = timeUnit;
        }

        public String getBaseFunction() {
            return baseFunction;
        }

        public String getTimeUnit() {
            return timeUnit;
        }
    }

    private static void applyJsonRules(String json, Set<String> supported,
            Set<String> unsupported, Map<String, String> replacement) {
        try {
            // Parse "pushdown" -> "supported" and "unsupported" arrays
            List<String> supportedList = extractJsonStringArray(json, "supported");
            for (String fn : supportedList) {
                supported.add(fn.toLowerCase());
            }
            List<String> unsupportedList = extractJsonStringArray(json, "unsupported");
            for (String fn : unsupportedList) {
                unsupported.add(fn.toLowerCase());
            }
            // Parse "rewrite" -> { "key": "value", ... }
            Map<String, String> rewriteMap = extractJsonStringMap(json, "rewrite");
            replacement.putAll(rewriteMap);
        } catch (Exception e) {
            LOG.warn("Failed to parse function_rules JSON: {}", json, e);
        }
    }

    /**
     * Extracts a JSON string array value by key name from a flat/nested JSON string.
     * Supports format: {@code "key": ["val1", "val2"]}.
     */
    private static List<String> extractJsonStringArray(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\\[([^\\]]*)\\]");
        Matcher m = p.matcher(json);
        if (!m.find()) {
            return Collections.emptyList();
        }
        String arrayContent = m.group(1).trim();
        if (arrayContent.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(arrayContent.replaceAll("\"", "").split("\\s*,\\s*"));
    }

    /**
     * Extracts a JSON string map value by key name from a JSON string.
     * Supports format: {@code "key": {"k1": "v1", "k2": "v2"}}.
     */
    private static Map<String, String> extractJsonStringMap(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\\{([^}]*)\\}");
        Matcher m = p.matcher(json);
        if (!m.find()) {
            return Collections.emptyMap();
        }
        String mapContent = m.group(1).trim();
        if (mapContent.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        Pattern kvPattern = Pattern.compile("\"([^\"]+)\"\\s*:\\s*\"([^\"]+)\"");
        Matcher kvMatcher = kvPattern.matcher(mapContent);
        while (kvMatcher.find()) {
            result.put(kvMatcher.group(1), kvMatcher.group(2));
        }
        return result;
    }
}
