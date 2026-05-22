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

package org.apache.doris.connector.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * Extracts text format properties (field delimiters, escape characters, etc.)
 * from HMS table/partition SerDe parameters.
 *
 * <p>These properties are passed to BE via scan node properties so that
 * the text/CSV/JSON file reader can parse the data correctly.</p>
 *
 * <p>The property keys mirror the constants used in fe-core's
 * {@code HiveProperties} and {@code HiveMetaStoreClientHelper}.</p>
 */
public final class HiveTextProperties {

    // SerDe library class names
    public static final String HIVE_TEXT_SERDE =
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    public static final String HIVE_MULTI_DELIMIT_SERDE =
            "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
    public static final String HIVE_OPEN_CSV_SERDE =
            "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    public static final String HIVE_JSON_SERDE =
            "org.apache.hive.hcatalog.data.JsonSerDe";
    public static final String OPENX_JSON_SERDE =
            "org.openx.data.jsonserde.JsonSerDe";

    // SerDe property keys
    private static final String FIELD_DELIM = "field.delim";
    private static final String SERIALIZATION_FORMAT = "serialization.format";
    private static final String LINE_DELIM = "line.delim";
    private static final String MAPKEY_DELIM = "mapkey.delim";
    private static final String COLLECTION_DELIM = "collection.delim";
    private static final String ESCAPE_DELIM = "escape.delim";
    private static final String SERIALIZATION_NULL_FORMAT = "serialization.null.format";
    private static final String SKIP_HEADER_LINE_COUNT = "skip.header.line.count";

    // CSV SerDe property keys
    private static final String SEPARATOR_CHAR = "separatorChar";
    private static final String QUOTE_CHAR = "quoteChar";
    private static final String ESCAPE_CHAR = "escapeChar";

    // Output property key prefix for scan node properties
    public static final String PROP_PREFIX = "hive.text.";

    private HiveTextProperties() {
    }

    /**
     * Extracts text format properties from the SerDe parameters of a table or partition.
     * Returns properties prefixed with {@link #PROP_PREFIX} for use in scan node properties.
     *
     * @param serDeLib  the SerDe library class name
     * @param sdParams  the StorageDescriptor / SerDeInfo parameters
     * @param tableParams the table-level parameters (for skip.header.line.count)
     * @return map of text properties, empty if not a text-based format
     */
    public static Map<String, String> extract(String serDeLib,
            Map<String, String> sdParams, Map<String, String> tableParams) {
        Map<String, String> result = new HashMap<>();
        if (serDeLib == null) {
            return result;
        }
        if (HIVE_TEXT_SERDE.equals(serDeLib) || HIVE_MULTI_DELIMIT_SERDE.equals(serDeLib)) {
            extractTextSerDeProps(sdParams, result,
                    HIVE_MULTI_DELIMIT_SERDE.equals(serDeLib));
        } else if (HIVE_OPEN_CSV_SERDE.equals(serDeLib)) {
            extractCsvSerDeProps(sdParams, result);
        } else if (HIVE_JSON_SERDE.equals(serDeLib) || OPENX_JSON_SERDE.equals(serDeLib)) {
            extractJsonSerDeProps(serDeLib, result);
        } else {
            return result;
        }

        // Skip header count from table parameters
        int skipLines = getSkipHeaderCount(tableParams);
        result.put(PROP_PREFIX + "skip_lines", String.valueOf(skipLines));
        result.put(PROP_PREFIX + "serde_lib", serDeLib);
        return result;
    }

    private static void extractTextSerDeProps(Map<String, String> params,
            Map<String, String> result, boolean supportMultiChar) {
        // Column separator
        String fieldDelim = getFieldDelimiter(params, supportMultiChar);
        result.put(PROP_PREFIX + "column_separator", fieldDelim);
        // Line delimiter
        result.put(PROP_PREFIX + "line_delimiter", getLineDelimiter(params));
        // MapKV delimiter
        result.put(PROP_PREFIX + "mapkv_delimiter", getMapKvDelimiter(params));
        // Collection delimiter
        result.put(PROP_PREFIX + "collection_delimiter", getCollectionDelimiter(params));
        // Escape delimiter
        String escape = getParamOrDefault(params, ESCAPE_DELIM, null);
        if (escape != null && !escape.isEmpty()) {
            result.put(PROP_PREFIX + "escape", escape);
        }
        // Null format
        result.put(PROP_PREFIX + "null_format",
                getParamOrDefault(params, SERIALIZATION_NULL_FORMAT, "\\N"));
    }

    private static void extractCsvSerDeProps(Map<String, String> params,
            Map<String, String> result) {
        result.put(PROP_PREFIX + "column_separator",
                getParamOrDefault(params, SEPARATOR_CHAR, ","));
        result.put(PROP_PREFIX + "line_delimiter", getLineDelimiter(params));
        String quoteChar = getParamOrDefault(params, QUOTE_CHAR, "\"");
        result.put(PROP_PREFIX + "enclose", quoteChar);
        String escapeChar = getParamOrDefault(params, ESCAPE_CHAR, "\\");
        result.put(PROP_PREFIX + "escape", escapeChar);
        result.put(PROP_PREFIX + "null_format", "");
    }

    private static void extractJsonSerDeProps(String serDeLib,
            Map<String, String> result) {
        result.put(PROP_PREFIX + "column_separator", "\t");
        result.put(PROP_PREFIX + "line_delimiter", "\n");
        result.put(PROP_PREFIX + "is_json", "true");
        result.put(PROP_PREFIX + "json_serde_lib", serDeLib);
    }

    private static String getFieldDelimiter(Map<String, String> params,
            boolean supportMultiChar) {
        String delim = getParamOrDefault(params, FIELD_DELIM, null);
        if (delim == null || delim.isEmpty()) {
            delim = getParamOrDefault(params, SERIALIZATION_FORMAT, null);
        }
        if (delim == null || delim.isEmpty()) {
            return "\001"; // Default Hive field delimiter (Ctrl-A)
        }
        if (!supportMultiChar && delim.length() == 1) {
            return delim;
        }
        return delim;
    }

    private static String getLineDelimiter(Map<String, String> params) {
        return getParamOrDefault(params, LINE_DELIM, "\n");
    }

    private static String getMapKvDelimiter(Map<String, String> params) {
        return getParamOrDefault(params, MAPKEY_DELIM, "\003");
    }

    private static String getCollectionDelimiter(Map<String, String> params) {
        return getParamOrDefault(params, COLLECTION_DELIM, "\002");
    }

    private static int getSkipHeaderCount(Map<String, String> tableParams) {
        if (tableParams == null) {
            return 0;
        }
        String value = tableParams.get(SKIP_HEADER_LINE_COUNT);
        if (value == null || value.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String getParamOrDefault(Map<String, String> params,
            String key, String defaultVal) {
        if (params == null) {
            return defaultVal;
        }
        String val = params.get(key);
        return (val != null) ? val : defaultVal;
    }
}
