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

    // SerDe library class names. Must stay in sync with HiveFileFormat's detection set: every serde the
    // detector classifies as TEXT/CSV/JSON needs a branch here, or its text params (delimiters etc.) are
    // dropped and BE reads with defaults.
    public static final String HIVE_TEXT_SERDE =
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    // MultiDelimitSerDe exists under two package names across Hive versions; both read as FORMAT_TEXT.
    public static final String HIVE_MULTI_DELIMIT_SERDE =
            "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe";
    public static final String HIVE_MULTI_DELIMIT_SERDE_SERDE2 =
            "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
    public static final String HIVE_OPEN_CSV_SERDE =
            "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    public static final String HIVE_JSON_SERDE =
            "org.apache.hive.hcatalog.data.JsonSerDe";
    // Legacy hive-2 JSON serde; legacy fe-core maps it to FORMAT_JSON like the hcatalog one.
    public static final String LEGACY_HIVE_JSON_SERDE =
            "org.apache.hadoop.hive.serde2.JsonSerDe";
    public static final String OPENX_JSON_SERDE =
            "org.openx.data.jsonserde.JsonSerDe";

    // SerDe property keys
    private static final String FIELD_DELIM = "field.delim";
    private static final String SERIALIZATION_FORMAT = "serialization.format";
    private static final String LINE_DELIM = "line.delim";
    private static final String MAPKEY_DELIM = "mapkey.delim";
    // Hive3 uses "collection.delim"; Hive2 uses the historically misspelled "colelction.delim". Legacy
    // fe-core checks both, so parity requires recognizing both.
    private static final String COLLECTION_DELIM = "collection.delim";
    private static final String COLLECTION_DELIM_HIVE2 = "colelction.delim";
    private static final String ESCAPE_DELIM = "escape.delim";
    private static final String SERIALIZATION_NULL_FORMAT = "serialization.null.format";
    private static final String SKIP_HEADER_LINE_COUNT = "skip.header.line.count";

    // Default delimiters, mirroring legacy HiveProperties. These are byte values (Ctrl-A etc.), not the
    // literal digit characters Hive stores them as in serialization.format / *.delim SerDe params.
    private static final String DEFAULT_FIELD_DELIM = "\001";
    private static final String DEFAULT_LINE_DELIM = "\n";
    private static final String DEFAULT_MAPKV_DELIM = "\003";
    private static final String DEFAULT_COLLECTION_DELIM = "\002";
    private static final String DEFAULT_ESCAPE_DELIM = "\\";
    private static final String DEFAULT_NULL_FORMAT = "\\N";

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
        boolean multiDelimit = HIVE_MULTI_DELIMIT_SERDE.equals(serDeLib)
                || HIVE_MULTI_DELIMIT_SERDE_SERDE2.equals(serDeLib);
        if (HIVE_TEXT_SERDE.equals(serDeLib) || multiDelimit) {
            extractTextSerDeProps(sdParams, tableParams, result, multiDelimit);
        } else if (HIVE_OPEN_CSV_SERDE.equals(serDeLib)) {
            extractCsvSerDeProps(sdParams, result);
        } else if (HIVE_JSON_SERDE.equals(serDeLib) || LEGACY_HIVE_JSON_SERDE.equals(serDeLib)
                || OPENX_JSON_SERDE.equals(serDeLib)) {
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

    private static void extractTextSerDeProps(Map<String, String> sdParams,
            Map<String, String> tableParams, Map<String, String> result, boolean supportMultiChar) {
        // Column separator. Hive stores single-char delimiters as their numeric byte value (the default
        // LazySimpleSerDe field delimiter is serialization.format="1" == byte 0x01, NOT the character
        // '1'), so they must be decoded via getByte(). MultiDelimitSerDe keeps its raw multi-char value.
        result.put(PROP_PREFIX + "column_separator",
                getFieldDelimiter(sdParams, tableParams, supportMultiChar));
        // Line delimiter
        result.put(PROP_PREFIX + "line_delimiter",
                getByte(serdeVal(sdParams, tableParams, LINE_DELIM), DEFAULT_LINE_DELIM));
        // MapKV delimiter
        result.put(PROP_PREFIX + "mapkv_delimiter",
                getByte(serdeVal(sdParams, tableParams, MAPKEY_DELIM), DEFAULT_MAPKV_DELIM));
        // Collection delimiter (Hive2 "colelction.delim" typo first, then Hive3 "collection.delim")
        result.put(PROP_PREFIX + "collection_delimiter",
                getByte(serdeVal(sdParams, tableParams, COLLECTION_DELIM_HIVE2, COLLECTION_DELIM),
                        DEFAULT_COLLECTION_DELIM));
        // Escape delimiter: emitted only when the SerDe sets it, decoded via getByte
        String escape = serdeVal(sdParams, tableParams, ESCAPE_DELIM);
        if (escape != null) {
            result.put(PROP_PREFIX + "escape", getByte(escape, DEFAULT_ESCAPE_DELIM));
        }
        // Null format (raw string; NOT byte-decoded)
        String nullFormat = serdeVal(sdParams, tableParams, SERIALIZATION_NULL_FORMAT);
        result.put(PROP_PREFIX + "null_format", nullFormat != null ? nullFormat : DEFAULT_NULL_FORMAT);
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

    private static String getFieldDelimiter(Map<String, String> sdParams,
            Map<String, String> tableParams, boolean supportMultiChar) {
        String delim = serdeVal(sdParams, tableParams, FIELD_DELIM, SERIALIZATION_FORMAT);
        if (delim == null) {
            delim = "";
        }
        // MultiDelimitSerDe delimiters may be multiple characters; keep them raw (no byte decode).
        return supportMultiChar ? delim : getByte(delim, DEFAULT_FIELD_DELIM);
    }

    private static String getLineDelimiter(Map<String, String> params) {
        return getParamOrDefault(params, LINE_DELIM, "\n");
    }

    /**
     * Looks up a SerDe property mirroring legacy {@code HiveMetaStoreClientHelper.getSerdeProperty}:
     * table parameters take precedence over StorageDescriptor/SerDeInfo parameters, and the keys are
     * tried in order. An empty string counts as present. Returns {@code null} if no key is set.
     */
    private static String serdeVal(Map<String, String> sdParams, Map<String, String> tableParams,
            String... keys) {
        for (String key : keys) {
            if (tableParams != null && tableParams.get(key) != null) {
                return tableParams.get(key);
            }
            if (sdParams != null && sdParams.get(key) != null) {
                return sdParams.get(key);
            }
        }
        return null;
    }

    /**
     * Decodes a Hive delimiter. Hive stores single-char delimiters as their numeric byte value
     * ("1" == 0x01, "9" == 0x09); a non-numeric value is taken literally and truncated to its first
     * character; an empty/absent value falls back to {@code defValue}. Mirrors legacy
     * {@code HiveMetaStoreClientHelper.getByte}.
     */
    private static String getByte(String altValue, String defValue) {
        if (altValue != null && !altValue.isEmpty()) {
            try {
                return Character.toString((char) ((Byte.parseByte(altValue) + 256) % 256));
            } catch (NumberFormatException e) {
                return altValue.substring(0, 1);
            }
        }
        return defValue;
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
