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

package org.apache.doris.connector.spi.scan;

/**
 * The key contract of the scan-node property table — the {@code Map<String, String>} a connector returns
 * from {@link ConnectorScanPlanProvider#getScanNodeProperties} (or wraps in
 * {@link ConnectorScanPlanProvider#getScanNodePropertiesResult}).
 *
 * <p>Two directions share that one map:</p>
 * <ul>
 *   <li><b>connector -&gt; engine</b>: the keys below that the engine reads while building the scan node's
 *       thrift. A connector emits only the ones it needs; every key is optional.</li>
 *   <li><b>engine -&gt; connector</b>: the {@code SYNTHETIC_*} keys the generic scan node injects into the
 *       copies of the map it hands to {@code appendExplainInfo} and {@code populateScanLevelParams}. They are
 *       facts only the engine knows (split counts, the pushed-down limit, whether the filtering was fully
 *       taken); they are never forwarded to BE as properties, though a connector may of course DECIDE
 *       something from them and write that decision into the thrift it builds.</li>
 * </ul>
 *
 * <p>Keys NOT listed here are connector-private: the engine never reads them, so a connector may use its
 * own namespaced keys (e.g. {@code paimon.*}) to carry information from its property builder to its own
 * {@code populateScanLevelParams} / {@code appendExplainInfo}. Prefix them with the connector's own name so
 * they cannot collide with a future engine-read key.</p>
 *
 * <p>Why this class exists: every literal here used to be duplicated between the engine's private constants
 * and each connector's inline string. A one-letter mismatch compiles, starts, and returns wrong data
 * silently (a mistyped {@link #TEXT_COLUMN_SEPARATOR} puts the whole text row into the first column). The
 * literals are the historical wire values and must not be changed — only referenced.</p>
 */
public final class ScanNodePropertyKeys {

    // ------------------------------------------------------------------------
    // connector -> engine
    // ------------------------------------------------------------------------

    /**
     * Selects the BE file reader. Recognized values: {@code parquet}, {@code orc}, {@code text},
     * {@code csv}, {@code json}, {@code avro}, {@code es_http}. Any other value — including the key being
     * absent — selects the JNI reader.
     *
     * <p>This is a SCAN-level decision (BE picks the scanner implementation before it looks at any single
     * range), so a connector that reads some ranges natively and some through JNI must not emit
     * {@code jni} here; see {@link ConnectorScanRange#isNativeReadRange()}.</p>
     */
    public static final String FILE_FORMAT_TYPE = "file_format_type";

    /**
     * Comma-separated Doris column names that are encoded in the file PATH rather than in the file body,
     * so BE does not try to decode them from the data. Names are taken verbatim (same case the connector
     * reported in its schema).
     */
    public static final String PATH_PARTITION_KEYS = "path_partition_keys";

    /**
     * Prefix for the storage configuration BE needs to read the files. The engine strips the prefix and
     * passes the remaining key/value pairs through as the range's location properties, so
     * {@code LOCATION_PREFIX + "s3.access_key"} arrives at BE as {@code s3.access_key}.
     */
    public static final String LOCATION_PREFIX = "location.";

    /**
     * The remote query text a passthrough connector rendered, used ONLY for the {@code QUERY:} line of
     * EXPLAIN. It is not sent to BE through this map.
     */
    public static final String REMOTE_QUERY = "query";

    // ------------------------------------------------------------------------
    // connector -> engine: text-family file attributes
    // ------------------------------------------------------------------------

    /**
     * Prefix of the file attributes for the text family of formats (TEXT / CSV / JSON). The literal is the
     * historical wire value and is NOT hive-specific: hudi, iceberg and any other connector reading a
     * text-family file goes through the same engine code path.
     *
     * <p>Only the suffixes declared below are read by the engine. A connector may put additional
     * {@code TEXT_PROPERTY_PREFIX}-prefixed keys in the map for its own consumption, but the engine
     * ignores them.</p>
     */
    public static final String TEXT_PROPERTY_PREFIX = "hive.text.";

    /** Fully qualified name of the SerDe class; selects the text sub-dialect on BE. */
    public static final String TEXT_SERDE_LIB = TEXT_PROPERTY_PREFIX + "serde_lib";

    /** Number of leading lines to skip (header rows). Parsed as an integer. */
    public static final String TEXT_SKIP_LINES = TEXT_PROPERTY_PREFIX + "skip_lines";

    /** Column separator. May be a multi-byte string. */
    public static final String TEXT_COLUMN_SEPARATOR = TEXT_PROPERTY_PREFIX + "column_separator";

    /** Line delimiter. May be a multi-byte string. */
    public static final String TEXT_LINE_DELIMITER = TEXT_PROPERTY_PREFIX + "line_delimiter";

    /** Separator between a map entry's key and value. */
    public static final String TEXT_MAPKV_DELIMITER = TEXT_PROPERTY_PREFIX + "mapkv_delimiter";

    /** Separator between the elements of an array / map / struct. */
    public static final String TEXT_COLLECTION_DELIMITER = TEXT_PROPERTY_PREFIX + "collection_delimiter";

    /** Escape character; a single character. */
    public static final String TEXT_ESCAPE = TEXT_PROPERTY_PREFIX + "escape";

    /** The literal that represents SQL NULL in the file. */
    public static final String TEXT_NULL_FORMAT = TEXT_PROPERTY_PREFIX + "null_format";

    /** Quote character enclosing a field; a single character. */
    public static final String TEXT_ENCLOSE = TEXT_PROPERTY_PREFIX + "enclose";

    /** {@code "true"} to strip the enclosing quotes from field values. */
    public static final String TEXT_TRIM_DOUBLE_QUOTES = TEXT_PROPERTY_PREFIX + "trim_double_quotes";

    /** {@code "true"} when the rows are JSON documents rather than delimited text. */
    public static final String TEXT_IS_JSON = TEXT_PROPERTY_PREFIX + "is_json";

    /** {@code "true"} to tolerate malformed JSON rows instead of failing the scan. Read only when
     * {@link #TEXT_IS_JSON} is {@code "true"}. */
    public static final String TEXT_OPENX_IGNORE_MALFORMED = TEXT_PROPERTY_PREFIX + "openx_ignore_malformed";

    // ------------------------------------------------------------------------
    // engine -> connector (never forwarded to BE as a property)
    // ------------------------------------------------------------------------

    /**
     * Number of scan ranges this node will read with a native BE reader, counted from
     * {@link ConnectorScanRange#isNativeReadRange()}. Injected by the engine into the property map passed to
     * {@link ConnectorScanPlanProvider#appendExplainInfo}, for connectors that surface a native/JNI split
     * distinction in EXPLAIN.
     */
    public static final String SYNTHETIC_NATIVE_READ_SPLITS = "__native_read_splits";

    /** Total number of scan ranges this node will read. Companion of {@link #SYNTHETIC_NATIVE_READ_SPLITS}. */
    public static final String SYNTHETIC_TOTAL_READ_SPLITS = "__total_read_splits";

    /**
     * Present with value {@code "true"} only while the engine renders a VERBOSE EXPLAIN, so a connector can
     * gate VERBOSE-only output from {@link ConnectorScanPlanProvider#appendExplainInfo} without an SPI
     * signature change. Absent otherwise.
     */
    public static final String SYNTHETIC_EXPLAIN_VERBOSE = "__explain_verbose";

    /**
     * The row limit the engine pushed down to this scan, as a decimal string; {@code "-1"} (or any
     * non-positive value) means there is none. Injected on BOTH the {@code appendExplainInfo} and the
     * {@code populateScanLevelParams} paths, because a connector that can ask its source to stop early needs
     * it while building thrift, not only while printing EXPLAIN.
     */
    public static final String SYNTHETIC_PUSHDOWN_LIMIT = "__pushdown_limit";

    /**
     * {@code "true"} when NO filtering is left for the engine to do after the scan — every conjunct was taken
     * by the connector. Companion of {@link #SYNTHETIC_PUSHDOWN_LIMIT}: limiting rows at the source is only
     * correct when the source is not going to hand back rows that a leftover engine-side filter would then
     * discard.
     *
     * <p>PRECONDITION, or this key lies: the engine can only know which conjuncts were taken if the connector
     * reported them, i.e. returned {@link ScanNodePropertiesResult#withPushdownTracking}. A connector that
     * returns the plain result reads {@code "false"} here even when it did in fact push everything. Report
     * tracking, or do not use this key.</p>
     */
    public static final String SYNTHETIC_ALL_CONJUNCTS_PUSHED = "__all_conjuncts_pushed";

    private ScanNodePropertyKeys() {
    }
}
