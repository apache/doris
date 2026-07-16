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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link HiveTextProperties} delimiter extraction, pinned to legacy fe-core parity
 * ({@code HiveProperties} + {@code HiveMetaStoreClientHelper.getByte}).
 *
 * <p>WHY these assertions matter: Hive stores text delimiters as NUMERIC BYTE-VALUE STRINGS
 * in SerDe params. The canonical case is a default {@code LazySimpleSerDe} table, whose field
 * delimiter is stored as {@code serialization.format=1} — meaning byte {@code 0x01} (Ctrl-A),
 * NOT the character {@code '1'} (0x31). Legacy applies {@code getByte()} to decode the numeric
 * string into the real delimiter byte; the connector must reproduce this exactly, or BE splits
 * text rows on the wrong character and every column collapses to null/merged.</p>
 */
public class HiveTextPropertiesTest {

    private static final String TEXT_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String MULTI_DELIMIT_SERDE = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";
    private static final String OPENX_JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe";
    private static final String HCATALOG_JSON_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe";
    private static final String OPEN_CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    private static final String PREFIX = "hive.text.";

    private static String colSep(String serde, Map<String, String> sd) {
        return HiveTextProperties.extract(serde, sd, new HashMap<>()).get(PREFIX + "column_separator");
    }

    private static Map<String, String> sd(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void testDefaultSerializationFormatDecodesToCtrlA() {
        // Default LazySimpleSerDe: Hive stores serialization.format="1" == byte 0x01, no field.delim.
        Assertions.assertEquals("\001", colSep(TEXT_SERDE, sd("serialization.format", "1")));
    }

    @Test
    public void testNoDelimiterFallsBackToDefaultCtrlA() {
        Assertions.assertEquals("\001", colSep(TEXT_SERDE, sd()));
    }

    @Test
    public void testLiteralFieldDelimiterPreserved() {
        // Explicit "FIELDS TERMINATED BY ','": Hive stores the literal comma; getByte keeps it.
        Assertions.assertEquals(",", colSep(TEXT_SERDE, sd("field.delim", ",")));
    }

    @Test
    public void testNumericFieldDelimiterDecodesToByte() {
        // field.delim="9" means byte 0x09 (tab), not the character '9'.
        Assertions.assertEquals("\t", colSep(TEXT_SERDE, sd("field.delim", "9")));
    }

    @Test
    public void testFieldDelimTakesPrecedenceOverSerializationFormat() {
        Assertions.assertEquals(",", colSep(TEXT_SERDE, sd("field.delim", ",", "serialization.format", "1")));
    }

    @Test
    public void testMultiDelimitSerDeKeepsRawMultiCharDelimiter() {
        // MultiDelimitSerDe supports multi-character delimiters; they must NOT be byte-decoded/truncated.
        Assertions.assertEquals("||", colSep(MULTI_DELIMIT_SERDE, sd("field.delim", "||")));
    }

    @Test
    public void testCollectionDelimiterDefaultAndHive2Typo() {
        Map<String, String> r = HiveTextProperties.extract(TEXT_SERDE, sd(), new HashMap<>());
        Assertions.assertEquals("\002", r.get(PREFIX + "collection_delimiter"));
        // Hive2 uses the famously misspelled key "colelction.delim"; "2" decodes to byte 0x02.
        Map<String, String> r2 = HiveTextProperties.extract(TEXT_SERDE, sd("colelction.delim", "2"), new HashMap<>());
        Assertions.assertEquals("\002", r2.get(PREFIX + "collection_delimiter"));
    }

    @Test
    public void testMapkvAndLineDefaults() {
        Map<String, String> r = HiveTextProperties.extract(TEXT_SERDE, sd(), new HashMap<>());
        Assertions.assertEquals("\003", r.get(PREFIX + "mapkv_delimiter"));
        Assertions.assertEquals("\n", r.get(PREFIX + "line_delimiter"));
    }

    @Test
    public void testTableParamsTakePrecedenceOverSerdeParams() {
        // Legacy getSerdeProperty checks table params first, then serde params.
        Map<String, String> sdParams = sd("serialization.format", "1");
        Map<String, String> tblParams = new HashMap<>();
        tblParams.put("field.delim", ",");
        String sep = HiveTextProperties.extract(TEXT_SERDE, sdParams, tblParams).get(PREFIX + "column_separator");
        Assertions.assertEquals(",", sep);
    }

    // ---- OpenX JSON ignore.malformed.json (legacy HiveScanNode OPENX_JSON_SERDE branch parity) ----

    @Test
    public void testOpenxJsonIgnoreMalformedTrueEmitted() {
        // OpenX table with SERDEPROPERTIES ignore.malformed.json=true -> BE must skip malformed rows.
        Map<String, String> r = HiveTextProperties.extract(
                OPENX_JSON_SERDE, sd("ignore.malformed.json", "true"), new HashMap<>());
        Assertions.assertEquals("true", r.get(PREFIX + "openx_ignore_malformed"));
        Assertions.assertEquals("true", r.get(PREFIX + "is_json"));
    }

    @Test
    public void testOpenxJsonIgnoreMalformedDefaultsFalse() {
        // No property -> "false" (== Thrift default): malformed rows still error, matching legacy default.
        Map<String, String> r = HiveTextProperties.extract(OPENX_JSON_SERDE, sd(), new HashMap<>());
        Assertions.assertEquals("false", r.get(PREFIX + "openx_ignore_malformed"));
    }

    @Test
    public void testOpenxJsonIgnoreMalformedTableParamPrecedence() {
        // Table param (true) beats serde param (false), mirroring legacy getSerdeProperty precedence.
        Map<String, String> tblParams = new HashMap<>();
        tblParams.put("ignore.malformed.json", "true");
        Map<String, String> r = HiveTextProperties.extract(
                OPENX_JSON_SERDE, sd("ignore.malformed.json", "false"), tblParams);
        Assertions.assertEquals("true", r.get(PREFIX + "openx_ignore_malformed"));
    }

    @Test
    public void testHcatalogJsonSerdeOmitsOpenxKey() {
        // Non-OpenX JSON serde never carried this flag: the key must be absent (legacy set it only for OpenX).
        Map<String, String> r = HiveTextProperties.extract(HCATALOG_JSON_SERDE, sd(), new HashMap<>());
        Assertions.assertFalse(r.containsKey(PREFIX + "openx_ignore_malformed"));
        Assertions.assertEquals("true", r.get(PREFIX + "is_json"));
    }

    // ---- #65501: trim_double_quotes must be emitted ONLY when the enclose char is the double-quote '"' ----

    @Test
    public void testCsvDefaultQuoteCharTrimsDoubleQuotes() {
        // OpenCSVSerde with no quoteChar -> default '"' -> BE must trim the wrapping double quotes.
        Map<String, String> r = HiveTextProperties.extract(OPEN_CSV_SERDE, sd(), new HashMap<>());
        Assertions.assertEquals("\"", r.get(PREFIX + "enclose"));
        Assertions.assertEquals("true", r.get(PREFIX + "trim_double_quotes"));
    }

    @Test
    public void testCsvNonDoubleQuoteEncloseDoesNotTrim() {
        // A single-quote quoteChar must NOT set trim_double_quotes; otherwise BE would strip literal '"'
        // characters from the data (the exact pre-#65501 bug the master fix corrected).
        Map<String, String> r = HiveTextProperties.extract(OPEN_CSV_SERDE, sd("quoteChar", "'"), new HashMap<>());
        Assertions.assertEquals("'", r.get(PREFIX + "enclose"));
        Assertions.assertEquals("false", r.get(PREFIX + "trim_double_quotes"));
    }

    @Test
    public void testCsvExplicitDoubleQuoteEncloseTrims() {
        // An explicit quoteChar="\"" is the double-quote case and must still trim.
        Map<String, String> r = HiveTextProperties.extract(OPEN_CSV_SERDE, sd("quoteChar", "\""), new HashMap<>());
        Assertions.assertEquals("true", r.get(PREFIX + "trim_double_quotes"));
    }
}
