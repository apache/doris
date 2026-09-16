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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.hive.HiveTextProperties;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileTextScanRangeParams;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class PluginDrivenScanNodeCsvPropertiesTest {
    private static final String CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

    @Test
    void testNormalizedCharactersSurviveScanNodeAndThrift() throws Exception {
        Map<String, String> properties = Map.of("separatorChar", "ss", "quoteChar", "qq", "escapeChar", "ee",
                "line.delim", "|");
        assertWireProperties(toWire(Map.of(), properties), "s", 'q', 'e', false);
        assertWireProperties(toWire(properties, Map.of("line.delim", "|")), "s", 'q', 'e', false);
    }

    @Test
    void testEffectiveTablePropertiesOverrideInvalidSerdeProperties() throws Exception {
        assertWireProperties(toWire(Map.of("separatorChar", "", "quoteChar", "é", "escapeChar", ""),
                Map.of("separatorChar", "ss", "quoteChar", "qq", "escapeChar", "ee")),
                "s", 'q', 'e', false);
    }

    @Test
    void testDefaultsAndNormalizedDoubleQuote() throws Exception {
        assertWireProperties(toWire(Map.of(), Map.of()), ",", '"', '\\', true);
        assertWireProperties(toWire(Map.of(),
                Map.of("separatorChar", "99", "quoteChar", "\"suffix", "escapeChar", "eé")),
                "9", '"', 'e', true);
    }

    @Test
    void testUtf8SeparatorRemainsACompleteCharacter() throws Exception {
        assertWireProperties(toWire(Map.of(), Map.of("separatorChar", "ésuffix")), "é", '"', '\\', true);
    }

    @Test
    void testInvalidCharactersFailBeforeThriftConstruction() {
        for (String key : new String[] {"separatorChar", "quoteChar", "escapeChar"}) {
            Assertions.assertThrows(DorisConnectorException.class, () -> toWire(Map.of(), Map.of(key, "")));
        }
        for (String key : new String[] {"quoteChar", "escapeChar"}) {
            Assertions.assertThrows(DorisConnectorException.class, () -> toWire(Map.of(), Map.of(key, "é")));
        }
        Assertions.assertThrows(DorisConnectorException.class,
                () -> toWire(Map.of(), Map.of("separatorChar", "😀")));
    }

    @Test
    void testDefaultEscapeSentinelSurvivesThriftAsBackslash() throws Exception {
        for (String quote : new String[] {"\"", "q"}) {
            Map<String, String> properties = Map.of("quoteChar", quote, "escapeChar", "\"suffix");
            assertWireProperties(toWire(Map.of(), properties), ",", quote.charAt(0), '\\', "\"".equals(quote));
            assertWireProperties(toWire(properties, Map.of()), ",", quote.charAt(0), '\\', "\"".equals(quote));
        }
    }

    @Test
    void testCharacterConflictsFailBeforeThriftConstruction() {
        for (Map<String, String> properties : List.of(
                Map.of("separatorChar", "|", "quoteChar", "|"),
                Map.of("separatorChar", "|", "escapeChar", "|"),
                Map.of("quoteChar", "q", "escapeChar", "q"),
                Map.of("separatorChar", "\\", "escapeChar", "\""),
                Map.of("separatorChar", "\0"))) {
            Assertions.assertThrows(DorisConnectorException.class, () -> toWire(Map.of(), properties));
            Assertions.assertThrows(DorisConnectorException.class, () -> toWire(properties, Map.of()));
        }
    }

    @Test
    void testDisabledQuoteAndEscapeSurviveThrift() throws Exception {
        Map<String, String> properties = Map.of("quoteChar", "\0", "escapeChar", "\0");
        assertWireProperties(toWire(Map.of(), properties), ",", '\0', '\0', false);
        assertWireProperties(toWire(properties, Map.of()), ",", '\0', '\0', false);
    }

    @Test
    void testOtherHiveSerdesDoNotEnableOpenCsvParsing() throws Exception {
        for (String serde : List.of(HiveTextProperties.HIVE_TEXT_SERDE, HiveTextProperties.HIVE_JSON_SERDE)) {
            TFileAttributes attributes = propertiesToWire(HiveTextProperties.extract(serde, Map.of(), Map.of()));
            Assertions.assertFalse(attributes.isSetHiveOpenCsv());
            Assertions.assertFalse(attributes.isHiveOpenCsv());
        }
    }

    private static TFileAttributes toWire(Map<String, String> serdeProperties,
            Map<String, String> tableProperties) throws Exception {
        return propertiesToWire(HiveTextProperties.extract(CSV_SERDE, serdeProperties, tableProperties));
    }

    private static TFileAttributes propertiesToWire(Map<String, String> properties) throws Exception {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(node, "scanNodeProperties", properties);
        Deencapsulation.setField(node, "sessionVariable", new SessionVariable());
        // Exercise the real scan-node consumer and wire encoding: map assertions cannot detect byte truncation.
        TFileAttributes attributes = node.getFileAttributes();
        TFileAttributes decoded = new TFileAttributes();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded,
                new TSerializer(new TCompactProtocol.Factory()).serialize(attributes));
        return decoded;
    }

    private static void assertWireProperties(TFileAttributes attributes, String separator,
            char quote, char escape, boolean trim) {
        assertWireProperties(attributes, separator, quote, escape, trim, "\n");
    }

    private static void assertWireProperties(TFileAttributes attributes, String separator,
            char quote, char escape, boolean trim, String lineDelimiter) {
        Assertions.assertTrue(attributes.isHiveOpenCsv());
        TFileTextScanRangeParams text = attributes.getTextParams();
        Assertions.assertEquals(separator, text.getColumnSeparator());
        Assertions.assertEquals(lineDelimiter, text.getLineDelimiter());
        Assertions.assertTrue(text.isSetEnclose());
        Assertions.assertEquals((byte) quote, text.getEnclose());
        Assertions.assertTrue(text.isSetEscape());
        Assertions.assertEquals((byte) escape, text.getEscape());
        Assertions.assertEquals("", text.getNullFormat());
        Assertions.assertEquals(trim, attributes.isTrimDoubleQuotes());
    }
}
