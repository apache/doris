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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class HiveAvroSchemaPolicyTest {
    private static final String LITERAL = "avro.schema.literal";
    private static final String URL = "avro.schema.url";
    private static final String GENERATED = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"v\",\"type\":[\"null\",\"int\"],\"default\":null}]}";
    private static final String DEFAULT_SEVEN = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"added\",\"type\":\"int\",\"default\":7}]}";

    private final HiveScanPlanProvider provider =
            new HiveScanPlanProvider(null, HiveTestProperties.minimal(), null, null);
    private final ConnectorSession session = new ScopeSession(1, "avro-schema", new TestStatementScope());

    private HiveTableHandle handle(Map<String, String> table, Map<String, String> serde) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
                .serializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
                .tableParameters(table).sdParameters(serde).build();
    }

    private void inspect(Map<String, String> table, Map<String, String> serde) {
        provider.getScanNodeProperties(session, handle(table, serde), Collections.emptyList(), Optional.empty());
    }

    @Test
    public void rejectsNonNullDefaultsBeforeReturningPlausibleNulls() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> inspect(Map.of(LITERAL, DEFAULT_SEVEN), Map.of()));
    }

    @Test
    public void everyPlanningEntryRejectsUnsupportedReaderResolution() {
        HiveTableHandle table = handle(Map.of(LITERAL, DEFAULT_SEVEN), Map.of());
        ConnectorScanRequest request = ConnectorScanRequest.builder(table, Collections.emptyList()).build();
        Assertions.assertThrows(DorisConnectorException.class, () -> provider.planScan(session, request));
        Assertions.assertThrows(DorisConnectorException.class, () -> provider.streamSplits(session, request));
    }

    @Test
    public void acceptsGeneratedNullableSchemaAndNoExplicitSchema() {
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, GENERATED), Map.of()));
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(), Map.of()));
    }

    @Test
    public void preservesTableAndLiteralPrecedenceWithoutFetchingUrls() {
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, GENERATED),
                Map.of(LITERAL, DEFAULT_SEVEN, URL, "https://example.org/schema.avsc")));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> inspect(Map.of(LITERAL, "none"),
                        Map.of(LITERAL, GENERATED, URL, "https://example.org/schema.avsc")));
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(URL, "none"),
                Map.of(URL, "https://example.org/schema.avsc")));
    }

    @Test
    public void rejectsMissingDefaultsAliasesAndNestedReaderFeatures() {
        String missing = GENERATED.replace(",\"default\":null", "");
        String aliased = GENERATED.replace("\"name\":\"v\"", "\"name\":\"v\",\"aliases\":[\"old\"]");
        String nested = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"v\",\"default\":null,\"type\":[\"null\","
                + "{\"type\":\"array\",\"items\":" + DEFAULT_SEVEN + "}]}]}";
        String enumeration = GENERATED.replace("\"int\"", "{\"type\":\"enum\",\"name\":\"e\","
                + "\"symbols\":[\"a\"],\"default\":\"a\"}");
        for (String schema : new String[] {missing, aliased, nested, enumeration}) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> inspect(Map.of(LITERAL, schema), Map.of()));
        }
    }

    @Test
    public void rejectsHiveCharAndVarcharReaderCoercionsRecursively() {
        for (String logical : new String[] {"char", "varchar", "ChAr", "VaRcHaR"}) {
            String type = "{\"type\":\"string\",\"logicalType\":\"" + logical + "\",\"maxLength\":3}";
            for (String value : new String[] {type, "{\"type\":\"array\",\"items\":" + type + "}",
                    "{\"type\":\"map\",\"values\":" + type + "}"}) {
                String schema = GENERATED.replace("\"int\"", value);
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> inspect(Map.of(LITERAL, schema), Map.of()));
            }
        }
    }

    @Test
    public void rejectsCaseInsensitiveDecimalAnnotationsBeforeNativeErasure() {
        for (String logical : new String[] {"DeCiMaL", "decımal"}) {
            String value = "{\"type\":\"bytes\",\"logicalType\":\"" + logical
                    + "\",\"precision\":9,\"scale\":2}";
            String schema = GENERATED.replace("\"int\"", value);
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> inspect(Map.of(LITERAL, schema), Map.of()));
        }
    }

    @Test
    public void ignoresAvroPropertiesForOtherPhysicalFormats() {
        HiveTableHandle table = handle(Map.of(LITERAL, DEFAULT_SEVEN), Map.of()).toBuilder()
                .inputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat").build();
        Assertions.assertDoesNotThrow(() -> provider.getScanNodeProperties(
                session, table, Collections.emptyList(), Optional.empty()));
    }
}
