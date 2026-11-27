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

import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
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
        Assertions.assertThrows(DorisConnectorException.class, () -> inspect(Map.of(URL, "none"),
                Map.of(URL, "https://example.org/schema.avsc")));
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, GENERATED, URL, "none"), Map.of()));
    }

    @Test
    public void nullReaderDefaultsRequireNullAsTheFirstUnionBranch() {
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, GENERATED), Map.of()));
        String reversed = GENERATED.replace("[\"null\",\"int\"]", "[\"int\",\"null\"]");
        Assertions.assertThrows(DorisConnectorException.class,
                () -> inspect(Map.of(LITERAL, reversed), Map.of()));
        String array = GENERATED.replace("\"int\"",
                "{\"type\":\"array\",\"items\":[\"int\",\"null\"]}");
        String map = GENERATED.replace("\"int\"",
                "{\"type\":\"map\",\"values\":[\"int\",\"null\"]}");
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, array), Map.of()));
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, map), Map.of()));
        String nested = GENERATED.replace("\"int\"", "{\"type\":\"record\",\"name\":\"nested\",\"fields\":["
                + "{\"name\":\"extra\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, nested), Map.of()));
        Assertions.assertThrows(DorisConnectorException.class, () -> inspect(Map.of(LITERAL,
                nested.replace("[\"null\",\"long\"]", "[\"long\",\"null\"]")), Map.of()));
    }

    @Test
    public void schemaUrlNoneIsNotAnAbsentTableOrSerdeProperty() {
        for (Map<String, String> properties : List.of(Map.of(URL, "none"), Map.of(LITERAL, "none", URL, "none"))) {
            Assertions.assertThrows(DorisConnectorException.class, () -> inspect(properties, Map.of()));
            Assertions.assertThrows(DorisConnectorException.class, () -> inspect(Map.of(), properties));
        }
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(URL, "none"), Map.of(LITERAL, GENERATED)));
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, "none"), Map.of()));
    }

    @Test
    public void partitionLiteralsCompareJsonStructureWithoutDiscardingReaderSemantics() {
        HiveTableHandle table = handle(Map.of(LITERAL, GENERATED), Map.of());
        String pretty = "{\n  \"fields\": [{\"default\": null, \"type\": [\"null\", \"int\"],"
                + " \"name\": \"v\"}],\n  \"name\": \"r\", \"type\": \"record\"\n}";
        HmsPartitionInfo same = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(), Map.of(LITERAL, pretty));
        Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                table, same, HiveFileFormat.AVRO));
        for (String different : List.of(GENERATED.replace("[\"null\",\"int\"]", "[\"int\",\"null\"]"),
                GENERATED.replace("\"default\":null", "\"default\":7"),
                GENERATED.replace("\"int\"", "{\"type\":\"int\",\"logicalType\":\"date\"}"),
                " ".repeat(1024 * 1024) + GENERATED)) {
            HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(LITERAL, different), Map.of());
            Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                    table, partition, HiveFileFormat.AVRO));
        }
        String first = "{\"name\":\"first\",\"type\":[\"null\",\"int\"],\"default\":null}";
        String second = "{\"name\":\"second\",\"type\":[\"null\",\"string\"],\"default\":null}";
        String ordered = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + first + "," + second + "]}";
        String reversed = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + second + "," + first + "]}";
        HmsPartitionInfo reordered = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, reversed), Map.of());
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                handle(Map.of(LITERAL, ordered), Map.of()), reordered, HiveFileFormat.AVRO));
    }

    @Test
    public void hiveCommentsDoNotChangeTheReaderSchema() {
        String literal = "/* \"ignored\\x\" */\n" + GENERATED;
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, literal), Map.of()));
        HiveTableHandle table = handle(Map.of(LITERAL, literal), Map.of());
        HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, literal), Map.of());
        Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                table, partition, HiveFileFormat.AVRO));
        HmsPartitionInfo different = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, "\n" + literal), Map.of());
        Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                table, different, HiveFileFormat.AVRO));
    }

    @Test
    public void rejectsBomAndNonJsonKeywordsBeforeTableOrPartitionAdmission() {
        for (String invalid : List.of("\uFEFF" + GENERATED,
                GENERATED.replace("\"default\":null", "\"default\":NULL"),
                GENERATED.replace("\"default\":null", "\"default\":Null"),
                GENERATED.replace("\"default\":null", "\"default\":nUlL"),
                GENERATED.replace("\"fields\"", "\"custom\":TRUE,\"fields\""),
                GENERATED.replace("\"fields\"", "\"custom\":FALSE,\"fields\""))) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> inspect(Map.of(LITERAL, invalid), Map.of()));
            String valid = invalid.replace("\uFEFF", "").replace(":NULL", ":null")
                    .replace(":Null", ":null").replace(":nUlL", ":null")
                    .replace(":TRUE", ":true").replace(":FALSE", ":false");
            HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(LITERAL, invalid), Map.of());
            Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                    handle(Map.of(LITERAL, valid), Map.of()), partition, HiveFileFormat.AVRO));
        }
        String quoted = GENERATED.replace("\"fields\"", "\"doc\":\"NULL TRUE FALSE\",\"fields\"");
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, quoted), Map.of()));
    }

    @Test
    public void boundsNumericMaterializationWithoutRejectingLongFiniteFractions() {
        String fraction = GENERATED.replace("\"fields\"", "\"custom\":0." + "0".repeat(1500) + ",\"fields\"");
        Assertions.assertDoesNotThrow(() -> inspect(Map.of(LITERAL, fraction), Map.of()));
        HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, "\n" + fraction), Map.of());
        Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                handle(Map.of(LITERAL, fraction), Map.of()), partition, HiveFileFormat.AVRO));
        String integer = GENERATED.replace("\"fields\"", "\"custom\":" + "9".repeat(10000) + ",\"fields\"");
        Assertions.assertThrows(DorisConnectorException.class, () -> inspect(Map.of(LITERAL, integer), Map.of()));
    }

    @Test
    public void nonstandardJsonAndDecimalNumericKindsDoNotBecomeEquivalentReaders() {
        String tab = GENERATED.replace("\"fields\"", "\"doc\":\"a\\tb\",\"fields\"");
        String apostrophe = GENERATED.replace("\"fields\"", "\"doc\":\"a'b\",\"fields\"");
        String decimal = GENERATED.replace("\"int\"",
                "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}");
        for (Map.Entry<String, String> pair : List.of(
                Map.entry(GENERATED, GENERATED.replace('"', '\'')),
                Map.entry(GENERATED, GENERATED.replace("\"type\":", "type:")),
                Map.entry(tab, tab.replace("a\\tb", "a\tb")),
                Map.entry(apostrophe, apostrophe.replace("a'b", "a\\'b")),
                Map.entry(decimal, decimal.replace("\"precision\":9", "\"precision\":9.0")),
                Map.entry(decimal, decimal.replace("\"scale\":2", "\"scale\":2.0")))) {
            HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(LITERAL, pair.getValue()), Map.of());
            Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                    handle(Map.of(LITERAL, pair.getKey()), Map.of()), partition, HiveFileFormat.AVRO));
        }
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
    public void validatesEffectivePartitionOverridesWithoutChangingLiteralPrecedence() {
        HiveTableHandle table = handle(Map.of(LITERAL, GENERATED), Map.of());
        for (Map<String, String> serde : List.of(Map.<String, String>of(), Map.of(LITERAL, GENERATED),
                Map.of(LITERAL, GENERATED, URL, "https://example.org/ignored.avsc"))) {
            HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(), serde);
            Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                    table, partition, HiveFileFormat.AVRO));
        }
        for (Map<String, String> serde : List.of(Map.of(LITERAL, DEFAULT_SEVEN), Map.of(LITERAL, "none"))) {
            HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(), serde);
            Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                    table, partition, HiveFileFormat.AVRO));
            Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                    table, partition, HiveFileFormat.ORC));
        }
        HmsPartitionInfo url = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(), Map.of(URL, "https://example.org/reader.avsc"));
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                handle(Map.of(), Map.of()), url, HiveFileFormat.AVRO));
    }

    @Test
    public void partitionParametersTakePrecedenceOverSerdeProperties() {
        HiveTableHandle table = handle(Map.of(LITERAL, GENERATED), Map.of());
        HmsPartitionInfo defaultSeven = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, DEFAULT_SEVEN), Map.of(LITERAL, GENERATED));
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                table, defaultSeven, HiveFileFormat.AVRO));
        HmsPartitionInfo generated = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, GENERATED), Map.of(LITERAL, DEFAULT_SEVEN));
        Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                table, generated, HiveFileFormat.AVRO));
        HmsPartitionInfo disabled = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(LITERAL, "none"), Map.of(LITERAL, GENERATED));
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                table, disabled, HiveFileFormat.AVRO));
        HiveTableHandle implicit = handle(Map.of(), Map.of());
        HmsPartitionInfo url = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(URL, "https://example.org/reader.avsc"), Map.of(URL, "none"));
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                implicit, url, HiveFileFormat.AVRO));
        HmsPartitionInfo noUrl = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                null, null, null, Map.of(URL, "none"), Map.of(URL, "https://example.org/reader.avsc"));
        Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                implicit, noUrl, HiveFileFormat.AVRO));
    }

    @Test
    public void partitionLocalUrlsAreRejectedBeforeTableLiteralFallback() {
        HiveTableHandle table = handle(Map.of(LITERAL, GENERATED), Map.of());
        for (String url : List.of("https://example.org/reader.avsc", "none")) {
            for (boolean parameters : List.of(false, true)) {
                HmsPartitionInfo partition = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                        null, null, null, parameters ? Map.of(URL, url) : Map.of(),
                        parameters ? Map.of() : Map.of(URL, url));
                Assertions.assertThrows(DorisConnectorException.class, () -> HiveAvroSchemaPolicy.validatePartition(
                        table, partition, HiveFileFormat.AVRO));
            }
            HmsPartitionInfo localLiteral = new HmsPartitionInfo(List.of("one"), "s3://bucket/p=one",
                    null, null, null, Map.of(URL, url), Map.of(LITERAL, GENERATED));
            Assertions.assertDoesNotThrow(() -> HiveAvroSchemaPolicy.validatePartition(
                    table, localLiteral, HiveFileFormat.AVRO));
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
