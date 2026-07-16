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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests {@link HmsTypeMapping} — the Hive type-string parser shared by the hms and hive
 * connectors (first test for fe-connector-hms; P3-T07 batch C baseline).
 *
 * <p>WHY: this is the SPI-clean equivalent of fe-core
 * {@code HiveMetaStoreClientHelper.hiveTypeToDorisType}. It is pure parsing logic where
 * bugs hide — nested complex types, precision/scale extraction, and option-driven
 * mappings. A wrong mapping silently mistypes every column of an HMS/Hive/Iceberg-on-HMS
 * table. These tests pin the exact ConnectorType per Hive type string and the
 * nesting-aware field splitting (Rule 9: encode the contract, not just the happy path).</p>
 */
public class HmsTypeMappingTest {

    private static ConnectorType map(String hiveType) {
        return HmsTypeMapping.toConnectorType(hiveType);
    }

    @Test
    public void testPrimitives() {
        Assertions.assertEquals(ConnectorType.of("BOOLEAN"), map("boolean"));
        Assertions.assertEquals(ConnectorType.of("TINYINT"), map("tinyint"));
        Assertions.assertEquals(ConnectorType.of("SMALLINT"), map("smallint"));
        Assertions.assertEquals(ConnectorType.of("INT"), map("int"));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), map("bigint"));
        Assertions.assertEquals(ConnectorType.of("FLOAT"), map("float"));
        Assertions.assertEquals(ConnectorType.of("DOUBLE"), map("double"));
        Assertions.assertEquals(ConnectorType.of("STRING"), map("string"));
        Assertions.assertEquals(ConnectorType.of("DATEV2"), map("date"));
    }

    @Test
    public void testTimestampUsesTimeScale() {
        // Default time scale is 6.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, -1), map("timestamp"));
        // A custom time scale flows through.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, -1),
                HmsTypeMapping.toConnectorType("timestamp", new HmsTypeMapping.Options(3, false, false)));
    }

    @Test
    public void testBinaryDefaultAndVarbinaryOption() {
        Assertions.assertEquals(ConnectorType.of("STRING"), map("binary"));
        Assertions.assertEquals(ConnectorType.of("VARBINARY"),
                HmsTypeMapping.toConnectorType("binary", new HmsTypeMapping.Options(6, true, false)));
    }

    @Test
    public void testCharAndVarcharLength() {
        Assertions.assertEquals(ConnectorType.of("CHAR", 10, -1), map("char(10)"));
        Assertions.assertEquals(ConnectorType.of("VARCHAR", 255, -1), map("varchar(255)"));
        // Missing length parameter degrades to the unparameterized type, not a crash.
        Assertions.assertEquals(ConnectorType.of("CHAR"), map("char"));
        Assertions.assertEquals(ConnectorType.of("VARCHAR"), map("varchar"));
    }

    @Test
    public void testDecimalPrecisionScaleAndDefaults() {
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 10, 2), map("decimal(10,2)"));
        // Only precision given -> default scale 0.
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 10, 0), map("decimal(10)"));
        // Bare decimal -> default precision 9, scale 0.
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 9, 0), map("decimal"));
    }

    @Test
    public void testArrayIncludingNested() {
        Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("INT")), map("array<int>"));
        Assertions.assertEquals(
                ConnectorType.arrayOf(ConnectorType.arrayOf(ConnectorType.of("STRING"))),
                map("array<array<string>>"));
    }

    @Test
    public void testMapIncludingNestedValue() {
        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")),
                map("map<string,int>"));
        // The inner comma of the nested array value must NOT be mistaken for the key/value
        // separator — this is exactly what findNextNestedField guards.
        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("INT"),
                        ConnectorType.arrayOf(ConnectorType.of("STRING"))),
                map("map<int,array<string>>"));
    }

    @Test
    public void testStructIncludingNestedFields() {
        Assertions.assertEquals(
                ConnectorType.structOf(Arrays.asList("a", "b"),
                        Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING"))),
                map("struct<a:int,b:string>"));
        Assertions.assertEquals(
                ConnectorType.structOf(Arrays.asList("x", "y"),
                        Arrays.asList(ConnectorType.arrayOf(ConnectorType.of("INT")),
                                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")))),
                map("struct<x:array<int>,y:map<string,bigint>>"));
    }

    @Test
    public void testTimestampWithLocalTimeZone() {
        // Default: mapped to DATETIMEV2.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, -1),
                map("timestamp with local time zone"));
        // With the timestamp-tz option: mapped to TIMESTAMPTZ.
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, -1),
                HmsTypeMapping.toConnectorType("timestamp with local time zone",
                        new HmsTypeMapping.Options(6, false, true)));
    }

    @Test
    public void testUnsupportedTypeIsUnsupportedNotCrash() {
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), map("interval_day_time"));
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), map("void"));
    }

    @Test
    public void testCaseInsensitiveAndLowercasesNestedNames() {
        Assertions.assertEquals(ConnectorType.of("INT"), map("INT"));
        Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("STRING")), map("ARRAY<STRING>"));
        // The whole type string is lowercased first, so struct field names are lowercased too.
        Assertions.assertEquals(
                ConnectorType.structOf(Arrays.asList("name"), Arrays.asList(ConnectorType.of("INT"))),
                map("STRUCT<Name:INT>"));
    }

    @Test
    public void testFindNextNestedFieldRespectsNesting() {
        // Top-level comma found at the right index...
        Assertions.assertEquals(3, HmsTypeMapping.findNextNestedField("int,string"));
        Assertions.assertEquals(10, HmsTypeMapping.findNextNestedField("array<int>,string"));
        // ...and a comma nested inside <> is skipped (returns the next top-level comma).
        Assertions.assertEquals(15, HmsTypeMapping.findNextNestedField("map<string,int>,extra"));
        // No top-level comma -> returns the length.
        Assertions.assertEquals(3, HmsTypeMapping.findNextNestedField("int"));
    }

    // ==================== reverse mapping: ConnectorType -> Hive type string ====================
    //
    // WHY: toHiveTypeString is the CREATE TABLE direction, the SPI-clean equivalent of fe-core
    // HiveMetaStoreClientHelper.dorisTypeToHiveType. Its input type names are Doris PrimitiveType
    // names (what ConnectorColumnConverter.toConnectorType emits via PrimitiveType.toString()). A
    // wrong reverse mapping silently mistypes every column of a table Doris creates in Hive, so
    // these pin the exact Hive string per Doris type — especially the ones that intentionally
    // collapse (VARCHAR->string) or drop parameters (timestamp), and the unsupported ones that
    // must throw rather than emit a bogus type.

    private static String hive(ConnectorType type) {
        return HmsTypeMapping.toHiveTypeString(type);
    }

    @Test
    public void testToHiveTypeStringPrimitives() {
        Assertions.assertEquals("boolean", hive(ConnectorType.of("BOOLEAN")));
        Assertions.assertEquals("tinyint", hive(ConnectorType.of("TINYINT")));
        Assertions.assertEquals("smallint", hive(ConnectorType.of("SMALLINT")));
        Assertions.assertEquals("int", hive(ConnectorType.of("INT")));
        Assertions.assertEquals("bigint", hive(ConnectorType.of("BIGINT")));
        Assertions.assertEquals("float", hive(ConnectorType.of("FLOAT")));
        Assertions.assertEquals("double", hive(ConnectorType.of("DOUBLE")));
        Assertions.assertEquals("string", hive(ConnectorType.of("STRING")));
    }

    @Test
    public void testToHiveTypeStringDateAndTimestampVariants() {
        // Both the legacy and V2 date/datetime primitives collapse to Hive date/timestamp.
        Assertions.assertEquals("date", hive(ConnectorType.of("DATE")));
        Assertions.assertEquals("date", hive(ConnectorType.of("DATEV2")));
        Assertions.assertEquals("timestamp", hive(ConnectorType.of("DATETIME")));
        // The datetime scale carried on the ConnectorType is intentionally dropped (Hive timestamp has none).
        Assertions.assertEquals("timestamp", hive(ConnectorType.of("DATETIMEV2", 6, -1)));
    }

    @Test
    public void testToHiveTypeStringCharVarcharString() {
        // CHAR carries its length in the precision field (create-request encoding).
        Assertions.assertEquals("char(10)", hive(ConnectorType.of("CHAR", 10, 0)));
        // VARCHAR intentionally maps to Hive string (parity with legacy dorisTypeToHiveType).
        Assertions.assertEquals("string", hive(ConnectorType.of("VARCHAR", 255, 0)));
        Assertions.assertEquals("string", hive(ConnectorType.of("STRING")));
    }

    @Test
    public void testToHiveTypeStringDecimalVariantsAndDefault() {
        // Every Doris decimal storage width maps to Hive decimal(p,s).
        Assertions.assertEquals("decimal(10,2)", hive(ConnectorType.of("DECIMAL64", 10, 2)));
        Assertions.assertEquals("decimal(38,10)", hive(ConnectorType.of("DECIMAL128", 38, 10)));
        Assertions.assertEquals("decimal(9,0)", hive(ConnectorType.of("DECIMALV2", 9, 0)));
        Assertions.assertEquals("decimal(76,0)", hive(ConnectorType.of("DECIMAL256", 76, 0)));
        // A read-origin DECIMALV3 name is accepted too.
        Assertions.assertEquals("decimal(5,3)", hive(ConnectorType.of("DECIMALV3", 5, 3)));
        // Precision 0 falls back to the default precision 9 (parity with legacy).
        Assertions.assertEquals("decimal(9,0)", hive(ConnectorType.of("DECIMAL32", 0, 0)));
    }

    @Test
    public void testToHiveTypeStringComplexIncludingNested() {
        Assertions.assertEquals("array<int>", hive(ConnectorType.arrayOf(ConnectorType.of("INT"))));
        Assertions.assertEquals("map<string,bigint>",
                hive(ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("BIGINT"))));
        Assertions.assertEquals("struct<a:int,b:string>",
                hive(ConnectorType.structOf(Arrays.asList("a", "b"),
                        Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")))));
        // Nested: an array of structs of a map.
        ConnectorType nested = ConnectorType.arrayOf(
                ConnectorType.structOf(Arrays.asList("m"),
                        Arrays.asList(ConnectorType.mapOf(ConnectorType.of("STRING"),
                                ConnectorType.of("INT")))));
        Assertions.assertEquals("array<struct<m:map<string,int>>>", hive(nested));
    }

    @Test
    public void testToHiveTypeStringUnsupportedThrows() {
        // Types Hive tables cannot represent must throw, not emit a bogus type string.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> hive(ConnectorType.of("LARGEINT")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> hive(ConnectorType.of("IPV4")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> hive(ConnectorType.of("JSONB")));
    }
}
