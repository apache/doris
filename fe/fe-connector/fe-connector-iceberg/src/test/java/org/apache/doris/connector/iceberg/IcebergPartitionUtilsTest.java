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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Parity oracle for {@link IcebergPartitionUtils} — the self-contained port of legacy
 * {@code IcebergUtils.{getIdentityPartitionColumns,getIdentityPartitionInfoMap,getPartitionValues,
 * getPartitionDataJson,serializePartitionValue}} (P6.2-T03). The value matrix mirrors legacy
 * {@code serializePartitionValue} cell-by-cell; the identity/json helpers are driven against real iceberg
 * {@link PartitionData}/{@link PartitionSpec}/{@link Table} objects (no Mockito). The connector cannot
 * import fe-core, so these are reproduced byte-faithfully with two deliberate, documented deltas: the
 * timezone argument is a resolved {@link ZoneId} (not a raw String, so a non-canonical session zone cannot
 * crash), and {@code partition_data_json} is rendered via iceberg's bundled Jackson (BE re-parses the JSON
 * array — value-identical to legacy Gson).
 */
public class IcebergPartitionUtilsTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

    private static Table tableWith(Schema schema, PartitionSpec spec) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", "t"), schema, spec);
    }

    // ---- serializePartitionValue: legacy type matrix (direct, package-private) ----

    @Test
    public void serializePrimitiveValuesUseToString() {
        Assertions.assertEquals("true",
                IcebergPartitionUtils.serializePartitionValue(Types.BooleanType.get(), Boolean.TRUE, ZoneOffset.UTC));
        Assertions.assertEquals("42",
                IcebergPartitionUtils.serializePartitionValue(Types.IntegerType.get(), 42, ZoneOffset.UTC));
        Assertions.assertEquals("42",
                IcebergPartitionUtils.serializePartitionValue(Types.LongType.get(), 42L, ZoneOffset.UTC));
        Assertions.assertEquals("abc",
                IcebergPartitionUtils.serializePartitionValue(Types.StringType.get(), "abc", ZoneOffset.UTC));
        Assertions.assertEquals("1.50",
                IcebergPartitionUtils.serializePartitionValue(Types.DecimalType.of(10, 2),
                        new BigDecimal("1.50"), ZoneOffset.UTC));
    }

    @Test
    public void serializeFloatAndDoubleUseTypedToString() {
        // MUTATION: value.toString() (the primitive branch) would print "1.5" for both, but legacy routes
        // FLOAT/DOUBLE through Float/Double.toString explicitly. Pin the typed branch.
        Assertions.assertEquals("1.5",
                IcebergPartitionUtils.serializePartitionValue(Types.FloatType.get(), 1.5f, ZoneOffset.UTC));
        Assertions.assertEquals("2.5",
                IcebergPartitionUtils.serializePartitionValue(Types.DoubleType.get(), 2.5d, ZoneOffset.UTC));
    }

    @Test
    public void serializeDateAndTimeUseIso() {
        // DATE stored as days-since-epoch (Integer); 18628 = 2021-01-01.
        Assertions.assertEquals("2021-01-01",
                IcebergPartitionUtils.serializePartitionValue(Types.DateType.get(), 18628, ZoneOffset.UTC));
        // TIME stored as micros-since-midnight (Long); 3661_000_000 micros = 01:01:01.
        Assertions.assertEquals("01:01:01",
                IcebergPartitionUtils.serializePartitionValue(Types.TimeType.get(), 3661_000_000L, ZoneOffset.UTC));
    }

    @Test
    public void serializeTimestampWithoutZoneIsUtcWallClock() {
        // micros since epoch; 1609459200_000_000 = 2021-01-01T00:00:00Z. No zone adjust -> UTC wall clock.
        Assertions.assertEquals("2021-01-01T00:00:00",
                IcebergPartitionUtils.serializePartitionValue(Types.TimestampType.withoutZone(),
                        1609459200_000_000L, SHANGHAI));
    }

    @Test
    public void serializeTimestamptzShiftsToSessionZone() {
        // timestamptz (shouldAdjustToUTC) -> the stored UTC instant is rendered in the session zone.
        // 2021-01-01T00:00:00Z in Asia/Shanghai (+08) = 2021-01-01T08:00:00. MUTATION: ignoring the zone -> red.
        Assertions.assertEquals("2021-01-01T08:00:00",
                IcebergPartitionUtils.serializePartitionValue(Types.TimestampType.withZone(),
                        1609459200_000_000L, SHANGHAI));
    }

    @Test
    public void serializeNullValueReturnsNull() {
        Assertions.assertNull(
                IcebergPartitionUtils.serializePartitionValue(Types.StringType.get(), null, ZoneOffset.UTC));
        Assertions.assertNull(
                IcebergPartitionUtils.serializePartitionValue(Types.TimestampType.withZone(), null, SHANGHAI));
    }

    @Test
    public void serializeBinaryThrowsUnsupported() {
        // Legacy throws UnsupportedOperationException for BINARY/FIXED (utf8 round-trip would corrupt data);
        // callers catch it and drop the field. MUTATION: silently returning a string -> red.
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                IcebergPartitionUtils.serializePartitionValue(Types.BinaryType.get(),
                        java.nio.ByteBuffer.wrap(new byte[] {1}), ZoneOffset.UTC));
    }

    // ---- getIdentityPartitionColumns ----

    @Test
    public void identityPartitionColumnsAreIdentityOnlyLowercasedAndDeduped() {
        // Schema with an UPPERCASE column to prove lowercasing; spec mixes identity + a bucket transform.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "P", Types.IntegerType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("P")
                .identity("region")
                .bucket("id", 4)
                .build();
        Table table = tableWith(schema, spec);

        List<String> cols = IcebergPartitionUtils.getIdentityPartitionColumns(table);

        // Only the two identity columns, lowercased, in spec order; the bucket(id) transform is excluded.
        // MUTATION: including non-identity transforms -> "id_bucket"/"id" leaks -> red.
        Assertions.assertEquals(java.util.Arrays.asList("p", "region"), cols);
    }

    // ---- getIdentityPartitionInfoMap ----

    @Test
    public void identityPartitionInfoMapSkipsNonIdentityAndLowercasesKeys() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "P", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("P")
                .bucket("id", 4)
                .build();
        Table table = tableWith(schema, spec);
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, 7);          // P = 7  (identity)
        pd.set(1, 2);          // id_bucket = 2 (non-identity -> skipped)

        Map<String, String> info =
                IcebergPartitionUtils.getIdentityPartitionInfoMap(pd, spec, table, ZoneOffset.UTC);

        // Only the identity column survives, key lowercased. MUTATION: emitting id_bucket -> size 2 -> red.
        Assertions.assertEquals(Collections.singletonMap("p", "7"), info);
    }

    @Test
    public void identityPartitionInfoMapKeepsNullValue() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        Table table = tableWith(schema, spec);
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, null);       // genuine null partition value

        Map<String, String> info =
                IcebergPartitionUtils.getIdentityPartitionInfoMap(pd, spec, table, ZoneOffset.UTC);

        Assertions.assertTrue(info.containsKey("p"));
        Assertions.assertNull(info.get("p"));
    }

    // ---- getPartitionDataJson ----

    @Test
    public void partitionDataJsonIsJsonArrayOverAllFields() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.IntegerType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").identity("region").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, 5);
        pd.set(1, "cn");

        String json = IcebergPartitionUtils.getPartitionDataJson(pd, spec, ZoneOffset.UTC);

        // A JSON array of the serialized partition values, in spec order. MUTATION: dropping a field -> red.
        Assertions.assertEquals("[\"5\",\"cn\"]", json);
    }

    @Test
    public void partitionDataJsonRendersNullAsJsonNull() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, null);

        Assertions.assertEquals("[null]",
                IcebergPartitionUtils.getPartitionDataJson(pd, spec, ZoneOffset.UTC));
    }

    // ---- parsePartitionValueFromString: legacy IcebergUtils.parsePartitionValueFromString matrix (T04) ----
    // The write-direction inverse of serializePartitionValue: BE sends human-readable partition strings, the
    // connector converts them to iceberg internal partition objects for PartitionData. Mirrors legacy cell-by-cell.

    @Test
    public void parseNullValueReturnsNull() {
        Assertions.assertNull(IcebergPartitionUtils.parsePartitionValueFromString(
                null, Types.StringType.get(), ZoneOffset.UTC));
        Assertions.assertNull(IcebergPartitionUtils.parsePartitionValueFromString(
                null, Types.TimestampType.withZone(), SHANGHAI));
    }

    @Test
    public void parsePrimitiveValuesByType() {
        Assertions.assertEquals("abc", IcebergPartitionUtils.parsePartitionValueFromString(
                "abc", Types.StringType.get(), ZoneOffset.UTC));
        // INTEGER -> Integer, LONG -> Long: the typed object distinguishes int32 from int64 partitions.
        Assertions.assertEquals(Integer.valueOf(42), IcebergPartitionUtils.parsePartitionValueFromString(
                "42", Types.IntegerType.get(), ZoneOffset.UTC));
        Assertions.assertEquals(Long.valueOf(42L), IcebergPartitionUtils.parsePartitionValueFromString(
                "42", Types.LongType.get(), ZoneOffset.UTC));
        Assertions.assertEquals(Boolean.TRUE, IcebergPartitionUtils.parsePartitionValueFromString(
                "true", Types.BooleanType.get(), ZoneOffset.UTC));
        Assertions.assertEquals(new BigDecimal("1.50"), IcebergPartitionUtils.parsePartitionValueFromString(
                "1.50", Types.DecimalType.of(10, 2), ZoneOffset.UTC));
    }

    @Test
    public void parseFloatAndDoubleAreTyped() {
        // MUTATION: returning a Double for a FLOAT partition would break the iceberg PartitionData type check.
        Assertions.assertEquals(Float.valueOf(1.5f), IcebergPartitionUtils.parsePartitionValueFromString(
                "1.5", Types.FloatType.get(), ZoneOffset.UTC));
        Assertions.assertEquals(Double.valueOf(2.5d), IcebergPartitionUtils.parsePartitionValueFromString(
                "2.5", Types.DoubleType.get(), ZoneOffset.UTC));
    }

    @Test
    public void parseFloatNormalizesNanAndInfinity() {
        // Legacy normalizes Doris's "nan"/"inf"/"-inf"/"infinity" spellings to Java's NaN/Infinity tokens
        // before Float/Double.parse. MUTATION: passing the raw token straight to parseFloat -> NumberFormatException.
        Assertions.assertTrue(Float.isNaN((Float) IcebergPartitionUtils.parsePartitionValueFromString(
                "nan", Types.FloatType.get(), ZoneOffset.UTC)));
        Assertions.assertEquals(Float.POSITIVE_INFINITY, IcebergPartitionUtils.parsePartitionValueFromString(
                "inf", Types.FloatType.get(), ZoneOffset.UTC));
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, IcebergPartitionUtils.parsePartitionValueFromString(
                "-infinity", Types.DoubleType.get(), ZoneOffset.UTC));
    }

    @Test
    public void parseDateReturnsEpochDay() {
        // DATE stored as days-since-epoch (Integer); 2021-01-01 = 18628. Inverse of serializeDateAndTimeUseIso.
        Assertions.assertEquals(Integer.valueOf(18628), IcebergPartitionUtils.parsePartitionValueFromString(
                "2021-01-01", Types.DateType.get(), ZoneOffset.UTC));
    }

    @Test
    public void parseTimestampWithoutZoneIsInterpretedInUtc() {
        // No zone-adjust: the wall-clock string is interpreted in UTC -> micros. Round-trips
        // serializeTimestampWithoutZoneIsUtcWallClock (1609459200_000_000 = 2021-01-01T00:00:00Z).
        Assertions.assertEquals(1609459200_000_000L, IcebergPartitionUtils.parsePartitionValueFromString(
                "2021-01-01 00:00:00", Types.TimestampType.withoutZone(), SHANGHAI));
    }

    @Test
    public void parseTimestamptzIsInterpretedInSessionZone() {
        // timestamptz (shouldAdjustToUTC): the wall-clock string is read in the session zone, stored as UTC
        // micros. 2021-01-01T08:00:00 Asia/Shanghai (+08) = 2021-01-01T00:00:00Z. Inverse of
        // serializeTimestamptzShiftsToSessionZone. MUTATION: ignoring the zone -> 8h off -> red.
        Assertions.assertEquals(1609459200_000_000L, IcebergPartitionUtils.parsePartitionValueFromString(
                "2021-01-01 08:00:00", Types.TimestampType.withZone(), SHANGHAI));
    }

    @Test
    public void parseTimestampKeepsMicrosecondFraction() {
        // BE may send sub-second precision; the micros fraction must survive (not be truncated to seconds).
        Assertions.assertEquals(1609459200_123456L, IcebergPartitionUtils.parsePartitionValueFromString(
                "2021-01-01 00:00:00.123456", Types.TimestampType.withoutZone(), ZoneOffset.UTC));
    }

    @Test
    public void parseUnsupportedTypeThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                IcebergPartitionUtils.parsePartitionValueFromString(
                        "x", Types.BinaryType.get(), ZoneOffset.UTC));
    }

    // ---- parsePartitionValuesFromJson: legacy IcebergUtils.parsePartitionValuesFromJson (T04) ----

    @Test
    public void parseJsonRoundTripsGetPartitionDataJson() {
        // Inverse of getPartitionDataJson: ["5","cn"] -> ["5","cn"].
        Assertions.assertEquals(java.util.Arrays.asList("5", "cn"),
                IcebergPartitionUtils.parsePartitionValuesFromJson("[\"5\",\"cn\"]"));
    }

    @Test
    public void parseJsonKeepsNullElement() {
        // A genuine null partition value renders as JSON null and must parse back to a null list element.
        List<String> values = IcebergPartitionUtils.parsePartitionValuesFromJson("[null]");
        Assertions.assertEquals(1, values.size());
        Assertions.assertNull(values.get(0));
    }

    @Test
    public void parseJsonBlankReturnsEmptyList() {
        Assertions.assertTrue(IcebergPartitionUtils.parsePartitionValuesFromJson(null).isEmpty());
        Assertions.assertTrue(IcebergPartitionUtils.parsePartitionValuesFromJson("").isEmpty());
        Assertions.assertTrue(IcebergPartitionUtils.parsePartitionValuesFromJson("   ").isEmpty());
    }
}
