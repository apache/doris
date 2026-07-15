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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    public void identityPartitionColumnsAreIdentityOnlyCasePreservedAndDeduped() {
        // Schema with an UPPERCASE column to prove case preservation; spec mixes identity + a bucket transform.
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

        // Only the two identity columns, CASE-PRESERVED (#65094 read-path alignment), in spec order; the
        // bucket(id) transform is excluded. MUTATION: including non-identity transforms -> "id_bucket"/"id"
        // leaks -> red. MUTATION: re-lowercasing -> "p" != "P" -> red.
        Assertions.assertEquals(java.util.Arrays.asList("P", "region"), cols);
    }

    // ---- getIdentityPartitionInfoMap ----

    @Test
    public void identityPartitionInfoMapSkipsNonIdentityAndPreservesKeyCase() {
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

        // Only the identity column survives, key CASE-PRESERVED (#65094 read-path alignment). MUTATION:
        // emitting id_bucket -> size 2 -> red. MUTATION: re-lowercasing the key -> "p" != "P" -> red.
        Assertions.assertEquals(Collections.singletonMap("P", "7"), info);
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

    // ---- getPartitionDataObjectJson ($position_deletes, upstream #65135) ----

    /** The metadata table's `partition` struct fields, i.e. what {@code outputPartitionFields} carries. */
    private static List<Types.NestedField> outputFields(Types.NestedField... fields) {
        return Arrays.asList(fields);
    }

    @Test
    public void partitionDataObjectJsonIsTypeNativeObjectNotStringArray() {
        // WHY: the two renderers travel in the SAME thrift field (TIcebergFileDesc.partition_data_json) on
        // different paths and are NOT interchangeable. BE does not parse this as JSON — it feeds it to the
        // STRUCT text serde, which requires a leading '{' and matches keys by name; and
        // DataTypeNullableSerDe::from_string SWALLOWS a parse failure into a NULL partition while returning
        // OK. So handing it getPartitionDataJson's `["10"]` array would be silent wrong data, never an error.
        // The int must also stay UNQUOTED (the regression suite's pd_int_partitioned pins "p":10, not "p":"10").
        // MUTATION: delegating to getPartitionDataJson -> array -> red; quoting the int -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, 10);
        Types.NestedField out = Types.NestedField.optional(
                spec.fields().get(0).fieldId(), "p", Types.IntegerType.get());

        String json = IcebergPartitionUtils.getPartitionDataObjectJson(
                pd, spec, outputFields(out), false, ZoneOffset.UTC);

        Assertions.assertEquals("{\"p\":10}", json);
    }

    @Test
    public void partitionDataObjectJsonKeysByOutputFieldIdNotSpecPosition() {
        // WHY: the $position_deletes metadata table REASSIGNS partition field ids and rebuilds each spec via
        // BaseMetadataTable.transformSpec, keeping the field ID but taking the OUTPUT (metadata-table) name.
        // A rename therefore shows up as: same id, new name. Rendering by spec position (or by the writing
        // spec's name) would emit the stale key, BE's struct serde would find no matching field, and the
        // column would come back NULL — silently. This is the FE half of the suite's pd_partition_rename case.
        // MUTATION: keying the JSON off the writing spec's field name -> {"p":10} -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, 10);
        // Same field id, renamed to p2 on the metadata table.
        Types.NestedField renamed = Types.NestedField.optional(
                spec.fields().get(0).fieldId(), "p2", Types.IntegerType.get());

        Assertions.assertEquals("{\"p2\":10}", IcebergPartitionUtils.getPartitionDataObjectJson(
                pd, spec, outputFields(renamed), false, ZoneOffset.UTC));
    }

    @Test
    public void partitionDataObjectJsonRendersFieldMissingFromWritingSpecAsNull() {
        // WHY: under partition evolution a delete file's own spec is a SUBSET of the metadata table's union
        // partition type. The absent field must materialize as NULL, not shift the remaining values over.
        // The suite pins exactly this: the old-spec row's JSON contains ":null", the new-spec row's does not.
        // MUTATION: skipping unmatched output fields entirely and letting BE positionally bind the rest -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, 10);
        Types.NestedField present = Types.NestedField.optional(
                spec.fields().get(0).fieldId(), "p", Types.IntegerType.get());
        // A field only the LATER spec has (id 9999 is in no writing spec here).
        Types.NestedField evolved = Types.NestedField.optional(9999, "id_bucket", Types.IntegerType.get());

        String json = IcebergPartitionUtils.getPartitionDataObjectJson(
                pd, spec, outputFields(present, evolved), false, ZoneOffset.UTC);

        Assertions.assertEquals("{\"p\":10,\"id_bucket\":null}", json);
    }

    @Test
    public void partitionDataObjectJsonKeepsDecimalScaleExact() {
        // WHY: stock Jackson's JsonNodeFactory has bigDecimalExact=false, so valueToTree(new BigDecimal("10"))
        // renders 1E+1 and BigDecimal("1.50") renders 1.5 — the first is a JSON *syntax* change, not just lost
        // scale. Legacy fe-core rendered these through Gson, which is scale-exact. Verified empirically
        // against gson 2.10.1 / jackson 2.16.0 (design doc T0.1). Whether BE's decimal text parser even
        // accepts 1E+1 is untested — the point is to never emit it.
        // MUTATION: using the stock JsonUtil.mapper() instead of the withExactBigDecimals copy -> red on both.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.DecimalType.of(10, 2)));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, new BigDecimal("1.50"));
        Types.NestedField out = Types.NestedField.optional(
                spec.fields().get(0).fieldId(), "p", Types.DecimalType.of(10, 2));

        Assertions.assertEquals("{\"p\":1.50}", IcebergPartitionUtils.getPartitionDataObjectJson(
                pd, spec, outputFields(out), false, ZoneOffset.UTC));

        PartitionData integral = new PartitionData(spec.partitionType());
        integral.set(0, new BigDecimal("10"));
        Assertions.assertEquals("{\"p\":10}", IcebergPartitionUtils.getPartitionDataObjectJson(
                        integral, spec, outputFields(out), false, ZoneOffset.UTC),
                "an integral decimal must not degrade to scientific notation (1E+1)");
    }

    @Test
    public void partitionDataObjectJsonRejectsBinaryAndFixedPartitionValues() {
        // WHY: this text transport cannot round-trip raw bytes. Legacy fails loud rather than let BE
        // materialize a corrupted or silently-NULL partition value — and silent is exactly what would happen,
        // since the struct serde swallows parse failures. MUTATION: emitting a utf8 rendering (or letting
        // getPartitionValues' null-on-unsupported through) -> no throw -> red.
        for (Types.NestedField field : outputFields(
                Types.NestedField.required(2, "p", Types.BinaryType.get()),
                Types.NestedField.required(2, "p", Types.FixedType.ofLength(2)))) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()), field);
            PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
            PartitionData pd = new PartitionData(spec.partitionType());
            pd.set(0, ByteBuffer.wrap(new byte[] {0, (byte) 0xff}));
            Types.NestedField out = Types.NestedField.optional(
                    spec.fields().get(0).fieldId(), "p", field.type());

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> IcebergPartitionUtils.getPartitionDataObjectJson(
                            pd, spec, outputFields(out), false, ZoneOffset.UTC));
            Assertions.assertTrue(e.getMessage().contains("partition field 'p'"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains(field.type().toString()), e.getMessage());
        }
    }

    @Test
    public void partitionDataObjectJsonRejectsUuidOnlyWhenVarbinaryMappingIsOn() {
        // WHY: enable.mapping.varbinary makes UUID a VARBINARY column, which this text transport cannot carry
        // either — but with the flag OFF a UUID is a plain string and MUST still work. So the guard is
        // conditional, and a test that only checks the throw would not catch over-rejection.
        // MUTATION: rejecting UUID unconditionally -> the flag-off case -> red; never rejecting it -> the
        // flag-on case -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "p", Types.UUIDType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        UUID uuid = UUID.fromString("00000000-0000-0000-0000-00000000002a");
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, uuid);
        Types.NestedField out = Types.NestedField.optional(
                spec.fields().get(0).fieldId(), "p", Types.UUIDType.get());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergPartitionUtils.getPartitionDataObjectJson(
                        pd, spec, outputFields(out), true, ZoneOffset.UTC));
        Assertions.assertTrue(e.getMessage().contains("partition field 'p'"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("uuid"), e.getMessage());

        Assertions.assertEquals("{\"p\":\"" + uuid + "\"}", IcebergPartitionUtils.getPartitionDataObjectJson(
                        pd, spec, outputFields(out), false, ZoneOffset.UTC),
                "with varbinary mapping off a UUID partition is a plain string and must still render");
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

    // ─────────── B-2: MTMV RANGE partition view — transform math (buildRange), port of getPartitionRange ───────────
    // The transform value is the iceberg partition ordinal: HOUR=hours-since-epoch, DAY=days, MONTH=months, YEAR=years.
    // Bounds are pre-rendered [lower, upper); a TIMESTAMP source -> "yyyy-MM-dd HH:mm:ss", a DATE source -> "yyyy-MM-dd".

    @Test
    public void buildRangeDayWithTimestampSourceRendersDatetimeBounds() {
        // day ordinal 100 = 1970-01-01 + 100 days = 1970-04-11; upper = +1 day. Source TIMESTAMP -> datetime form.
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "ts_day=100", "100", "day", Types.TimestampType.withoutZone(), 5L, 99L);
        Assertions.assertEquals(Collections.singletonList("1970-04-11 00:00:00"), rb.getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1970-04-12 00:00:00"), rb.getUpperBound());
    }

    @Test
    public void buildRangeDayWithDateSourceRendersDateBounds() {
        // Same day ordinal, but a DATE source -> "yyyy-MM-dd". MUTATION: a single fixed formatter would render
        // the datetime form here (or the date form in the timestamp test) -> red.
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "d_day=100", "100", "day", Types.DateType.get(), 5L, 99L);
        Assertions.assertEquals(Collections.singletonList("1970-04-11"), rb.getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1970-04-12"), rb.getUpperBound());
    }

    @Test
    public void buildRangeHourTruncatesToHourBoundary() {
        // hour ordinal 5 = 1970-01-01 05:00:00; upper = +1 hour. HOUR's source is always a timestamp.
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "ts_hour=5", "5", "hour", Types.TimestampType.withoutZone(), 1L, 1L);
        Assertions.assertEquals(Collections.singletonList("1970-01-01 05:00:00"), rb.getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1970-01-01 06:00:00"), rb.getUpperBound());
    }

    @Test
    public void buildRangeMonthTruncatesToMonthBoundary() {
        // month ordinal 2 = 1970-03-01; upper = +1 month = 1970-04-01.
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "ts_month=2", "2", "month", Types.TimestampType.withoutZone(), 1L, 1L);
        Assertions.assertEquals(Collections.singletonList("1970-03-01 00:00:00"), rb.getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1970-04-01 00:00:00"), rb.getUpperBound());
    }

    @Test
    public void buildRangeYearTruncatesToYearBoundary() {
        // year ordinal 2 = 1972; upper = 1973. DATE source -> "yyyy-MM-dd".
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "d_year=2", "2", "year", Types.DateType.get(), 1L, 1L);
        Assertions.assertEquals(Collections.singletonList("1972-01-01"), rb.getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1973-01-01"), rb.getUpperBound());
    }

    @Test
    public void buildRangeNullValueEmitsSuccessorSignal() {
        // A NULL partition value -> lower "0000-01-01" + EMPTY upper (the generic model derives lower.successor()).
        // MUTATION: rendering a concrete upper here would not match master's nullLowKey.successor() per scale.
        IcebergPartitionUtils.RangeBuild rb = IcebergPartitionUtils.buildRange(
                "ts_day=null", null, "day", Types.TimestampType.withoutZone(), 1L, 1L);
        Assertions.assertEquals(Collections.singletonList("0000-01-01"), rb.getLowerBound());
        Assertions.assertTrue(rb.getUpperBound().isEmpty());
    }

    @Test
    public void buildRangeUnsupportedTransformThrows() {
        Assertions.assertThrows(RuntimeException.class, () -> IcebergPartitionUtils.buildRange(
                "id_bucket=2", "2", "bucket[4]", Types.IntegerType.get(), 1L, 1L));
    }

    // ─────────── B-2: overlap merge + snapshot-id resolution (port mergeOverlapPartitions / getLatestSnapshotId) ───────────

    private static IcebergPartitionUtils.RangeBuild rangeBuild(String name, LocalDateTime lower, LocalDateTime upper,
            long lastUpdateTime, long lastSnapshotId) {
        return new IcebergPartitionUtils.RangeBuild(name, lower, upper,
                Collections.singletonList(lower.toString()), Collections.singletonList(upper.toString()),
                lastUpdateTime, lastSnapshotId);
    }

    @Test
    public void mergeEnclosedDayIntoEnclosingMonth() {
        // MONTH [1970-03-01, 1970-04-01) encloses DAY [1970-03-15, 1970-03-16): the day is merged away and the
        // month becomes the single surviving Doris partition (parity master mergeOverlapPartitions on aligned ranges).
        IcebergPartitionUtils.RangeBuild month = rangeBuild("ts_month=2",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 4, 1, 0, 0), 10L, 100L);
        IcebergPartitionUtils.RangeBuild day = rangeBuild("ts_day=73",
                LocalDateTime.of(1970, 3, 15, 0, 0), LocalDateTime.of(1970, 3, 16, 0, 0), 20L, 200L);

        Set<String> survivors = new LinkedHashSet<>(Arrays.asList("ts_month=2", "ts_day=73"));
        Map<String, Set<String>> mergeMap = IcebergPartitionUtils.mergeOverlapPartitions(
                Arrays.asList(month, day), survivors);

        Assertions.assertEquals(Collections.singleton("ts_month=2"), survivors);
        Assertions.assertEquals(new HashSet<>(Arrays.asList("ts_month=2", "ts_day=73")),
                mergeMap.get("ts_month=2"));
    }

    @Test
    public void nonOverlappingPartitionsAreNotMerged() {
        // Two disjoint days: neither encloses the other, both survive, no merge map entry. MUTATION: an encloses
        // that returns true for disjoint ranges would wrongly drop one.
        IcebergPartitionUtils.RangeBuild d1 = rangeBuild("ts_day=1",
                LocalDateTime.of(1970, 1, 2, 0, 0), LocalDateTime.of(1970, 1, 3, 0, 0), 10L, 100L);
        IcebergPartitionUtils.RangeBuild d2 = rangeBuild("ts_day=2",
                LocalDateTime.of(1970, 1, 3, 0, 0), LocalDateTime.of(1970, 1, 4, 0, 0), 20L, 200L);

        Set<String> survivors = new LinkedHashSet<>(Arrays.asList("ts_day=1", "ts_day=2"));
        Map<String, Set<String>> mergeMap = IcebergPartitionUtils.mergeOverlapPartitions(
                Arrays.asList(d1, d2), survivors);

        Assertions.assertEquals(new HashSet<>(Arrays.asList("ts_day=1", "ts_day=2")), survivors);
        Assertions.assertTrue(mergeMap.isEmpty());
    }

    @Test
    public void mergeTieBreaksEqualLowerByLargerUpperFirst() {
        // SAME lower bound (1970-03-01), different uppers: MONTH [03-01,04-01) and first-of-month DAY
        // [03-01,03-02). The comparator's tie-break (equal lower -> LARGER upper first) must place the month
        // first so it encloses the day -> one survivor. Inputs are passed day-first to prove the COMPARATOR (not
        // input order) decides. MUTATION: an ascending tie-break sorts the day first, encloses() is false, both
        // survive (2 where master yields 1) -> red.
        IcebergPartitionUtils.RangeBuild month = rangeBuild("ts_month=2",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 4, 1, 0, 0), 10L, 100L);
        IcebergPartitionUtils.RangeBuild firstDay = rangeBuild("ts_day=59",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 3, 2, 0, 0), 20L, 200L);

        Set<String> survivors = new LinkedHashSet<>(Arrays.asList("ts_month=2", "ts_day=59"));
        Map<String, Set<String>> mergeMap = IcebergPartitionUtils.mergeOverlapPartitions(
                Arrays.asList(firstDay, month), survivors);

        Assertions.assertEquals(Collections.singleton("ts_month=2"), survivors);
        Assertions.assertEquals(new HashSet<>(Arrays.asList("ts_month=2", "ts_day=59")),
                mergeMap.get("ts_month=2"));
    }

    @Test
    public void mergeIdenticalNameSelfEnclosesSoCallerMustDedupeFirst() {
        // Two entries with the SAME name + byte-identical range: encloses() is true on equal endpoints, so the
        // merge removes the (shared) secondKey -> the name vanishes from survivors. This is exactly WHY
        // buildMvccPartitionView dedupes the raw rows into allByName (last-wins) BEFORE calling this — master
        // keys nameToPartitionItem by name (loadPartitionInfo), so a name can never enclose its own twin.
        // Documents the invariant the merge-input-dedup fix restores.
        IcebergPartitionUtils.RangeBuild a = rangeBuild("ts_day=100",
                LocalDateTime.of(1970, 4, 11, 0, 0), LocalDateTime.of(1970, 4, 12, 0, 0), 10L, 100L);
        IcebergPartitionUtils.RangeBuild b = rangeBuild("ts_day=100",
                LocalDateTime.of(1970, 4, 11, 0, 0), LocalDateTime.of(1970, 4, 12, 0, 0), 20L, 200L);

        Set<String> survivors = new LinkedHashSet<>(Collections.singletonList("ts_day=100"));
        IcebergPartitionUtils.mergeOverlapPartitions(Arrays.asList(a, b), survivors);

        Assertions.assertTrue(survivors.isEmpty(),
                "identical-name entries self-enclose; buildMvccPartitionView must dedupe by name before merging");
    }

    @Test
    public void latestSnapshotIdForMergedPicksMostRecentUpdate() {
        // The merged month's freshness is the snapshot id of the most-recently-updated member (the day, t=20>10).
        IcebergPartitionUtils.RangeBuild month = rangeBuild("ts_month=2",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 4, 1, 0, 0), 10L, 100L);
        IcebergPartitionUtils.RangeBuild day = rangeBuild("ts_day=73",
                LocalDateTime.of(1970, 3, 15, 0, 0), LocalDateTime.of(1970, 3, 16, 0, 0), 20L, 200L);
        Map<String, IcebergPartitionUtils.RangeBuild> all = new HashMap<>();
        all.put("ts_month=2", month);
        all.put("ts_day=73", day);
        Map<String, Set<String>> mergeMap = Collections.singletonMap(
                "ts_month=2", new HashSet<>(Arrays.asList("ts_month=2", "ts_day=73")));

        Assertions.assertEquals(200L, IcebergPartitionUtils.latestSnapshotId("ts_month=2", mergeMap, all));
    }

    @Test
    public void latestSnapshotIdForStandalonePartitionIsOwnSnapshot() {
        // A partition that encloses nothing (absent from the merge map) reports its OWN last snapshot id.
        IcebergPartitionUtils.RangeBuild day = rangeBuild("ts_day=1",
                LocalDateTime.of(1970, 1, 2, 0, 0), LocalDateTime.of(1970, 1, 3, 0, 0), 10L, 77L);
        Map<String, IcebergPartitionUtils.RangeBuild> all =
                Collections.singletonMap("ts_day=1", day);

        Assertions.assertEquals(77L,
                IcebergPartitionUtils.latestSnapshotId("ts_day=1", Collections.emptyMap(), all));
    }

    @Test
    public void latestSnapshotIdSkipsInvalidUpdateTimesAndAllInvalidReturnsMinusOne() {
        IcebergPartitionUtils.RangeBuild month = rangeBuild("ts_month=2",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 4, 1, 0, 0), 10L, 100L);
        // day has an UNKNOWN (<=0) update time -> skipped; the month (t=10) wins.
        IcebergPartitionUtils.RangeBuild day = rangeBuild("ts_day=73",
                LocalDateTime.of(1970, 3, 15, 0, 0), LocalDateTime.of(1970, 3, 16, 0, 0), -1L, 200L);
        Map<String, IcebergPartitionUtils.RangeBuild> all = new HashMap<>();
        all.put("ts_month=2", month);
        all.put("ts_day=73", day);
        Map<String, Set<String>> mergeMap = Collections.singletonMap(
                "ts_month=2", new HashSet<>(Arrays.asList("ts_month=2", "ts_day=73")));
        Assertions.assertEquals(100L, IcebergPartitionUtils.latestSnapshotId("ts_month=2", mergeMap, all));

        // Both members have invalid update times -> no snapshot id resolvable (-1); the caller then falls back
        // to the table snapshot id.
        IcebergPartitionUtils.RangeBuild month0 = rangeBuild("ts_month=2",
                LocalDateTime.of(1970, 3, 1, 0, 0), LocalDateTime.of(1970, 4, 1, 0, 0), 0L, 100L);
        Map<String, IcebergPartitionUtils.RangeBuild> allInvalid = new HashMap<>();
        allInvalid.put("ts_month=2", month0);
        allInvalid.put("ts_day=73", day);
        Assertions.assertEquals(-1L, IcebergPartitionUtils.latestSnapshotId("ts_month=2", mergeMap, allInvalid));
    }

    // ─────────── B-2: eligibility gate (isValidRelatedTable), port of IcebergExternalTable.isValidRelatedTable ───────────

    private static final Schema RELATED_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "ts2", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "region", Types.StringType.get()));

    @Test
    public void validRelatedTableSingleTimeTransform() {
        Table table = tableWith(RELATED_SCHEMA, PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build());
        Assertions.assertTrue(IcebergPartitionUtils.isValidRelatedTable(table));
    }

    @Test
    public void invalidRelatedTableMultipleFields() {
        // Two partition fields -> not a valid related table (master supports a single field only).
        Table table = tableWith(RELATED_SCHEMA,
                PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").identity("region").build());
        Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(table));
    }

    @Test
    public void invalidRelatedTableNonTimeTransform() {
        // A non year/month/day/hour transform (bucket) -> invalid. MUTATION: accepting any transform -> red.
        Table table = tableWith(RELATED_SCHEMA, PartitionSpec.builderFor(RELATED_SCHEMA).bucket("id", 4).build());
        Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(table));
    }

    @Test
    public void invalidRelatedTableUnpartitioned() {
        Table table = tableWith(RELATED_SCHEMA, PartitionSpec.unpartitioned());
        Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(table));
    }

    @Test
    public void invalidRelatedTableEvolutionRetainsVoidFieldSoMultiField() {
        // Partition evolution that moves the source (ts -> ts2) retains the removed field as a VOID transform, so
        // the new spec has 2 fields and the table is not a valid related table. This documents that iceberg never
        // produces "two single-field specs on different sources" — master's allFields.size()==1 source-stability
        // check is a faithful but practically-unreachable defensive guard (the field-count check fires first).
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t"), RELATED_SCHEMA,
                PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build());
        table.updateSpec().removeField("ts_day")
                .addField(org.apache.iceberg.expressions.Expressions.day("ts2")).commit();
        Table evolved = catalog.loadTable(TableIdentifier.of("db1", "t"));
        Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(evolved));
    }

    // ─────────── B-2: end-to-end PARTITIONS-metadata scan (buildMvccPartitionView / listPartitionNames) ───────────
    // Real InMemoryCatalog tables with appended partitioned data files; the PARTITIONS metadata table is scanned
    // exactly as in production (no Mockito). This covers the gate -> RANGE/UNPARTITIONED style decision, the scan,
    // and the per-partition freshness wiring on top of the unit-tested math/merge above.

    // partitionPaths use the iceberg human-readable form (a DAY transform takes the DATE string, e.g.
    // "ts_day=1970-04-11"); the PARTITIONS metadata table stores/returns the integer ordinal (100), which is
    // what the connector reads back into the partition name "ts_day=100".
    private static Table dayPartitionedTable(PartitionSpec spec, String... partitionPaths) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t"), RELATED_SCHEMA, spec);
        org.apache.iceberg.AppendFiles append = table.newAppend();
        int i = 0;
        for (String partitionPath : partitionPaths) {
            append.appendFile(DataFiles.builder(spec)
                    .withPath("s3://b/db1/t/f" + (i++) + ".parquet")
                    .withFileSizeInBytes(100)
                    .withRecordCount(1)
                    .withPartitionPath(partitionPath)
                    .withFormat(FileFormat.PARQUET)
                    .build());
        }
        append.commit();
        return catalog.loadTable(TableIdentifier.of("db1", "t"));
    }

    @Test
    public void buildMvccPartitionViewEnumeratesRangePartitions() {
        PartitionSpec spec = PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build();
        Table table = dayPartitionedTable(spec, "ts_day=1970-04-11", "ts_day=1970-07-20");
        long snapshotId = table.currentSnapshot().snapshotId();

        ConnectorMvccPartitionView view = IcebergPartitionUtils.buildMvccPartitionView(table, -1L);

        Assertions.assertEquals(ConnectorMvccPartitionView.Style.RANGE, view.getStyle());
        Assertions.assertEquals(ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID, view.getFreshness());
        List<ConnectorMvccPartition> parts = view.getPartitions();
        Assertions.assertEquals(2, parts.size());
        // Sorted by name: "ts_day=100" < "ts_day=200".
        Assertions.assertEquals(Arrays.asList("ts_day=100", "ts_day=200"),
                parts.stream().map(ConnectorMvccPartition::getName).collect(Collectors.toList()));
        // The day=100 partition's pre-rendered datetime bounds match the unit-tested transform math.
        Assertions.assertEquals(Collections.singletonList("1970-04-11 00:00:00"), parts.get(0).getLowerBound());
        Assertions.assertEquals(Collections.singletonList("1970-04-12 00:00:00"), parts.get(0).getUpperBound());
        // Freshness is a resolved iceberg snapshot id (the single commit's snapshot, whether read directly from
        // last_updated_snapshot_id or fallen back to the table snapshot id). MUTATION: a 0/-1 sentinel -> red.
        for (ConnectorMvccPartition part : parts) {
            Assertions.assertEquals(snapshotId, part.getFreshnessValue());
        }
        // The view also carries the table's newest-update-time (max last_updated_at), the MONOTONIC marker the
        // generic model answers the dictionary auto-refresh probe with (snapshot ids are non-monotonic). A real
        // committed table has a positive value. MUTATION: mapping lastUpdateTime->0 (or orElse over no rows) -> red.
        Assertions.assertTrue(view.getNewestUpdateTimeMillis() > 0,
                "a committed RANGE table must report a positive newest-update-time for dictionary refresh");
    }

    @Test
    public void buildMvccPartitionViewResolvesPerPartitionSnapshotId() {
        // Two SEPARATE commits: ts_day=100 lands in snapshot S1, ts_day=200 in S2. Each partition's freshness is
        // the snapshot that last updated IT (S1 vs S2), NOT the table's current snapshot (S2 for both). This pins
        // the per-partition snapshot-id resolution AND the `latest > 0 ? latest : tableSnapshotId` branch: a
        // fallback-to-table mutation would make the first partition report S2 instead of its own S1.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        PartitionSpec spec = PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        Table table = catalog.createTable(id, RELATED_SCHEMA, spec);
        table.newAppend().appendFile(DataFiles.builder(spec).withPath("s3://b/db1/t/f0.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).withPartitionPath("ts_day=1970-04-11")
                .withFormat(FileFormat.PARQUET).build()).commit();
        long s1 = catalog.loadTable(id).currentSnapshot().snapshotId();
        table.newAppend().appendFile(DataFiles.builder(spec).withPath("s3://b/db1/t/f1.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).withPartitionPath("ts_day=1970-07-20")
                .withFormat(FileFormat.PARQUET).build()).commit();
        long s2 = catalog.loadTable(id).currentSnapshot().snapshotId();
        Assertions.assertNotEquals(s1, s2, "the two appends must create distinct snapshots");

        List<ConnectorMvccPartition> parts = IcebergPartitionUtils.buildMvccPartitionView(
                catalog.loadTable(id), -1L).getPartitions();
        Assertions.assertEquals(2, parts.size());
        // Sorted by name: ts_day=100 (committed in S1), ts_day=200 (committed in S2).
        Assertions.assertEquals("ts_day=100", parts.get(0).getName());
        Assertions.assertEquals(s1, parts.get(0).getFreshnessValue());
        Assertions.assertEquals("ts_day=200", parts.get(1).getName());
        Assertions.assertEquals(s2, parts.get(1).getFreshnessValue());

        // pinnedSnapshotId = S1 enumerates AT the older snapshot: only ts_day=100 existed then. This pins the
        // partition set + freshness to the query's MVCC snapshot (so the generic model keeps them consistent
        // with the data-scan pin) instead of always reading the live latest. MUTATION: ignoring the pin and
        // using currentSnapshot() -> 2 partitions -> red.
        List<ConnectorMvccPartition> atS1 = IcebergPartitionUtils.buildMvccPartitionView(
                catalog.loadTable(id), s1).getPartitions();
        Assertions.assertEquals(Collections.singletonList("ts_day=100"),
                atS1.stream().map(ConnectorMvccPartition::getName).collect(Collectors.toList()));
        Assertions.assertEquals(s1, atS1.get(0).getFreshnessValue());

        // newest-update-time is max() (NOT min()) over the two partitions' last_updated_at. p2 was committed in
        // the later snapshot S2, so the full-table marker tracks S2 and must STRICTLY EXCEED the S1-only value
        // whenever the two commits landed in different clock ticks (a min() would equal the S1-only value). This
        // relationally kills the max->min mutation in practice without a flaky absolute-timestamp assertion.
        long s1ts = catalog.loadTable(id).snapshot(s1).timestampMillis();
        long s2ts = catalog.loadTable(id).snapshot(s2).timestampMillis();
        long fullNewest = IcebergPartitionUtils.buildMvccPartitionView(catalog.loadTable(id), -1L)
                .getNewestUpdateTimeMillis();
        long s1OnlyNewest = IcebergPartitionUtils.buildMvccPartitionView(catalog.loadTable(id), s1)
                .getNewestUpdateTimeMillis();
        if (s2ts > s1ts) {
            Assertions.assertTrue(fullNewest > s1OnlyNewest,
                    "newest-update must be max (track the later snapshot S2), not min; full=" + fullNewest
                    + " s1Only=" + s1OnlyNewest);
        } else {
            Assertions.assertTrue(fullNewest >= s1OnlyNewest,
                    "newest-update must be monotonic (the two commits tied on the clock; max==min)");
        }
    }

    @Test
    public void buildMvccPartitionViewInvalidTableIsUnpartitioned() {
        // A bucket-partitioned table fails the eligibility gate -> UNPARTITIONED (NOT a degraded LIST).
        Table table = dayPartitionedTable(
                PartitionSpec.builderFor(RELATED_SCHEMA).bucket("id", 4).build(), "id_bucket=1");
        ConnectorMvccPartitionView view = IcebergPartitionUtils.buildMvccPartitionView(table, -1L);
        Assertions.assertEquals(ConnectorMvccPartitionView.Style.UNPARTITIONED, view.getStyle());
        Assertions.assertTrue(view.getPartitions().isEmpty());
        // An unpartitioned view reports newest-update-time 0 (the gate failed before any PARTITIONS scan;
        // dictionary treats it as "unchanged"). MUTATION: a non-zero default -> red.
        Assertions.assertEquals(0L, view.getNewestUpdateTimeMillis());
    }

    @Test
    public void buildMvccPartitionViewSnapshotButNoPartitionRowsReportsZeroNewestUpdate() {
        // A valid related table that HAS a snapshot but whose PARTITIONS scan yields zero rows (an empty
        // append still advances the current snapshot). This is the only path that reaches the
        // max(...).orElse(0L) reduction with an EMPTY stream, so it pins the orElse fallback to 0.
        // MUTATION: orElse(1L) (or any non-zero) -> red.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        PartitionSpec spec = PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        Table table = catalog.createTable(id, RELATED_SCHEMA, spec);
        table.newAppend().commit();   // empty append: advances the snapshot with no data files
        Table loaded = catalog.loadTable(id);
        // Fail LOUD (not assumeTrue/skip): this is the SOLE test that reaches the max(...).orElse(0L) reduction
        // with an EMPTY stream. If a future iceberg version made an empty append a no-op (no snapshot), a silent
        // skip would drop the orElse(0L) mutation coverage undetected — assertNotNull surfaces that regression.
        Assertions.assertNotNull(loaded.currentSnapshot(),
                "empty append must create a snapshot so this case reaches the orElse(0L) empty-stream path");

        ConnectorMvccPartitionView view = IcebergPartitionUtils.buildMvccPartitionView(loaded, -1L);
        Assertions.assertEquals(ConnectorMvccPartitionView.Style.RANGE, view.getStyle());
        Assertions.assertTrue(view.getPartitions().isEmpty(),
                "a snapshot with no data files has no partitions");
        Assertions.assertEquals(0L, view.getNewestUpdateTimeMillis(),
                "an empty partition stream must reduce to newest-update-time 0 (orElse fallback)");
    }

    @Test
    public void buildMvccPartitionViewEmptyValidTableIsRangeWithNoPartitions() {
        // A valid related spec but no data yet: RANGE on the spec alone, empty partition set (parity master:
        // getPartitionType=RANGE, getIcebergPartitionItems empty). MUTATION: returning UNPARTITIONED -> red.
        Table table = tableWith(RELATED_SCHEMA, PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build());
        ConnectorMvccPartitionView view = IcebergPartitionUtils.buildMvccPartitionView(table, -1L);
        Assertions.assertEquals(ConnectorMvccPartitionView.Style.RANGE, view.getStyle());
        Assertions.assertTrue(view.getPartitions().isEmpty());
        // No partitions yet -> newest-update-time 0 (parity master max(...).orElse(0)). MUTATION: orElse non-zero -> red.
        Assertions.assertEquals(0L, view.getNewestUpdateTimeMillis());
    }

    @Test
    public void listPartitionNamesReturnsRawIcebergNames() {
        PartitionSpec spec = PartitionSpec.builderFor(RELATED_SCHEMA).day("ts").build();
        Table table = dayPartitionedTable(spec, "ts_day=1970-04-11", "ts_day=1970-07-20");
        List<String> names = IcebergPartitionUtils.listPartitionNames(table);
        Assertions.assertEquals(new HashSet<>(Arrays.asList("ts_day=100", "ts_day=200")), new HashSet<>(names));
    }

    @Test
    public void listPartitionNamesUnpartitionedIsEmpty() {
        Table table = tableWith(RELATED_SCHEMA, PartitionSpec.unpartitioned());
        Assertions.assertTrue(IcebergPartitionUtils.listPartitionNames(table).isEmpty());
    }

    @Test
    public void listPartitionsDegradesToEmptyWhenPartitionSourceColumnDropped() {
        // Partition-evolution regression (external_table_p0/iceberg/test_iceberg_partition_evolution): a
        // HISTORICAL spec references a source column that was later DROPPED, while the CURRENT spec stays
        // partitioned on a surviving column. Building the PARTITIONS metadata table unifies the partition type
        // across ALL specs, so iceberg throws ValidationException ("Cannot find source column for partition
        // field: ...") for the orphaned field. listPartitions is display/enforcement metadata only (never the
        // read set), so it must degrade to an empty (UNPARTITIONED) list instead of failing the whole query.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get()));
        TableIdentifier id = TableIdentifier.of("db1", "t");
        // format-version 2 so a partition field can be removed (v1 partition specs are append-only).
        Table table = catalog.createTable(id, schema,
                PartitionSpec.builderFor(schema).bucket("region", 8).build(),
                Collections.singletonMap("format-version", "2"));
        // A data file under the original (bucket-on-region) spec so the PARTITIONS metadata scan has a row whose
        // spec must be unified.
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("s3://b/db1/t/f0.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartitionPath("region_bucket=1").withFormat(FileFormat.PARQUET).build()).commit();
        // Evolve: drop the bucket(region) partition field and add identity(id) — the CURRENT spec stays
        // PARTITIONED (on the surviving id column), so listPartitions passes the isUnpartitioned() early-return
        // and genuinely reaches the metadata scan (guards this test against a vacuous unpartitioned pass).
        table.updateSpec().removeField("region_bucket").addField("id").commit();
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("s3://b/db1/t/f1.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartitionPath("id=5").withFormat(FileFormat.PARQUET).build()).commit();
        // Drop the source column referenced only by the historical spec, leaving that spec dangling.
        table.updateSchema().deleteColumn("region").commit();
        Table evolved = catalog.loadTable(id);
        Assertions.assertTrue(evolved.spec().isPartitioned(),
                "current spec must stay partitioned so listPartitions reaches the metadata scan, not the "
                        + "unpartitioned early-return (otherwise this test would pass vacuously)");

        // Precondition — prove the raw iceberg partition-metadata scan genuinely throws ValidationException here
        // (guards against a future iceberg that tolerates the dangling spec, which would make this test vacuous).
        Assertions.assertThrows(ValidationException.class, () -> {
            Table partitionsTable = MetadataTableUtils.createMetadataTableInstance(
                    evolved, MetadataTableType.PARTITIONS);
            try (CloseableIterable<FileScanTask> tasks = partitionsTable.newScan().planFiles()) {
                tasks.forEach(t -> { });
            }
        });

        // The fix: listPartitions swallows exactly that failure and reports UNPARTITIONED (empty), so a
        // full-table select on such a table is not blocked by uncomputable display metadata. MUTATION:
        // rethrowing (or removing the catch) -> this throws instead of returning empty -> red.
        Assertions.assertTrue(IcebergPartitionUtils.listPartitions(evolved).isEmpty());
    }

    @Test
    public void listPartitionNamesForNonRelatedPartitionedTableStillLists() {
        // SHOW PARTITIONS is NOT gated on the MTMV eligibility rules: a bucket-partitioned table still lists its
        // physical partitions (M-10: master rejected iceberg SHOW PARTITIONS, so an empty default would be a
        // silent-zero-rows regression).
        Table table = dayPartitionedTable(
                PartitionSpec.builderFor(RELATED_SCHEMA).bucket("id", 4).build(), "id_bucket=1");
        Assertions.assertEquals(Collections.singletonList("id_bucket=1"),
                IcebergPartitionUtils.listPartitionNames(table));
    }

    @Test
    public void listPartitionsKeepsPartitionColumnCaseForNameLookup() {
        // #65094 read-path alignment regression: the ConnectorPartitionInfo value map that listPartitions
        // returns is keyed by the partition-field SOURCE column name (generateRawPartition). fe-core
        // PluginDrivenExternalTable.getNameToPartitionItems looks each value up by the CASE-PRESERVED
        // partition_columns remote name (IcebergConnectorMetadata.buildTableSchema emits the source name
        // verbatim). If the key were lower-cased, a mixed-case partition column ("Pt") would key the map "pt"
        // while the lookup uses "Pt" -> the partition value is silently dropped (MTMV / partition pruning sees
        // null). Pin the key to the case-preserving name.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "Pt", Types.IntegerType.get()));
        TableIdentifier id = TableIdentifier.of("db1", "t");
        Table table = catalog.createTable(id, schema,
                PartitionSpec.builderFor(schema).identity("Pt").build());
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("s3://b/db1/t/f0.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartitionPath("Pt=7").withFormat(FileFormat.PARQUET).build()).commit();

        List<ConnectorPartitionInfo> parts = IcebergPartitionUtils.listPartitions(catalog.loadTable(id));

        Assertions.assertEquals(1, parts.size());
        Map<String, String> values = parts.get(0).getPartitionValues();
        // Case-preserved key so getNameToPartitionItems' "Pt" remote-name lookup hits.
        Assertions.assertTrue(values.containsKey("Pt"),
                "partition value map must be keyed by the case-preserved partition column name");
        Assertions.assertEquals("7", values.get("Pt"));
        // MUTATION: re-lowercase generateRawPartition's key -> "pt" present, "Pt" absent -> the case-preserving
        // remote-name lookup misses -> red.
        Assertions.assertFalse(values.containsKey("pt"));
    }
}
