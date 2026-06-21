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

import org.apache.doris.connector.api.ConnectorType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-direction parity tests for {@link IcebergTypeMapping#fromIcebergType}, pinning the
 * Iceberg-&gt;Doris type mapping byte-for-byte against legacy
 * {@code IcebergUtils.icebergPrimitiveTypeToDorisType} / {@code icebergTypeToDorisType} (fe-core).
 * Mirrors the paimon connector's {@code PaimonTypeMappingReadTest}.
 *
 * <p>The emitted {@link ConnectorType} type names must be ones that
 * {@code ConnectorColumnConverter.convertScalarType} actually recognizes, otherwise a column silently
 * degrades to UNSUPPORTED. This is exactly the {@code TIMESTAMPTZ} (P6-T08) fix: the converter has a
 * {@code TIMESTAMPTZ} case (mapping to {@code ScalarType.createTimeStampTzType(precision)}) but no
 * {@code TIMESTAMPTZV2} case, so the connector must emit the former.
 */
public class IcebergTypeMappingReadTest {

    private static final int MS6 = 6;

    /** Map with both mapping flags OFF (the production default). */
    private static ConnectorType mapOff(Type t) {
        return IcebergTypeMapping.fromIcebergType(t, false, false);
    }

    /** Map with both mapping flags ON (varbinary + timestamp-tz). */
    private static ConnectorType mapOn(Type t) {
        return IcebergTypeMapping.fromIcebergType(t, true, true);
    }

    private static void assertScalar(ConnectorType actual, String name, int precision, int scale) {
        Assertions.assertEquals(name, actual.getTypeName());
        Assertions.assertEquals(precision, actual.getPrecision(), "precision of " + name);
        Assertions.assertEquals(scale, actual.getScale(), "scale of " + name);
    }

    // ---------------------------------------------------------------------
    // Flag-independent primitives — must match legacy icebergPrimitiveTypeToDorisType exactly.
    // ---------------------------------------------------------------------

    @Test
    public void flagIndependentPrimitivesMatchLegacy() {
        // WHY: these eight Iceberg primitives map to a fixed Doris type regardless of either mapping
        // flag; legacy returns Type.BOOLEAN/INT/BIGINT/FLOAT/DOUBLE/STRING, DateV2, and DatetimeV2(6)
        // for a no-zone TIMESTAMP. The connector must reproduce the SAME Doris type names so DESCRIBE /
        // SHOW CREATE TABLE report identically. MUTATION: renaming any (e.g. INTEGER->"INTEGER") -> red.
        Assertions.assertEquals("BOOLEAN", mapOff(Types.BooleanType.get()).getTypeName());
        Assertions.assertEquals("INT", mapOff(Types.IntegerType.get()).getTypeName());
        Assertions.assertEquals("BIGINT", mapOff(Types.LongType.get()).getTypeName());
        Assertions.assertEquals("FLOAT", mapOff(Types.FloatType.get()).getTypeName());
        Assertions.assertEquals("DOUBLE", mapOff(Types.DoubleType.get()).getTypeName());
        Assertions.assertEquals("STRING", mapOff(Types.StringType.get()).getTypeName());
        Assertions.assertEquals("DATEV2", mapOff(Types.DateType.get()).getTypeName());

        // A no-zone TIMESTAMP is DATETIMEV2(6) whether or not the tz flag is on (the flag only affects
        // zoned timestamps). Legacy: createDatetimeV2Type(ICEBERG_DATETIME_SCALE_MS=6).
        assertScalar(mapOff(Types.TimestampType.withoutZone()), "DATETIMEV2", MS6, 0);
        assertScalar(mapOn(Types.TimestampType.withoutZone()), "DATETIMEV2", MS6, 0);

        // TIME has no Doris analogue -> UNSUPPORTED (legacy Type.UNSUPPORTED).
        Assertions.assertEquals("UNSUPPORTED", mapOff(Types.TimeType.get()).getTypeName());
    }

    @Test
    public void decimalCarriesPrecisionAndScale() {
        // WHY: Iceberg DECIMAL(p,s) maps to Doris DECIMALV3(p,s) carrying both p and s verbatim; legacy
        // createDecimalV3Type(precision, scale). MUTATION: dropping scale, or emitting DECIMALV2 -> red.
        assertScalar(mapOff(Types.DecimalType.of(20, 4)), "DECIMALV3", 20, 4);
    }

    // ---------------------------------------------------------------------
    // enable.mapping.varbinary toggle — UUID / BINARY / FIXED
    // ---------------------------------------------------------------------

    @Test
    public void varbinaryFlagOffMapsToStringOrChar() {
        // WHY: with the varbinary flag OFF, UUID and BINARY fall back to STRING and a FIXED(n) becomes
        // CHAR(n) — legacy returns Type.STRING / Type.STRING / createCharType(length). This is the
        // compatibility default. MUTATION: emitting VARBINARY when the flag is off -> red.
        Assertions.assertEquals("STRING", mapOff(Types.UUIDType.get()).getTypeName());
        Assertions.assertEquals("STRING", mapOff(Types.BinaryType.get()).getTypeName());
        assertScalar(mapOff(Types.FixedType.ofLength(12)), "CHAR", 12, 0);
    }

    @Test
    public void varbinaryFlagOnMapsToVarbinaryWithLegacyLengths() {
        // WHY: with the varbinary flag ON, UUID -> VARBINARY(16) and FIXED(n) -> VARBINARY(n); the
        // lengths are load-bearing — legacy createVarbinaryType(16 / fixed.length()). MUTATION: wrong
        // length, or staying STRING/CHAR under the flag -> red.
        assertScalar(mapOn(Types.UUIDType.get()), "VARBINARY", 16, 0);
        assertScalar(mapOn(Types.FixedType.ofLength(12)), "VARBINARY", 12, 0);

        // WHY: an Iceberg BINARY is UNBOUNDED, and legacy maps it to the max-length varbinary —
        // createVarbinaryType(VarBinaryType.MAX_VARBINARY_LENGTH == ScalarType.MAX_VARBINARY_LENGTH ==
        // 0x7fffffff). The connector must NOT stamp a concrete length like 65535 (that renders a
        // different DESCRIBE / SHOW CREATE type than legacy). Emitting precision -1 (no explicit length)
        // makes ConnectorColumnConverter fall to its default branch
        // createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH) — byte-identical to legacy.
        // MUTATION: stamping VARBINARY(65535) (the pre-fix divergence) -> precision != -1 -> red.
        ConnectorType binOn = mapOn(Types.BinaryType.get());
        Assertions.assertEquals("VARBINARY", binOn.getTypeName());
        Assertions.assertEquals(-1, binOn.getPrecision(),
                "unbounded BINARY must carry no explicit length (-1) so the converter applies the "
                        + "shared MAX_VARBINARY_LENGTH, matching legacy");
    }

    // ---------------------------------------------------------------------
    // enable.mapping.timestamp_tz toggle — THE P6-T08 fix (TIMESTAMPTZ, not TIMESTAMPTZV2)
    // ---------------------------------------------------------------------

    @Test
    public void zonedTimestampWithFlagOnMapsToConverterRecognizedTimestamptz() {
        // WHY: a zoned Iceberg TIMESTAMP (shouldAdjustToUTC) with the tz flag ON must map to a type the
        // converter actually understands. ConnectorColumnConverter recognizes "TIMESTAMPTZ" (->
        // createTimeStampTzType(precision)) but NOT "TIMESTAMPTZV2"; emitting the latter silently
        // degrades the column to UNSUPPORTED. Legacy createTimeStampTzType(ICEBERG_DATETIME_SCALE_MS=6)
        // => the connector must emit TIMESTAMPTZ with precision 6. MUTATION: reverting to TIMESTAMPTZV2
        // (the pre-T08 bug) -> red.
        assertScalar(mapOn(Types.TimestampType.withZone()), "TIMESTAMPTZ", MS6, 0);
    }

    @Test
    public void zonedTimestampWithFlagOffStaysDatetimev2() {
        // WHY: the tz flag gates the TIMESTAMPTZ mapping; with it OFF even a zoned timestamp must stay
        // DATETIMEV2(6) (legacy createDatetimeV2Type(6)). This guards a fix that accidentally promotes
        // zoned timestamps unconditionally. MUTATION: emitting TIMESTAMPTZ when the flag is off -> red.
        assertScalar(IcebergTypeMapping.fromIcebergType(Types.TimestampType.withZone(), false, false),
                "DATETIMEV2", MS6, 0);
    }

    // ---------------------------------------------------------------------
    // Nested types — ARRAY / MAP / STRUCT recurse with the same flags
    // ---------------------------------------------------------------------

    @Test
    public void arrayRecursesElementType() {
        // WHY: an Iceberg LIST maps to a Doris ARRAY whose element is the mapped element type, threading
        // the flags through. Legacy ArrayType.create(icebergTypeToDorisType(element, ...)). Here the
        // zoned-timestamp element + tz flag proves both recursion and flag propagation reach the leaf.
        // MUTATION: not recursing (raw element), or dropping the flags on recursion -> red.
        Types.ListType list = Types.ListType.ofOptional(1, Types.TimestampType.withZone());
        ConnectorType arr = mapOn(list);
        Assertions.assertEquals("ARRAY", arr.getTypeName());
        Assertions.assertEquals(1, arr.getChildren().size());
        assertScalar(arr.getChildren().get(0), "TIMESTAMPTZ", MS6, 0);
    }

    @Test
    public void mapRecursesKeyAndValueTypes() {
        // WHY: an Iceberg MAP maps to a Doris MAP with both key and value mapped (flags threaded).
        // Legacy new MapType(mapped(key), mapped(value)). MUTATION: swapping/dropping a child, or not
        // recursing -> red.
        Types.MapType map = Types.MapType.ofOptional(
                1, 2, Types.StringType.get(), Types.BinaryType.get());
        ConnectorType m = mapOn(map);
        Assertions.assertEquals("MAP", m.getTypeName());
        Assertions.assertEquals(2, m.getChildren().size());
        Assertions.assertEquals("STRING", m.getChildren().get(0).getTypeName());
        // The unbounded-BINARY value recurses to VARBINARY with no explicit length (-1 -> converter
        // applies the shared MAX_VARBINARY_LENGTH, matching legacy).
        Assertions.assertEquals("VARBINARY", m.getChildren().get(1).getTypeName());
        Assertions.assertEquals(-1, m.getChildren().get(1).getPrecision());
    }

    @Test
    public void structRecursesFieldsPreservingNamesAndOrder() {
        // WHY: an Iceberg STRUCT maps to a Doris STRUCT preserving field names, order, and mapped field
        // types (flags threaded). Legacy builds StructField(name, mapped(type)) per field in order.
        // MUTATION: reordering, dropping names, or not recursing field types -> red.
        Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, "a", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.TimestampType.withZone()));
        ConnectorType s = mapOn(struct);
        Assertions.assertEquals("STRUCT", s.getTypeName());
        List<String> names = s.getFieldNames();
        Assertions.assertEquals(List.of("a", "b"), names);
        Assertions.assertEquals("INT", s.getChildren().get(0).getTypeName());
        assertScalar(s.getChildren().get(1), "TIMESTAMPTZ", MS6, 0);
    }
}
