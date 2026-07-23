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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * P5-T11 — pins the Doris-&gt;Paimon reverse type mapping in
 * {@link PaimonTypeMapping#toPaimonType} to byte-parity with the legacy fe-core
 * {@code DorisToPaimonTypeVisitor}.
 *
 * <p>The CREATE TABLE path produces these {@link ConnectorType} descriptors; this mapping is
 * what decides the on-disk Paimon column type, so any drift silently changes the physical schema
 * of newly created tables.</p>
 */
public class PaimonTypeMappingToPaimonTest {

    @Test
    public void scalarPrimitivesMapExactly() {
        // WHY: the narrow scalar set is the legacy contract; each must produce the exact paimon
        // no-arg type. MUTATION: swapping e.g. INT -> BigIntType, or adding precision to a no-arg
        // type, changes the persisted column type and turns these red.
        Assertions.assertEquals(new BooleanType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("BOOLEAN")));
        Assertions.assertEquals(new IntType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("INT")));
        Assertions.assertEquals(new IntType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("INTEGER")));
        Assertions.assertEquals(new BigIntType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("BIGINT")));
        Assertions.assertEquals(new FloatType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("FLOAT")));
        Assertions.assertEquals(new DoubleType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("DOUBLE")));
        Assertions.assertEquals(new DateType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("DATE")));
        Assertions.assertEquals(new DateType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("DATEV2")));
        Assertions.assertEquals(new VariantType(), PaimonTypeMapping.toPaimonType(ConnectorType.of("VARIANT")));
    }

    @Test
    public void charFamilyCollapsesToVarcharMaxDroppingLength() {
        // WHY: legacy isCharFamily -> VarCharType(MAX_LENGTH) unconditionally; the declared length
        // is intentionally dropped and CHAR is NOT mapped to paimon CharType. MUTATION: honoring
        // the declared length (e.g. new VarCharType(10)) or mapping CHAR -> CharType makes these red.
        DataType expected = new VarCharType(VarCharType.MAX_LENGTH);
        Assertions.assertEquals(expected, PaimonTypeMapping.toPaimonType(ConnectorType.of("CHAR", 10, 0)));
        Assertions.assertEquals(expected, PaimonTypeMapping.toPaimonType(ConnectorType.of("VARCHAR", 20, 0)));
        Assertions.assertEquals(expected, PaimonTypeMapping.toPaimonType(ConnectorType.of("STRING")));
    }

    @Test
    public void datetimeDropsScaleToNoArgTimestamp() {
        // WHY: legacy maps DATETIME/DATETIMEV2 -> new TimestampType() (no-arg, precision 6); the
        // requested datetime scale is intentionally dropped, and it is a plain timestamp not a
        // zoned one. MUTATION: propagating the scale (new TimestampType(scale)) or using
        // LocalZonedTimestampType makes this red.
        TimestampType expected = new TimestampType();
        Assertions.assertEquals(6, expected.getPrecision(), "no-arg TimestampType must default to precision 6");
        Assertions.assertEquals(expected,
                PaimonTypeMapping.toPaimonType(ConnectorType.of("DATETIMEV2", 3, 0)),
                "DATETIMEV2(scale 3) must drop the scale -> TimestampType() precision 6");
        Assertions.assertEquals(expected, PaimonTypeMapping.toPaimonType(ConnectorType.of("DATETIME")));
    }

    @Test
    public void decimalCarriesPrecisionAndScale() {
        // WHY: every decimal family member carries precision/scale through verbatim. MUTATION:
        // hardcoding a precision/scale, or swapping the two args, turns this red.
        Assertions.assertEquals(new DecimalType(18, 4),
                PaimonTypeMapping.toPaimonType(ConnectorType.of("DECIMAL64", 18, 4)));
        Assertions.assertEquals(new DecimalType(9, 2),
                PaimonTypeMapping.toPaimonType(ConnectorType.of("DECIMAL32", 9, 2)));
        Assertions.assertEquals(new DecimalType(27, 9),
                PaimonTypeMapping.toPaimonType(ConnectorType.of("DECIMALV2", 27, 9)));
    }

    @Test
    public void varbinaryMapsToVarBinaryMax() {
        // WHY: legacy isVarbinaryType -> VarBinaryType(MAX_LENGTH). MUTATION: honoring a declared
        // length or mapping to BinaryType makes this red.
        Assertions.assertEquals(new VarBinaryType(VarBinaryType.MAX_LENGTH),
                PaimonTypeMapping.toPaimonType(ConnectorType.of("VARBINARY", 16, 0)));
    }

    @Test
    public void arrayRecursesElement() {
        // WHY: ARRAY<INT> must wrap the recursively mapped element. MUTATION: dropping the recursion
        // (e.g. wrapping a raw VarChar) or losing the element type makes this red.
        Assertions.assertEquals(new ArrayType(new IntType()),
                PaimonTypeMapping.toPaimonType(ConnectorType.arrayOf(ConnectorType.of("INT"))));
    }

    @Test
    public void mapForcesNonNullKey() {
        // WHY: legacy MAP forces the key non-null via keyResult.copy(false) while the value keeps
        // the paimon default (nullable). This is part of the type structure, not column nullability.
        // MUTATION: dropping the .copy(false) on the key (so the key is nullable) makes this red.
        DataType actual = PaimonTypeMapping.toPaimonType(
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")));
        MapType expected = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH).copy(false), new IntType());
        Assertions.assertEquals(expected, actual);
        Assertions.assertFalse(((MapType) actual).getKeyType().isNullable(),
                "the map key type must be non-null (legacy .copy(false) parity)");
    }

    @Test
    public void structBuildsSequentialFieldIdsAndNames() {
        // WHY: STRUCT must build DataFields with sequential ids 0,1,... (legacy AtomicInteger(-1)
        // incrementAndGet) and names from getFieldNames, recursing each field type. MUTATION: a
        // wrong starting id (e.g. 1), reused ids, or losing a field name turns this red.
        ConnectorType struct = ConnectorType.structOf(
                Arrays.asList("a", "b"),
                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")));
        RowType row = (RowType) PaimonTypeMapping.toPaimonType(struct);

        DataField f0 = new DataField(0, "a", new IntType());
        DataField f1 = new DataField(1, "b", new VarCharType(VarCharType.MAX_LENGTH));
        Assertions.assertEquals(new RowType(Arrays.asList(f0, f1)), row);
        Assertions.assertEquals(0, row.getFields().get(0).id(), "first struct field id must be 0");
        Assertions.assertEquals(1, row.getFields().get(1).id(), "second struct field id must be 1");
    }

    @Test
    public void nestedNullabilityPreservedForArrayElement() {
        // WHY (FIX-L13): a declared NOT NULL element (ARRAY<INT NOT NULL>) must map to a non-null paimon
        // element type (legacy DorisToPaimonTypeVisitor array = elementResult.copy(array.getContainsNull())).
        // MUTATION: dropping the .copy(isChildNullable(0)) on the ARRAY element leaves it nullable -> red.
        DataType actual = PaimonTypeMapping.toPaimonType(
                ConnectorType.arrayOf(ConnectorType.of("INT"), /*elementNullable*/ false));
        Assertions.assertFalse(((ArrayType) actual).getElementType().isNullable(),
                "a NOT NULL array element must map to a non-null paimon element type");
    }

    @Test
    public void nestedNullabilityPreservedForMapValue() {
        // WHY (FIX-L13): a declared NOT NULL map value (MAP<STRING, INT NOT NULL>) must map to a non-null
        // paimon value type (legacy map value = valueResult.copy(map.getIsValueContainsNull())), while the
        // key stays non-null. MUTATION: dropping the .copy(isChildNullable(1)) on the MAP value -> red.
        DataType actual = PaimonTypeMapping.toPaimonType(ConnectorType.mapOf(
                ConnectorType.of("STRING"), ConnectorType.of("INT"), /*valueNullable*/ false));
        Assertions.assertFalse(((MapType) actual).getValueType().isNullable(),
                "a NOT NULL map value must map to a non-null paimon value type");
        Assertions.assertFalse(((MapType) actual).getKeyType().isNullable(),
                "the map key stays non-null regardless (legacy .copy(false))");
    }

    @Test
    public void nestedNullabilityPreservedForStructField() {
        // WHY (FIX-L13): declared per-field nullability (STRUCT<x:INT NOT NULL, y:STRING>) must survive to
        // the paimon DataField types (legacy struct = fieldResults.get(i).copy(field.getContainsNull())).
        // Asserted via field.type().isNullable() to isolate the nullability facet; the nested field
        // comment is now carried too and is covered separately by nestedStructFieldCommentPreserved.
        // MUTATION: dropping the .copy(isChildNullable(i)) on the struct DataField -> field 0 stays nullable.
        ConnectorType struct = ConnectorType.structOf(
                Arrays.asList("x", "y"),
                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")),
                Arrays.asList(false, true),
                Collections.emptyList());
        RowType row = (RowType) PaimonTypeMapping.toPaimonType(struct);
        Assertions.assertFalse(row.getFields().get(0).type().isNullable(),
                "a NOT NULL struct field must map to a non-null paimon field type");
        Assertions.assertTrue(row.getFields().get(1).type().isNullable(),
                "a nullable struct field stays nullable");
    }

    @Test
    public void nestedStructFieldCommentPreserved() {
        // WHY: a COMMENT on a field nested inside a STRUCT column must survive CREATE TABLE mapping,
        // reaching the paimon DataField description (parity with top-level column comments already
        // carried by PaimonSchemaBuilder). Without it the on-disk paimon schema — and, via the
        // symmetric read fix, DESCRIBE — loses the comment.
        // MUTATION: reverting to the 3-arg DataField (dropping type.getChildComment(i)) leaves the
        // description null -> this assertion goes red.
        ConnectorType struct = ConnectorType.structOf(
                Arrays.asList("x", "y"),
                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")),
                Arrays.asList(true, true),
                Arrays.asList("note on x", ""));
        RowType row = (RowType) PaimonTypeMapping.toPaimonType(struct);
        Assertions.assertEquals("note on x", row.getFields().get(0).description(),
                "a nested struct field comment must be carried into the paimon DataField");
    }

    @Test
    public void unsupportedScalarTypesThrow() {
        // WHY: the legacy visitor had no branch for these and threw; the connector preserves that
        // gap by throwing DorisConnectorException rather than inventing a mapping. MUTATION: adding
        // a TINYINT/SMALLINT/LARGEINT/TIME/TIMESTAMPTZ branch (silently widening support) would
        // make the corresponding assertion red.
        for (String unsupported : new String[] {"TINYINT", "SMALLINT", "LARGEINT", "TIMEV2", "TIMESTAMPTZ"}) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> PaimonTypeMapping.toPaimonType(ConnectorType.of(unsupported)),
                    unsupported + " must throw (legacy gap preserved)");
        }
    }

    @Test
    public void nestedUnsupportedTypePropagatesThrow() {
        // WHY: an unsupported element nested inside a complex type must still fail-fast, proving the
        // throw is reached through the recursion (not swallowed). MUTATION: catching/degrading the
        // nested throw inside array/map/struct handling would make this red.
        ConnectorType arrayOfTinyint = ConnectorType.arrayOf(ConnectorType.of("TINYINT"));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonTypeMapping.toPaimonType(arrayOfTinyint));

        ConnectorType structWithBadField = ConnectorType.structOf(
                Collections.singletonList("x"),
                Collections.singletonList(ConnectorType.of("JSON")));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonTypeMapping.toPaimonType(structWithBadField));
    }
}
