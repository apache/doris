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

package org.apache.doris.fluss;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.NestedProjection;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluss cannot push a nested projection down, so a pruned struct still arrives with every field. These
 * pin that the decoder picks fields by the shape rather than by the position Doris asked in.
 */
public class FlussNestedProjectionTest {

    private static final RowType PROFILE = new RowType(Arrays.asList(
            new DataField("city", DataTypes.STRING()),
            new DataField("zip", DataTypes.INT()),
            new DataField("street", DataTypes.STRING())));

    private static GenericRow profileRow() {
        GenericRow row = new GenericRow(3);
        row.setField(0, BinaryString.fromString("beijing"));
        row.setField(1, 100000);
        row.setField(2, BinaryString.fromString("road-a"));
        return row;
    }

    @Test
    public void prunedStructReadsTheRequestedFieldNotTheOneAtThatPosition() {
        // "city" (source position 0) is STRING and "photo" (source position 2) is BINARY. That
        // difference in byte representation is deliberate: PROFILE above cannot gate this test because
        // city and street are BOTH string-shaped, so a decoder that read the type sitting at the
        // REQUESTED position (0) instead of the RESOLVED source position would still take the same
        // "case STRING" branch in readBytes() and the test would pass regardless. Here it would instead
        // try to read photo's raw bytes as a BinaryString and throw.
        RowType source = new RowType(Arrays.asList(
                new DataField("city", DataTypes.STRING()),
                new DataField("zip", DataTypes.INT()),
                new DataField("photo", DataTypes.BINARY(4))));
        GenericRow sourceRow = new GenericRow(3);
        sourceRow.setField(0, BinaryString.fromString("beijing"));
        sourceRow.setField(1, 100000);
        sourceRow.setField(2, new byte[] {1, 2, 3, 4});

        // Doris asks for {photo, zip}; a position-aligned decoder would hand back {city, zip}.
        ColumnType required = ColumnType.parseType("p", "struct<photo:varbinary,zip:int>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, source, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow outer = new GenericRow(1);
        outer.setField(0, sourceRow);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(outer);
        value.setIdx(0, required, source, shape);

        List<ColumnValue> children = new ArrayList<>();
        value.unpackStruct(Arrays.asList(0, 1), children);

        Assertions.assertArrayEquals(new byte[] {1, 2, 3, 4}, children.get(0).getBytes());
        Assertions.assertEquals(100000, children.get(1).getInt());
    }

    @Test
    public void wholeStructStillDecodesWhenTheShapeIsIdentity() {
        ColumnType required =
                ColumnType.parseType("p", "struct<city:string,zip:int,street:string>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, PROFILE, FlussNestedTypeSource.INSTANCE);
        Assertions.assertTrue(shape.isIdentity());

        GenericRow outer = new GenericRow(1);
        outer.setField(0, profileRow());
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(outer);
        // The scanner passes null for an identity column so the hot path stays exactly as it was.
        value.setIdx(0, required, PROFILE, null);

        List<ColumnValue> children = new ArrayList<>();
        value.unpackStruct(Arrays.asList(0, 1, 2), children);

        Assertions.assertEquals("beijing", children.get(0).getString());
        Assertions.assertEquals(100000, children.get(1).getInt());
        Assertions.assertEquals("road-a", children.get(2).getString());
    }

    @Test
    public void nestedStructInsideStructUsesTheChildsOwnShapeNotTheParents() {
        RowType inner = new RowType(Arrays.asList(
                new DataField("a", DataTypes.STRING()),
                new DataField("b", DataTypes.INT())));
        RowType outer = new RowType(Arrays.asList(
                new DataField("filler", DataTypes.INT()),
                new DataField("nested", inner)));
        GenericRow innerRow = new GenericRow(2);
        innerRow.setField(0, BinaryString.fromString("a-value"));
        innerRow.setField(1, 55);
        GenericRow outerRow = new GenericRow(2);
        outerRow.setField(0, -1);
        outerRow.setField(1, innerRow);

        // "nested" resolves to source index 1, and inside it "b" resolves to source index 1 too: if the
        // shape handed to the CHILD value were lost (e.g. null, or the parent's own shape), unpacking the
        // child would fall back to position 0 ("a") instead.
        ColumnType required = ColumnType.parseType("p", "struct<nested:struct<b:int>>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, outer, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow topRow = new GenericRow(1);
        topRow.setField(0, outerRow);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(topRow);
        value.setIdx(0, required, outer, shape);

        List<ColumnValue> outerChildren = new ArrayList<>();
        value.unpackStruct(Collections.singletonList(0), outerChildren);
        ColumnValue nested = outerChildren.get(0);

        List<ColumnValue> innerChildren = new ArrayList<>();
        nested.unpackStruct(Collections.singletonList(0), innerChildren);
        Assertions.assertEquals(55, innerChildren.get(0).getInt());
    }

    @Test
    public void structInsideArrayUsesTheElementsOwnShape() {
        RowType element = new RowType(Arrays.asList(
                new DataField("first", DataTypes.INT()),
                new DataField("second", DataTypes.STRING())));
        ArrayType arrayType = new ArrayType(element);
        GenericRow elem0 = new GenericRow(2);
        elem0.setField(0, 1);
        elem0.setField(1, BinaryString.fromString("one"));
        GenericRow elem1 = new GenericRow(2);
        elem1.setField(0, 2);
        elem1.setField(1, BinaryString.fromString("two"));
        GenericArray array = new GenericArray(new Object[] {elem0, elem1});

        // "second" resolves to source index 1: an element that lost its own shape would read position 0
        // ("first") instead, an int where a string was requested.
        ColumnType required = ColumnType.parseType("p", "array<struct<second:string>>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, arrayType, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow topRow = new GenericRow(1);
        topRow.setField(0, array);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(topRow);
        value.setIdx(0, required, arrayType, shape);

        List<ColumnValue> elements = new ArrayList<>();
        value.unpackArray(elements);
        Assertions.assertEquals(2, elements.size());

        List<ColumnValue> fields0 = new ArrayList<>();
        elements.get(0).unpackStruct(Collections.singletonList(0), fields0);
        Assertions.assertEquals("one", fields0.get(0).getString());

        List<ColumnValue> fields1 = new ArrayList<>();
        elements.get(1).unpackStruct(Collections.singletonList(0), fields1);
        Assertions.assertEquals("two", fields1.get(0).getString());
    }

    @Test
    public void structInsideMapValueUsesTheValuesOwnShape() {
        RowType mapValue = new RowType(Arrays.asList(
                new DataField("p", DataTypes.INT()),
                new DataField("q", DataTypes.STRING())));
        MapType mapType = new MapType(DataTypes.STRING(), mapValue);
        GenericRow val = new GenericRow(2);
        val.setField(0, 10);
        val.setField(1, BinaryString.fromString("qq"));
        Map<Object, Object> raw = new LinkedHashMap<>();
        raw.put(BinaryString.fromString("k1"), val);
        GenericMap map = new GenericMap(raw);

        // "q" resolves to source index 1: a value that lost its own shape would read position 0 ("p")
        // instead, an int where a string was requested.
        ColumnType required = ColumnType.parseType("p", "map<string,struct<q:string>>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, mapType, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow topRow = new GenericRow(1);
        topRow.setField(0, map);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(topRow);
        value.setIdx(0, required, mapType, shape);

        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();
        value.unpackMap(keys, values);
        Assertions.assertEquals(1, values.size());

        List<ColumnValue> fields = new ArrayList<>();
        values.get(0).unpackStruct(Collections.singletonList(0), fields);
        Assertions.assertEquals("qq", fields.get(0).getString());
    }

    @Test
    public void structAsMapKeyUsesTheKeysOwnShape() {
        RowType mapKey = new RowType(Arrays.asList(
                new DataField("m", DataTypes.INT()),
                new DataField("n", DataTypes.STRING())));
        MapType mapType = new MapType(mapKey, DataTypes.INT());
        GenericRow key = new GenericRow(2);
        key.setField(0, -1);
        key.setField(1, BinaryString.fromString("key-n"));
        Map<Object, Object> raw = new LinkedHashMap<>();
        raw.put(key, 42);
        GenericMap map = new GenericMap(raw);

        // "n" resolves to source index 1: a key that lost its own shape would read position 0 ("m")
        // instead, an int where a string was requested.
        ColumnType required = ColumnType.parseType("p", "map<struct<n:string>,int>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, mapType, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow topRow = new GenericRow(1);
        topRow.setField(0, map);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(topRow);
        value.setIdx(0, required, mapType, shape);

        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();
        value.unpackMap(keys, values);
        Assertions.assertEquals(1, keys.size());

        List<ColumnValue> fields = new ArrayList<>();
        keys.get(0).unpackStruct(Collections.singletonList(0), fields);
        Assertions.assertEquals("key-n", fields.get(0).getString());
    }

    @Test
    public void mixedCaseFlussFieldNameResolvesCaseInsensitively() {
        // ColumnType.parseType lowercases every legacy-grammar STRUCT field name. That grammar is now
        // only the rolling-upgrade fallback for a BE that predates fluss_jni_reader.h's encoded-schema
        // override (see exactCaseFlussFieldNameResolvesByExactMatch below for the normal path); on the
        // fallback, a fluss field whose name is not already lowercase can only resolve through
        // NestedProjection's case-insensitive fallback, and this pins that path.
        RowType source = new RowType(Arrays.asList(
                new DataField("Zip", DataTypes.INT()),
                new DataField("City", DataTypes.STRING())));
        GenericRow sourceRow = new GenericRow(2);
        sourceRow.setField(0, 999);
        sourceRow.setField(1, BinaryString.fromString("beijing"));

        ColumnType required = ColumnType.parseType("p", "struct<city:string>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, source, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());

        GenericRow outer = new GenericRow(1);
        outer.setField(0, sourceRow);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(outer);
        value.setIdx(0, required, source, shape);

        List<ColumnValue> children = new ArrayList<>();
        value.unpackStruct(Collections.singletonList(0), children);
        Assertions.assertEquals("beijing", children.get(0).getString());
    }

    @Test
    public void exactCaseFlussFieldNameResolvesByExactMatch() {
        // The encoded schema payload (what fluss_jni_reader.h now publishes) preserves a STRUCT field's
        // exact spelling instead of lowercasing it: ColumnType.parseTypeWithEncodedStructFields, not the
        // legacy parseType the sibling test above exercises. A requested name that already matches the
        // source exactly must resolve through NestedProjection's exact-match branch on the first pass,
        // not fall through to the case-insensitive one.
        RowType source = new RowType(Arrays.asList(
                new DataField("Zip", DataTypes.INT()),
                new DataField("City", DataTypes.STRING())));
        GenericRow sourceRow = new GenericRow(2);
        sourceRow.setField(0, 999);
        sourceRow.setField(1, BinaryString.fromString("beijing"));

        // "$Q2l0eQ==" is the base64 encoding of "City", version-marked the way the encoded grammar
        // requires.
        ColumnType required = ColumnType.parseTypeWithEncodedStructFields("p", "struct<$Q2l0eQ==:string>");
        NestedProjection<DataType> shape =
                NestedProjection.of(required, source, FlussNestedTypeSource.INSTANCE);
        Assertions.assertFalse(shape.isIdentity());
        Assertions.assertEquals(1, shape.sourceChildIndex(0));

        GenericRow outer = new GenericRow(1);
        outer.setField(0, sourceRow);
        FlussColumnValue value = new FlussColumnValue("UTC");
        value.setRow(outer);
        value.setIdx(0, required, source, shape);

        List<ColumnValue> children = new ArrayList<>();
        value.unpackStruct(Collections.singletonList(0), children);
        Assertions.assertEquals("beijing", children.get(0).getString());
    }

    @Test
    public void duplicateRequestedFieldNamesFromLegacyLoweringFailLoud() {
        // The legacy grammar is the rolling-upgrade fallback this scanner falls back to when BE has not
        // published the encoded schema. It lowercases every requested STRUCT field name, so a source
        // whose fields differ only by case ("a" and "A") makes Doris ask for the same spelling twice.
        // Both requested children then resolve to source index 0: without NestedProjection's collision
        // guard, field 0 is read twice and field 1 is never read, silently -- today this is read
        // correctly by position, and this pins that a request that cannot be satisfied fails loud
        // instead of turning that into a silent wrong-column read once decoding resolves by name. This
        // is the core regression guard for this task.
        RowType source = new RowType(Arrays.asList(
                new DataField("a", DataTypes.INT()),
                new DataField("A", DataTypes.INT())));

        ColumnType required = ColumnType.parseType("p", "struct<a:int,a:int>");

        try {
            NestedProjection.of(required, source, FlussNestedTypeSource.INSTANCE);
            Assertions.fail("two requested children resolving to one source field must not resolve");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("a"));
        }
    }

    /**
     * Constructs a scanner without a live fluss cluster (the constructor never calls {@code open()}),
     * exposing the constructor-parsed types the way {@code InspectablePaimonJniScanner} does in the
     * sibling paimon-scanner test.
     */
    private static final class InspectableFlussJniScanner extends FlussJniScanner {
        InspectableFlussJniScanner(int batchSize, Map<String, String> params) {
            super(batchSize, params);
        }

        ColumnType[] requiredTypes() {
            return types;
        }
    }

    @Test
    public void encodedSchemaFromBePreservesStructFieldSpellingThroughTheScannerConstructor() {
        // exactCaseFlussFieldNameResolvesByExactMatch above pins ColumnType.parseTypeWithEncodedStructFields
        // itself; this pins that FlussJniScanner's constructor actually reaches for it when BE publishes
        // the encoded pair, rather than always taking the legacy (lowercasing) parseType branch. That
        // distinction lives entirely inside the constructor's own `encodedSchema ? ... : ...`, which no
        // other test in this module exercises with an encoded payload.
        String encodedFieldName =
                "$" + Base64.getEncoder().encodeToString("address".getBytes(StandardCharsets.UTF_8));
        String encodedChildName =
                "$" + Base64.getEncoder().encodeToString("City".getBytes(StandardCharsets.UTF_8));
        String encodedTypeDescriptor = "$" + Base64.getEncoder().encodeToString(
                ("struct<" + encodedChildName + ":string>").getBytes(StandardCharsets.UTF_8));

        Map<String, String> params = new HashMap<>();
        params.put("fluss.range_type", "LOG");
        params.put("fluss.log_start_offset", "0");
        params.put("fluss.log_stop_offset", "1");
        params.put("fluss.bucket_id", "0");
        params.put("required_fields_base64", encodedFieldName);
        params.put("columns_types_base64", encodedTypeDescriptor);

        InspectableFlussJniScanner scanner = new InspectableFlussJniScanner(128, params);

        Assertions.assertEquals(1, scanner.requiredTypes().length);
        ColumnType structType = scanner.requiredTypes()[0];
        Assertions.assertTrue(structType.isStruct());
        Assertions.assertEquals(Collections.singletonList("City"), structType.getChildNames());
    }
}
