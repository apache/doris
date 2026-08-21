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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PaimonColumnValueTest {
    private final RowType paimonStructType = RowType.of(
            new DataType[] {new IntType(), new VarCharType(), new BigIntType()},
            new String[] {"a", "b", "c"});
    private final ColumnType dorisStructType =
            ColumnType.parseType("s", "struct<a:int,b:string,c:bigint>");

    @Test
    public void testUnpackNonLeadingStructField() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Collections.singletonList(2), values);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals(100L, values.get(0).getLong());
    }

    @Test
    public void testUnpackNonContiguousStructFields() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Arrays.asList(0, 2), values);

        Assert.assertEquals(2, values.size());
        Assert.assertEquals(10, values.get(0).getInt());
        Assert.assertEquals(100L, values.get(1).getLong());
    }

    @Test
    public void testUnpackCompleteStruct() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Arrays.asList(0, 1, 2), values);

        Assert.assertEquals(3, values.size());
        Assert.assertEquals(10, values.get(0).getInt());
        Assert.assertEquals("x", values.get(1).getString());
        Assert.assertEquals(100L, values.get(2).getLong());
    }

    @Test
    public void testReuseArrayElementsAcrossRows() {
        ArrayType paimonArrayType = new ArrayType(new IntType());
        ColumnType dorisArrayType = ColumnType.parseType("a", "array<int>");
        PaimonColumnValue arrayValue = new PaimonColumnValue(
                GenericRow.of(new GenericArray(new int[] {1, 2})), 0,
                dorisArrayType, paimonArrayType, "UTC");

        List<ColumnValue> firstValues = new ArrayList<>();
        arrayValue.unpackArray(firstValues);
        arrayValue.setOffsetRow(GenericRow.of(new GenericArray(new int[] {3, 4, 5})));
        List<ColumnValue> secondValues = new ArrayList<>();
        arrayValue.unpackArray(secondValues);

        Assert.assertSame(firstValues.get(0), secondValues.get(0));
        Assert.assertSame(firstValues.get(1), secondValues.get(1));
        Assert.assertEquals(Arrays.asList(3, 4, 5), getInts(secondValues));

        arrayValue.setOffsetRow(GenericRow.of(new GenericArray(new int[] {6})));
        List<ColumnValue> thirdValues = new ArrayList<>();
        arrayValue.unpackArray(thirdValues);
        Assert.assertSame(firstValues.get(0), thirdValues.get(0));
        Assert.assertEquals(Collections.singletonList(6), getInts(thirdValues));
    }

    @Test
    public void testReuseMapEntriesAcrossRows() {
        MapType paimonMapType = new MapType(new IntType(), new BigIntType());
        ColumnType dorisMapType = ColumnType.parseType("m", "map<int,bigint>");
        PaimonColumnValue mapValue = new PaimonColumnValue(
                GenericRow.of(new GenericMap(linkedMap(1, 10L, 2, 20L))), 0,
                dorisMapType, paimonMapType, "UTC");

        List<ColumnValue> firstKeys = new ArrayList<>();
        List<ColumnValue> firstValues = new ArrayList<>();
        mapValue.unpackMap(firstKeys, firstValues);
        mapValue.setOffsetRow(GenericRow.of(new GenericMap(linkedMap(3, 30L, 4, 40L))));
        List<ColumnValue> secondKeys = new ArrayList<>();
        List<ColumnValue> secondValues = new ArrayList<>();
        mapValue.unpackMap(secondKeys, secondValues);

        Assert.assertSame(firstKeys.get(0), secondKeys.get(0));
        Assert.assertSame(firstKeys.get(1), secondKeys.get(1));
        Assert.assertSame(firstValues.get(0), secondValues.get(0));
        Assert.assertSame(firstValues.get(1), secondValues.get(1));
        Assert.assertEquals(Arrays.asList(3, 4), getInts(secondKeys));
        Assert.assertEquals(Arrays.asList(30L, 40L), getLongs(secondValues));
    }

    @Test
    public void testReuseProjectedStructFieldsAcrossRows() {
        PaimonColumnValue structValue = createStructValue();
        List<Integer> projectedFields = Arrays.asList(0, 2);
        List<ColumnValue> firstValues = new ArrayList<>();
        structValue.unpackStruct(projectedFields, firstValues);

        structValue.setOffsetRow(createStructRow(20, "y", 200L));
        List<ColumnValue> secondValues = new ArrayList<>();
        structValue.unpackStruct(projectedFields, secondValues);

        Assert.assertSame(firstValues.get(0), secondValues.get(0));
        Assert.assertSame(firstValues.get(1), secondValues.get(1));
        Assert.assertEquals(20, secondValues.get(0).getInt());
        Assert.assertEquals(200L, secondValues.get(1).getLong());
    }

    @Test
    public void testReuseNestedArrayElementsAcrossRows() {
        ArrayType paimonNestedArrayType = new ArrayType(new ArrayType(new IntType()));
        ColumnType dorisNestedArrayType = ColumnType.parseType("a", "array<array<int>>");
        PaimonColumnValue arrayValue = new PaimonColumnValue(
                GenericRow.of(new GenericArray(new Object[] {new GenericArray(new int[] {1, 2})})), 0,
                dorisNestedArrayType, paimonNestedArrayType, "UTC");

        List<ColumnValue> firstOuterValues = new ArrayList<>();
        arrayValue.unpackArray(firstOuterValues);
        List<ColumnValue> firstInnerValues = new ArrayList<>();
        firstOuterValues.get(0).unpackArray(firstInnerValues);

        arrayValue.setOffsetRow(
                GenericRow.of(new GenericArray(new Object[] {new GenericArray(new int[] {3, 4})})));
        List<ColumnValue> secondOuterValues = new ArrayList<>();
        arrayValue.unpackArray(secondOuterValues);
        List<ColumnValue> secondInnerValues = new ArrayList<>();
        secondOuterValues.get(0).unpackArray(secondInnerValues);

        Assert.assertSame(firstOuterValues.get(0), secondOuterValues.get(0));
        Assert.assertSame(firstInnerValues.get(0), secondInnerValues.get(0));
        Assert.assertSame(firstInnerValues.get(1), secondInnerValues.get(1));
        Assert.assertEquals(Arrays.asList(3, 4), getInts(secondInnerValues));
    }

    @Test
    public void testNestedArrayCacheRetainsOnlyCurrentShape() throws Exception {
        int outerSize = 4;
        int innerSize = 32;
        ArrayType paimonNestedArrayType = new ArrayType(new ArrayType(new IntType()));
        ColumnType dorisNestedArrayType = ColumnType.parseType("a", "array<array<int>>");
        PaimonColumnValue arrayValue = new PaimonColumnValue(
                nestedArrayRow(outerSize, innerSize, 0, -1), 0,
                dorisNestedArrayType, paimonNestedArrayType, "UTC");

        consumeNestedArray(arrayValue);
        Assert.assertEquals(outerSize + innerSize, retainedCachedValues(arrayValue));

        // Moving the large child to position 1 makes position 0 a smaller, non-null array.
        arrayValue.setOffsetRow(nestedArrayRow(outerSize, innerSize, 1, -1));
        consumeNestedArray(arrayValue);
        Assert.assertEquals(outerSize + innerSize, retainedCachedValues(arrayValue));

        // Moving it again makes position 1 null, which must release that position's descendants.
        arrayValue.setOffsetRow(nestedArrayRow(outerSize, innerSize, 2, 1));
        consumeNestedArray(arrayValue);
        Assert.assertEquals(outerSize + innerSize, retainedCachedValues(arrayValue));
    }

    @Test
    public void testTimestampConversionsPreserveBoundaryValues() {
        Timestamp timestamp = Timestamp.fromEpochMillis(-1, 999_999);
        ColumnType dorisTimestampType = ColumnType.parseType("t", "datetimev2(9)");

        PaimonColumnValue timestampValue = new PaimonColumnValue(
                GenericRow.of(timestamp), 0, dorisTimestampType, new TimestampType(9), "UTC");
        Assert.assertEquals(
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
                timestampValue.getDateTime());
        Assert.assertEquals(
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
                timestampValue.getTimeStampTz());

        PaimonColumnValue localZonedValue = new PaimonColumnValue(
                GenericRow.of(Timestamp.fromInstant(Instant.parse("2024-03-10T10:30:00.123456789Z"))), 0,
                dorisTimestampType, new LocalZonedTimestampType(9), "America/Los_Angeles");
        Assert.assertEquals(
                LocalDateTime.of(2024, 3, 10, 3, 30, 0, 123_456_789),
                localZonedValue.getDateTime());

        localZonedValue.setTimeZone("Asia/Shanghai");
        Assert.assertEquals(
                LocalDateTime.of(2024, 3, 10, 18, 30, 0, 123_456_789),
                localZonedValue.getDateTime());

        localZonedValue.setTimeZone("CST");
        Assert.assertEquals(
                LocalDateTime.of(2024, 3, 10, 18, 30, 0, 123_456_789),
                localZonedValue.getDateTime());
    }

    @Test
    public void testBlobReturnsMaterializedBytes() {
        byte[] expected = new byte[] {0, 1, 2, (byte) 0xff};
        PaimonColumnValue blobValue = new PaimonColumnValue(
                GenericRow.of(Blob.fromData(expected)), 0,
                ColumnType.parseType("payload", "varbinary"), new BlobType(), "UTC");

        Assert.assertArrayEquals(expected, blobValue.getBytes());
    }

    @Test
    public void testVectorUnpacksDenseElements() {
        PaimonColumnValue vectorValue = new PaimonColumnValue(
                GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {1.5f, -2.0f, 3.25f})), 0,
                ColumnType.parseType("embedding", "array<float>"),
                new VectorType(3, new org.apache.paimon.types.FloatType(false)), "UTC");
        List<ColumnValue> values = new ArrayList<>();

        vectorValue.unpackArray(values);

        Assert.assertEquals(3, values.size());
        Assert.assertEquals(1.5f, values.get(0).getFloat(), 0.0f);
        Assert.assertEquals(-2.0f, values.get(1).getFloat(), 0.0f);
        Assert.assertEquals(3.25f, values.get(2).getFloat(), 0.0f);
    }

    @Test
    public void testVariantUnpacksLosslessBinaryChildren() {
        GenericVariant variant = GenericVariant.fromJson("{\"id\":7,\"tags\":[\"doris\"]}");
        PaimonColumnValue variantValue = new PaimonColumnValue(
                GenericRow.of(variant), 0,
                ColumnType.parseType("payload", "struct<value:varbinary,metadata:varbinary>"),
                new VariantType(), "UTC");
        List<ColumnValue> values = new ArrayList<>();

        variantValue.unpackStruct(Arrays.asList(0, 1), values);

        Assert.assertArrayEquals(variant.value(), values.get(0).getBytes());
        Assert.assertArrayEquals(variant.metadata(), values.get(1).getBytes());
    }

    private InternalRow nestedArrayRow(int outerSize, int innerSize, int populatedIndex, int nullIndex) {
        Object[] outerValues = new Object[outerSize];
        for (int i = 0; i < outerSize; i++) {
            if (i == nullIndex) {
                outerValues[i] = null;
            } else if (i == populatedIndex) {
                outerValues[i] = new GenericArray(new int[innerSize]);
            } else {
                outerValues[i] = new GenericArray(new int[0]);
            }
        }
        return GenericRow.of(new GenericArray(outerValues));
    }

    private void consumeNestedArray(PaimonColumnValue arrayValue) {
        List<ColumnValue> outerValues = new ArrayList<>();
        arrayValue.unpackArray(outerValues);
        for (ColumnValue outerValue : outerValues) {
            if (!outerValue.isNull()) {
                outerValue.unpackArray(new ArrayList<>());
            }
        }
    }

    private int retainedCachedValues(PaimonColumnValue value) throws Exception {
        int retained = 0;
        for (String fieldName : Arrays.asList("arrayValues", "mapKeys", "mapValues", "structValues")) {
            Field field = PaimonColumnValue.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            List<?> children = (List<?>) field.get(value);
            if (children == null) {
                continue;
            }
            for (Object child : children) {
                if (child != null) {
                    retained++;
                    retained += retainedCachedValues((PaimonColumnValue) child);
                }
            }
        }
        return retained;
    }

    private PaimonColumnValue createStructValue() {
        return new PaimonColumnValue(createStructRow(10, "x", 100L), 0,
                dorisStructType, paimonStructType, "UTC");
    }

    private InternalRow createStructRow(int intValue, String stringValue, long longValue) {
        GenericRow nestedRow = GenericRow.of(intValue, BinaryString.fromString(stringValue), longValue);
        RowType outerType = RowType.of(new DataType[] {paimonStructType}, new String[] {"s"});
        return new InternalRowSerializer(outerType).toBinaryRow(GenericRow.of(nestedRow));
    }

    private List<Integer> getInts(List<ColumnValue> values) {
        List<Integer> result = new ArrayList<>();
        for (ColumnValue value : values) {
            result.add(value.getInt());
        }
        return result;
    }

    private List<Long> getLongs(List<ColumnValue> values) {
        List<Long> result = new ArrayList<>();
        for (ColumnValue value : values) {
            result.add(value.getLong());
        }
        return result;
    }

    private Map<Integer, Long> linkedMap(int key1, long value1, int key2, long value2) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        result.put(key1, value1);
        result.put(key2, value2);
        return result;
    }
}
