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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MapConstructionFunctionsTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void testMapFromArraysCanBeAnalyzed() {
        Expression map = analyze("map_from_arrays([1, 2], [10, 20])");
        Assertions.assertTrue(map instanceof MapFromArrays);
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, TinyIntType.INSTANCE), map.getDataType());

        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_from_arrays([1, 2], 10)"));

        AnalysisException complexKeyException = Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_arrays([[1]], [10])"));
        Assertions.assertTrue(complexKeyException.getMessage().contains(
                "MAP key type must be a primitive type"), complexKeyException::getMessage);

        assertMapFromArraysSignature("map_from_arrays([null], [null])",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromArraysSignature("map_from_arrays(null, null)",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromArraysSignature("map_from_arrays([], [])",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromArraysSignature(
                "map_from_arrays(map_keys(map(null, null)), map_values(map(null, null)))",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromArraysSignature("map_from_arrays([1], [[null]])",
                TinyIntType.INSTANCE, ArrayType.of(TinyIntType.INSTANCE));
    }

    @Test
    public void testMapFromArraysPreservesIndependentPrecision() {
        DataType decimalKeyType = DecimalV3Type.createDecimalV3Type(38, 0);
        DataType decimalValueType = DecimalV3Type.createDecimalV3Type(38, 38);
        assertMapFromArraysSignature(
                "map_from_arrays(cast([1] as array<decimalv3(38, 0)>),"
                        + " cast([0.12345678901234567890123456789012345678]"
                        + " as array<decimalv3(38, 38)>))",
                decimalKeyType, decimalValueType);

        Expression map = analyze(
                "map_from_arrays(cast(['2026-01-01 00:00:00'] as array<datetimev2(0)>),"
                        + " array(struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6)))))");
        FunctionSignature signature = ((MapFromArrays) map).getSignature();
        DataType keyType = ((ArrayType) signature.getArgType(0)).getItemType();
        DataType valueType = ((ArrayType) signature.getArgType(1)).getItemType();
        Assertions.assertEquals(DateTimeV2Type.of(0), keyType);
        Assertions.assertTrue(valueType instanceof StructType);
        Assertions.assertEquals(DateTimeV2Type.of(6),
                ((StructType) valueType).getFields().get(0).getDataType());
        Assertions.assertEquals(MapType.of(keyType, valueType), signature.returnType);
    }

    @Test
    public void testMapFromEntriesCanBeAnalyzed() {
        Expression map = analyze("map_from_entries(array(struct(1, 10), struct(2, 20)))");
        Assertions.assertTrue(map instanceof MapFromEntries);
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, TinyIntType.INSTANCE), map.getDataType());

        Expression nullMap = analyze("map_from_entries(NULL)");
        Assertions.assertTrue(nullMap instanceof MapFromEntries);
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, TinyIntType.INSTANCE), nullMap.getDataType());

        Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(1)"));
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(array(struct(1)))"));
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(array(struct(1, 2, 3)))"));

        AnalysisException complexKeyException = Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(array(struct([1], 10)))"));
        Assertions.assertTrue(complexKeyException.getMessage().contains(
                "MAP key type must be a primitive type"), complexKeyException::getMessage);

        assertMapFromEntriesSignature("map_from_entries([])",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromEntriesSignature("map_from_entries([null])",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromEntriesSignature("map_from_entries(array(struct(1, null)))",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromEntriesSignature("map_from_entries(map_entries(map(null, null)))",
                TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        assertMapFromEntriesSignature("map_from_entries(array(struct(1, [null])))",
                TinyIntType.INSTANCE, ArrayType.of(TinyIntType.INSTANCE));
    }

    @Test
    public void testMapFromEntriesPreservesIndependentPrecision() {
        DataType decimalKeyType = DecimalV3Type.createDecimalV3Type(38, 0);
        DataType decimalValueType = DecimalV3Type.createDecimalV3Type(38, 38);
        assertMapFromEntriesSignature(
                "map_from_entries(map_entries(map(cast(1 as decimalv3(38, 0)),"
                        + " cast(0.12345678901234567890123456789012345678"
                        + " as decimalv3(38, 38)))))",
                decimalKeyType, decimalValueType);

        DataType timeKeyType = DateTimeV2Type.of(0);
        DataType timeValueType = new StructType(ImmutableList.of(
                new StructField("col1", DateTimeV2Type.of(6), true, "")));
        assertMapFromEntriesSignature(
                "map_from_entries(map_entries(map("
                        + "cast('2026-01-01 00:00:00' as datetimev2(0)),"
                        + " struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6))))))",
                timeKeyType, timeValueType);
    }

    @Test
    public void testMapEntriesPreservesIndependentPrecision() {
        DataType decimalKeyType = DecimalV3Type.createDecimalV3Type(38, 0);
        DataType decimalValueType = DecimalV3Type.createDecimalV3Type(38, 38);
        assertMapEntriesSignature(
                "map_entries(map(cast(1 as decimalv3(38, 0)),"
                        + " cast(0.12345678901234567890123456789012345678"
                        + " as decimalv3(38, 38))))",
                decimalKeyType, decimalValueType);

        DataType timeKeyType = DateTimeV2Type.of(0);
        DataType timeValueType = new StructType(ImmutableList.of(
                new StructField("col1", DateTimeV2Type.of(6), true, "")));
        assertMapEntriesSignature(
                "map_entries(map(cast('2026-01-01 00:00:00' as datetimev2(0)),"
                        + " struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6)))))",
                timeKeyType, timeValueType);
    }

    private void assertMapFromArraysSignature(String sql, DataType keyType, DataType valueType) {
        Expression map = analyze(sql);
        Assertions.assertTrue(map instanceof MapFromArrays);
        FunctionSignature signature = ((MapFromArrays) map).getSignature();
        Assertions.assertEquals(MapType.of(keyType, valueType), signature.returnType);
        Assertions.assertEquals(ArrayType.of(keyType), signature.getArgType(0));
        Assertions.assertEquals(ArrayType.of(valueType), signature.getArgType(1));
        assertNoNullType(signature);
    }

    private void assertMapFromEntriesSignature(String sql, DataType keyType, DataType valueType) {
        Expression map = analyze(sql);
        Assertions.assertTrue(map instanceof MapFromEntries);
        FunctionSignature signature = ((MapFromEntries) map).getSignature();
        Assertions.assertEquals(MapType.of(keyType, valueType), signature.returnType);
        Assertions.assertTrue(signature.getArgType(0) instanceof ArrayType);
        DataType itemType = ((ArrayType) signature.getArgType(0)).getItemType();
        Assertions.assertTrue(itemType instanceof StructType);
        Assertions.assertEquals(keyType, ((StructType) itemType).getFields().get(0).getDataType());
        Assertions.assertEquals(valueType, ((StructType) itemType).getFields().get(1).getDataType());
        assertNoNullType(signature);
    }

    private void assertMapEntriesSignature(String sql, DataType keyType, DataType valueType) {
        Expression entries = analyze(sql);
        Assertions.assertTrue(entries instanceof MapEntries);
        FunctionSignature signature = ((MapEntries) entries).getSignature();
        MapType mapType = MapType.of(keyType, valueType);
        Assertions.assertEquals(mapType, signature.getArgType(0));
        Assertions.assertEquals(mapType, entries.child(0).getDataType());
        DataType itemType = ((ArrayType) signature.returnType).getItemType();
        Assertions.assertTrue(itemType instanceof StructType);
        Assertions.assertEquals(keyType, ((StructType) itemType).getFields().get(0).getDataType());
        Assertions.assertEquals(valueType, ((StructType) itemType).getFields().get(1).getDataType());
    }

    private void assertNoNullType(FunctionSignature signature) {
        signature.returnType.validateDataType();
        for (DataType argumentType : signature.argumentsTypes) {
            argumentType.validateDataType();
        }
    }

    private Expression analyze(String sql) {
        return ExpressionRewriteTestHelper.typeCoercion(PARSER.parseExpression(sql));
    }
}
