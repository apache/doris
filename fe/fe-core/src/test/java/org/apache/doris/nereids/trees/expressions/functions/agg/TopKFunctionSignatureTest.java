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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TopKMergeMax;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TopKMergeMin;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopKFunctionSignatureTest {

    private static void assertPairType(DataType expectedValueType, StructType pairType) {
        Assertions.assertEquals(2, pairType.getFields().size());
        Assertions.assertEquals("value", pairType.getFields().get(0).getName());
        Assertions.assertEquals(expectedValueType, pairType.getField("value").getDataType());
        Assertions.assertFalse(pairType.getField("value").isNullable());
        Assertions.assertEquals("count", pairType.getFields().get(1).getName());
        Assertions.assertEquals(BigIntType.INSTANCE, pairType.getField("count").getDataType());
        Assertions.assertFalse(pairType.getField("count").isNullable());
    }

    @Test
    void testAggregateReturnType() {
        Expression value = SlotReference.of("value", IntegerType.INSTANCE);
        for (AggregateFunction function : new AggregateFunction[] {
                new TopKMinCount(value, new BigIntLiteral(2)),
                new TopKMaxCount(value, new BigIntLiteral(2))}) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(IntegerType.INSTANCE, signature.getArgType(0));
            Assertions.assertEquals(BigIntType.INSTANCE, signature.getArgType(1));
            StructType resultType = (StructType) signature.returnType;
            Assertions.assertEquals(2, resultType.getFields().size());
            Assertions.assertFalse(resultType.getField("values").isNullable());
            Assertions.assertTrue(resultType.getField("values").getDataType() instanceof ArrayType);
            StructType pairType = (StructType) ((ArrayType) resultType.getField("values").getDataType())
                    .getItemType();
            assertPairType(IntegerType.INSTANCE, pairType);
            // bound is nullable even for a NOT NULL input type, because "not truncated" is a NULL.
            Assertions.assertTrue(resultType.getField("bound").isNullable());
            Assertions.assertEquals(IntegerType.INSTANCE, resultType.getField("bound").getDataType());
            Assertions.assertFalse(function.nullable());
        }
    }

    @Test
    void testAggregateLegality() {
        Expression value = SlotReference.of("value", IntegerType.INSTANCE);
        Expression k = SlotReference.of("k", BigIntType.INSTANCE);

        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMinCount(value, k).checkLegalityBeforeTypeCoercion());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMinCount(true, value, new BigIntLiteral(1))
                        .checkLegalityBeforeTypeCoercion());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMinCount(value, NullLiteral.INSTANCE).checkLegalityAfterRewrite());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMinCount(value, new BigIntLiteral(-1)).checkLegalityAfterRewrite());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMaxCount(value, new BigIntLiteral(TopKCountFunction.MAX_TOPK_K + 1))
                        .checkLegalityAfterRewrite());
        Assertions.assertDoesNotThrow(
                () -> new TopKMinCount(value, new BigIntLiteral(TopKCountFunction.MAX_TOPK_K))
                        .checkLegalityAfterRewrite());

        Expression varbinary = SlotReference.of("v", VarBinaryType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class,
                () -> new TopKMinCount(varbinary, new BigIntLiteral(1))
                        .checkLegalityBeforeTypeCoercion());
    }

    @Test
    void testMergeSignature() {
        DataType valueType = IntegerType.INSTANCE;
        ArrayType listType = ArrayType.of(TopKCountFunction.pairType(valueType));
        Expression value = SlotReference.of("value", valueType);
        Expression list = SlotReference.of("list", listType);
        Expression n = new BigIntLiteral(1);

        for (ScalarFunction function : new ScalarFunction[] {
                new TopKMergeMin(list, value, list, value, list, value, n, n, n),
                new TopKMergeMax(list, value, list, value, list, value, n, n, n)}) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(9, signature.arity);
            Assertions.assertEquals(listType, signature.getArgType(0));
            Assertions.assertEquals(valueType, signature.getArgType(1));
            Assertions.assertEquals(BigIntType.INSTANCE, signature.getArgType(8));

            // The merge result is the state itself, identical to the aggregate's return type.
            StructType resultType = (StructType) signature.returnType;
            Assertions.assertEquals(TopKCountFunction.resultType(valueType), resultType);
            Assertions.assertEquals(2, resultType.getFields().size());
            assertPairType(valueType, (StructType) ((ArrayType) resultType.getField("values")
                    .getDataType()).getItemType());
            Assertions.assertTrue(resultType.getField("bound").isNullable());
            Assertions.assertFalse(function.nullable());
        }
    }

    @Test
    void testMergeKeepsDecimalPrecisionWithNullBounds() {
        // Untyped NULL bounds must not contribute a DECIMAL(1,0) that narrows the nested lists.
        DecimalV3Type valueType = DecimalV3Type.createDecimalV3Type(4, 2);
        ArrayType listType = ArrayType.of(TopKCountFunction.pairType(valueType));
        Expression list = SlotReference.of("list", listType);
        Expression n = new BigIntLiteral(1);
        TopKMergeMin function = new TopKMergeMin(list, NullLiteral.INSTANCE, list,
                NullLiteral.INSTANCE, list, NullLiteral.INSTANCE, n, n, n);

        FunctionSignature signature = function.getSignature();
        Assertions.assertEquals(listType, signature.getArgType(0));
        Assertions.assertEquals(valueType, signature.getArgType(1));
        StructType resultType = (StructType) signature.returnType;
        Assertions.assertEquals(valueType, resultType.getField("bound").getDataType());
        StructType pairType = (StructType) ((ArrayType) resultType.getField("values").getDataType())
                .getItemType();
        Assertions.assertEquals(valueType, pairType.getField("value").getDataType());
    }

    @Test
    void testMergeRejectsMixedValueTypes() {
        // Coercing the DOUBLE list to INT would merge 1.1 and 1.9 into one entry.
        DataType intList = ArrayType.of(TopKCountFunction.pairType(IntegerType.INSTANCE));
        DataType doubleList = ArrayType.of(TopKCountFunction.pairType(DoubleType.INSTANCE));
        Expression n = new BigIntLiteral(1);
        TopKMergeMin function = new TopKMergeMin(
                SlotReference.of("s", intList), NullLiteral.INSTANCE,
                SlotReference.of("i", doubleList), NullLiteral.INSTANCE,
                SlotReference.of("d", intList), NullLiteral.INSTANCE, n, n, n);
        Assertions.assertThrows(AnalysisException.class, function::checkLegalityBeforeTypeCoercion);
    }
}
