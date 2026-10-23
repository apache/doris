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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
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
                new MinTopK(value, new BigIntLiteral(2)),
                new MaxTopK(value, new BigIntLiteral(2))}) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(IntegerType.INSTANCE, signature.getArgType(0));
            Assertions.assertEquals(BigIntType.INSTANCE, signature.getArgType(1));
            // The aggregate returns the sorted list itself; truncation is inferred by the caller.
            ArrayType resultType = (ArrayType) signature.returnType;
            assertPairType(IntegerType.INSTANCE, (StructType) resultType.getItemType());
            Assertions.assertFalse(function.nullable());
        }
    }

    @Test
    void testAggregateLegality() {
        Expression value = SlotReference.of("value", IntegerType.INSTANCE);
        Expression k = SlotReference.of("k", BigIntType.INSTANCE);

        Assertions.assertThrows(AnalysisException.class,
                () -> new MinTopK(value, k).checkLegalityBeforeTypeCoercion());
        Assertions.assertThrows(AnalysisException.class,
                () -> new MinTopK(true, value, new BigIntLiteral(1))
                        .checkLegalityBeforeTypeCoercion());
        Assertions.assertThrows(AnalysisException.class,
                () -> new MinTopK(value, NullLiteral.INSTANCE).checkLegalityAfterRewrite());
        Assertions.assertThrows(AnalysisException.class,
                () -> new MinTopK(value, new BigIntLiteral(-1)).checkLegalityAfterRewrite());
        Assertions.assertThrows(AnalysisException.class,
                () -> new MaxTopK(value, new BigIntLiteral(TopKFunction.MAX_TOPK_K + 1))
                        .checkLegalityAfterRewrite());
        Assertions.assertDoesNotThrow(
                () -> new MinTopK(value, new BigIntLiteral(TopKFunction.MAX_TOPK_K))
                        .checkLegalityAfterRewrite());

        Expression varbinary = SlotReference.of("v", VarBinaryType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class,
                () -> new MinTopK(varbinary, new BigIntLiteral(1))
                        .checkLegalityBeforeTypeCoercion());
    }

}
