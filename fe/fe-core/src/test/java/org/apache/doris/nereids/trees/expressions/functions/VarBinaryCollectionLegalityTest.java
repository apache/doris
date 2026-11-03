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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectSet;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayContains;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayContainsAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayEnumerateUniq;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExcept;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayIntersect;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayPosition;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRemove;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraysOverlap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CountEqual;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class VarBinaryCollectionLegalityTest {
    private List<BoundFunction> collections(Expression array, Expression value) {
        return Arrays.asList(
                new ArrayContains(array, value), new ArrayPosition(array, value), new CountEqual(array, value),
                new ArrayDistinct(array), new ArrayRemove(array, value), new ArrayEnumerateUniq(array),
                new ArrayContainsAll(array, array), new ArraysOverlap(array, array), new ArrayUnion(array, array),
                new ArrayExcept(array, array), new ArrayIntersect(array, array));
    }

    private void assertRejectsVarBinary(List<BoundFunction> functions) {
        Assertions.assertAll(functions.stream().map(function -> () -> {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityBeforeTypeCoercion, function.getName());
            Assertions.assertEquals(function.getName() + " does not support VARBINARY arguments", error.getMessage());
        }));
    }

    @Test
    public void testFunctionsRejectVarBinaryBeforeCoercion() {
        Expression value = new SlotReference("bytes", VarBinaryType.INSTANCE);
        Expression array = new SlotReference("items", ArrayType.of(VarBinaryType.INSTANCE));
        assertRejectsVarBinary(collections(array, value));
        assertRejectsVarBinary(Arrays.asList(new CollectSet(value), new CollectSet(value, new IntegerLiteral(2))));
    }

    @Test
    public void testNestedArraysRejectVarBinary() {
        Expression value = new SlotReference("bytes", ArrayType.of(VarBinaryType.INSTANCE));
        Expression array = new SlotReference("items", ArrayType.of(ArrayType.of(VarBinaryType.INSTANCE)));
        assertRejectsVarBinary(collections(array, value));
    }

    @Test
    public void testChecksAllArgumentsBeforeCoercion() {
        Expression value = new SlotReference("bytes", VarBinaryType.INSTANCE);
        Expression binaryArray = new SlotReference("bytes_array", ArrayType.of(VarBinaryType.INSTANCE));
        Expression stringArray = new SlotReference("text_array", ArrayType.of(StringType.INSTANCE));
        assertRejectsVarBinary(Arrays.asList(
                new ArrayContains(stringArray, value), new ArrayPosition(stringArray, value),
                new CountEqual(stringArray, value), new ArrayRemove(stringArray, value),
                new ArrayContainsAll(stringArray, binaryArray), new ArraysOverlap(stringArray, binaryArray),
                new ArrayExcept(stringArray, binaryArray), new ArrayUnion(stringArray, stringArray, binaryArray),
                new ArrayIntersect(stringArray, stringArray, binaryArray),
                new ArrayEnumerateUniq(stringArray, binaryArray), new CollectSet(new IntegerLiteral(1), value)));
    }

    @Test
    public void testOrdinaryTypesRemainLegal() {
        for (DataType type : Arrays.asList(IntegerType.INSTANCE, StringType.INSTANCE)) {
            Expression value = new SlotReference("value", type);
            Expression array = new SlotReference("items", ArrayType.of(type));
            for (BoundFunction function : collections(array, value)) {
                Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion, function.getName());
            }
            Assertions.assertDoesNotThrow(new CollectSet(value)::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(new CollectSet(value, new IntegerLiteral(2))::checkLegalityBeforeTypeCoercion);
        }
    }
}
