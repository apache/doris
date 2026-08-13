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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StackTest {

    @Test
    public void testMultiColumnSignature() {
        Stack stack = new Stack(new IntegerLiteral(2), new IntegerLiteral(1),
                new StringLiteral("a"), new IntegerLiteral(2), new StringLiteral("b"));

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertEquals(5, signature.argumentsTypes.size());
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
        Assertions.assertEquals(StringType.INSTANCE, signature.argumentsTypes.get(2));
        Assertions.assertTrue(signature.returnType.isStructType());
        StructType returnType = (StructType) signature.returnType;
        Assertions.assertEquals(2, returnType.getFields().size());
        Assertions.assertEquals("col0", returnType.getFields().get(0).getName());
        Assertions.assertEquals(IntegerType.INSTANCE, returnType.getFields().get(0).getDataType());
        Assertions.assertEquals("col1", returnType.getFields().get(1).getName());
        Assertions.assertEquals(StringType.INSTANCE, returnType.getFields().get(1).getDataType());
    }

    @Test
    public void testNullUsesOutputColumnType() {
        Stack stack = new Stack(new IntegerLiteral(2), new IntegerLiteral(1),
                new StringLiteral("a"), new NullLiteral(), new StringLiteral("b"));

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(3));
        StructType returnType = (StructType) signature.returnType;
        Assertions.assertEquals(IntegerType.INSTANCE, returnType.getFields().get(0).getDataType());
        Assertions.assertEquals(StringType.INSTANCE, returnType.getFields().get(1).getDataType());
    }

    @Test
    public void testSingleColumnSignature() {
        Stack stack = new Stack(new IntegerLiteral(4), new IntegerLiteral(1),
                new IntegerLiteral(2), new IntegerLiteral(3));

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertEquals(IntegerType.INSTANCE, signature.returnType);
        Assertions.assertEquals(4, signature.argumentsTypes.size());
    }

    @Test
    public void testFoldableNumRowsExpression() {
        Stack stack = new Stack(
                new Cast(new Subtract(new IntegerLiteral(3), new IntegerLiteral(1)), IntegerType.INSTANCE),
                new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3));

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertTrue(signature.returnType.isStructType());
        Assertions.assertEquals(2, ((StructType) signature.returnType).getFields().size());
    }

    @Test
    public void testCardinalityNumRowsExpression() {
        Stack stack = new Stack(new Cardinality(new Array(new IntegerLiteral(1), new IntegerLiteral(2))),
                new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3));

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertTrue(signature.returnType.isStructType());
        Assertions.assertEquals(2, ((StructType) signature.returnType).getFields().size());
    }

    @Test
    public void testAllNullOutputColumn() {
        Stack stack = new Stack(new IntegerLiteral(2), new NullLiteral(), new NullLiteral());

        FunctionSignature signature = stack.getSignatures().get(0);
        Assertions.assertEquals(NullType.INSTANCE, signature.returnType);
    }

    @Test
    public void testInvalidArguments() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new Stack(new IntegerLiteral(0), new IntegerLiteral(1)).getSignatures());
        Assertions.assertThrows(AnalysisException.class,
                () -> new Stack(SlotReference.of("n", IntegerType.INSTANCE),
                        new IntegerLiteral(1)).getSignatures());
        Assertions.assertThrows(AnalysisException.class,
                () -> new Stack(new ConnectionId(), new IntegerLiteral(1)).getSignatures());
        Assertions.assertThrows(AnalysisException.class,
                () -> new Stack(new IntegerLiteral(2), new IntegerLiteral(1),
                        new StringLiteral("a")).getSignatures());
    }
}
