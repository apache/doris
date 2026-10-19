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
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentCatalog;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class StackTest {

    @Test
    public void testNumRowsLazyCompute() {
        AtomicInteger computeCount = new AtomicInteger();
        IntegerLiteral numRows = new IntegerLiteral(2) {
            @Override
            public boolean isConstant() {
                computeCount.incrementAndGet();
                return super.isConstant();
            }
        };
        Stack stack = new Stack(numRows, new IntegerLiteral(1),
                new IntegerLiteral(2), new IntegerLiteral(3));

        Assertions.assertEquals(0, computeCount.get());
        Assertions.assertEquals(2, stack.getOutputColumnCount());
        Assertions.assertEquals(2, stack.getOutputColumnCount());
        Assertions.assertEquals(1, computeCount.get());
    }

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
                () -> new Stack(new CurrentCatalog(), new IntegerLiteral(1)).getSignatures());
        Assertions.assertThrows(AnalysisException.class,
                () -> new Stack(new IntegerLiteral(2), new IntegerLiteral(1),
                        new StringLiteral("a")).getSignatures());
    }

    @Test
    public void testContextDependentNumRowsExpression() {
        AnalysisException keyException = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                        "select c1 from (select 1) t "
                                + "lateral view stack(KEY test_db.test_key, 1) s as c1"));
        Assertions.assertTrue(keyException.getMessage().contains(
                "The first argument of stack must be a positive constant integer"), keyException.getMessage());

        AnalysisException castKeyException = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                        "select c1 from (select 1) t "
                                + "lateral view stack(CAST(KEY test_db.test_key AS INT), 1) s as c1"));
        Assertions.assertTrue(castKeyException.getMessage().contains(
                "The first argument of stack must be a positive constant integer"), castKeyException.getMessage());
    }

    @Test
    public void testMultiColumnAliasCount() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                        "select c1 from (select 1) t "
                                + "lateral view stack(2, 1, 2, 3, 4, 5) s as c1"));
        Assertions.assertTrue(exception.getMessage().contains(
                "table s has 3 columns available but 1 columns specified"));

        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select c1, c2, c3 from (select 1) t "
                        + "lateral view stack(2, 1, 2, 3, 4, 5) s as c1, c2, c3");
    }

    @Test
    public void testSingleStructOutputColumn() {
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select c.a from (select 1) t "
                        + "lateral view stack(2, named_struct('a', 1), named_struct('a', 2)) s as c");
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select c.a, c.b from (select 1) t lateral view stack(2, "
                        + "named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)) s as c");
    }
}
