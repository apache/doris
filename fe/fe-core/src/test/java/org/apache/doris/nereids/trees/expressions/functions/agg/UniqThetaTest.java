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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Direct expression unit tests for {@link UniqTheta} (no FE service needed).
 */
public class UniqThetaTest {

    private final SlotReference intSlot = new SlotReference("col_int", IntegerType.INSTANCE);
    private final SlotReference strSlot = new SlotReference("col_str", VarcharType.SYSTEM_DEFAULT);
    private final SlotReference hllSlot = new SlotReference("col_hll", HllType.INSTANCE);

    @Test
    public void testOneArgConstructor() {
        UniqTheta func = new UniqTheta(intSlot);
        Assertions.assertEquals("uniq_theta", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertEquals(intSlot, func.getArgument(0));
        Assertions.assertFalse(func.isDistinct());
        Assertions.assertEquals(BigIntType.INSTANCE, func.getDataType());
    }

    @Test
    public void testDistinctConstructorIgnoresFlag() {
        UniqTheta func = new UniqTheta(true, intSlot);
        Assertions.assertEquals("uniq_theta", func.getName());
        Assertions.assertEquals(intSlot, func.getArgument(0));
        // distinct is absorbed (theta is already an approximate distinct)
        Assertions.assertFalse(func.isDistinct());
    }

    @Test
    public void testNotNullable() {
        UniqTheta func = new UniqTheta(intSlot);
        Assertions.assertFalse(func.nullable());
    }

    @Test
    public void testGetSignatures() {
        UniqTheta func = new UniqTheta(intSlot);
        Assertions.assertEquals(1, func.getSignatures().size());
        Assertions.assertSame(UniqTheta.SIGNATURES, func.getSignatures());
        Assertions.assertEquals(BigIntType.INSTANCE, func.getSignatures().get(0).returnType);
    }

    @Test
    public void testStaticSignatures() {
        Assertions.assertEquals(1, UniqTheta.SIGNATURES.size());
        Assertions.assertEquals(BigIntType.INSTANCE, UniqTheta.SIGNATURES.get(0).returnType);
        Assertions.assertEquals(1, UniqTheta.SIGNATURES.get(0).argumentsTypes.size());
    }

    @Test
    public void testWithDistinctAndChildren() {
        UniqTheta func = new UniqTheta(intSlot);
        UniqTheta replaced = func.withDistinctAndChildren(false, ImmutableList.of(strSlot));
        Assertions.assertEquals(1, replaced.arity());
        Assertions.assertEquals(strSlot, replaced.getArgument(0));
        Assertions.assertEquals(BigIntType.INSTANCE, replaced.getDataType());
    }

    @Test
    public void testWithChildren() {
        UniqTheta func = new UniqTheta(intSlot);
        Expression replaced = func.withChildren(ImmutableList.of(strSlot));
        Assertions.assertTrue(replaced instanceof UniqTheta);
        Assertions.assertEquals(strSlot, ((UniqTheta) replaced).getArgument(0));
    }

    @Test
    public void testWithDistinctAndChildrenIllegalArity() {
        UniqTheta func = new UniqTheta(intSlot);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> func.withDistinctAndChildren(false, ImmutableList.of(intSlot, strSlot)));
    }

    @Test
    public void testResultForEmptyInput() {
        UniqTheta func = new UniqTheta(intSlot);
        Expression empty = func.resultForEmptyInput();
        Assertions.assertTrue(empty instanceof BigIntLiteral);
        Assertions.assertEquals(0L, ((BigIntLiteral) empty).getValue().longValue());
    }

    @Test
    public void testCheckLegalityAcceptsNonMetricType() {
        UniqTheta func = new UniqTheta(intSlot);
        Assertions.assertDoesNotThrow(func::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testCheckLegalityRejectsMetricType() {
        UniqTheta func = new UniqTheta(hllSlot);
        Assertions.assertThrows(AnalysisException.class, func::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testAcceptVisitorDispatch() {
        UniqTheta func = new UniqTheta(intSlot);
        DefaultExpressionVisitor<String, Void> visitor = new DefaultExpressionVisitor<String, Void>() {
            @Override
            public String visitUniqTheta(UniqTheta f, Void ctx) {
                return "visited_uniq_theta";
            }
        };
        Assertions.assertEquals("visited_uniq_theta", func.accept(visitor, null));
    }

    @Test
    public void testAcceptVisitorDefaultDispatch() {
        // Exercises the default visitUniqTheta -> visitAggregateFunction path.
        UniqTheta func = new UniqTheta(intSlot);
        DefaultExpressionVisitor<String, Void> visitor = new DefaultExpressionVisitor<String, Void>() {
            @Override
            public String visitAggregateFunction(AggregateFunction f, Void ctx) {
                return "visited_agg_function";
            }
        };
        Assertions.assertEquals("visited_agg_function", func.accept(visitor, null));
    }
}
