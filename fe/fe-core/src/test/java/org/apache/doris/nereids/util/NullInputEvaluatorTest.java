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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NullInputEvaluatorTest {

    private final ExpressionRewriteContext context = new ExpressionRewriteContext(
            MemoTestUtils.createCascadesContext(
                    new UnboundRelation(new RelationId(1), ImmutableList.of("test_table"))));
    private final SlotReference slot = new SlotReference("value", IntegerType.INSTANCE, true);

    @Test
    void classifiesFullyFoldedLiteralResults() {
        Assertions.assertEquals(NullInputEvaluator.Result.NULL, evaluate(slot));
        Assertions.assertEquals(NullInputEvaluator.Result.FALSE,
                evaluate(new Not(new IsNull(slot))));
        Assertions.assertEquals(NullInputEvaluator.Result.TRUE, evaluate(new IsNull(slot)));
        Assertions.assertEquals(NullInputEvaluator.Result.TRUE,
                evaluate(new NullSafeEqual(slot, new NullLiteral(IntegerType.INSTANCE))));
        Assertions.assertEquals(NullInputEvaluator.Result.OTHER_NON_NULL,
                evaluate(new Coalesce(slot, new IntegerLiteral(7))));
    }

    @Test
    void returnsUnknownForIncompleteFoldAndException() {
        SlotReference unboundInput = new SlotReference("other", IntegerType.INSTANCE, true);
        Assertions.assertEquals(NullInputEvaluator.Result.UNKNOWN, evaluate(unboundInput));

        Expression brokenExpression = Mockito.mock(Expression.class);
        Mockito.when(brokenExpression.rewriteDownShortCircuit(Mockito.any()))
                .thenThrow(new IllegalStateException("synthetic rewrite failure"));
        Assertions.assertEquals(NullInputEvaluator.Result.UNKNOWN,
                NullInputEvaluator.evaluate(brokenExpression, ImmutableSet.of(), context));
    }

    @Test
    void returnsUnknownWhenConstantFoldingIsSkipped() {
        context.cascadesContext.getConnectContext().getSessionVariable().setDebugSkipFoldConstant(true);
        try {
            Assertions.assertEquals(NullInputEvaluator.Result.UNKNOWN,
                    evaluateOnFE(new Coalesce(slot, new IntegerLiteral(7))));
        } finally {
            context.cascadesContext.getConnectContext().getSessionVariable().setDebugSkipFoldConstant(false);
        }
    }

    @Test
    void evaluatesOnFEWithoutUsingBeFolding() {
        context.cascadesContext.getConnectContext().getSessionVariable().setEnableFoldConstantByBe(true);
        try {
            Assertions.assertEquals(NullInputEvaluator.Result.OTHER_NON_NULL,
                    evaluateOnFE(new Coalesce(slot, new IntegerLiteral(7))));
            Assertions.assertEquals(NullInputEvaluator.Result.NULL,
                    evaluateOnFE(new Add(slot, new Cast(new IsNull(slot), IntegerType.INSTANCE))));
        } finally {
            context.cascadesContext.getConnectContext().getSessionVariable().setEnableFoldConstantByBe(false);
        }
    }

    private NullInputEvaluator.Result evaluate(Expression expression) {
        return NullInputEvaluator.evaluate(expression, ImmutableSet.of(slot), context);
    }

    private NullInputEvaluator.Result evaluateOnFE(Expression expression) {
        return NullInputEvaluator.evaluateOnFE(expression, ImmutableSet.of(slot), context);
    }
}
