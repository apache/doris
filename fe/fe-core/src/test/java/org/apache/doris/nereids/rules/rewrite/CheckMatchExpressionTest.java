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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

class CheckMatchExpressionTest {

    private CheckMatchExpression checkMatchExpression;
    private Method checkChildrenMethod;

    @BeforeEach
    void setUp() throws Exception {
        checkMatchExpression = new CheckMatchExpression();
        checkChildrenMethod = CheckMatchExpression.class.getDeclaredMethod("checkChildren", LogicalFilter.class);
        checkChildrenMethod.setAccessible(true);
    }

    @Test
    void testRejectsRootVariantMatch() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        MatchAny match = new MatchAny(rootVariantSlot, new StringLiteral("doris"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(match));
        Assertions.assertTrue(exception.getMessage().contains("VARIANT root column does not support MATCH"),
                exception.getMessage());
    }

    @Test
    void testRejectsCastOnRootVariantMatch() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        MatchAny match = new MatchAny(new Cast(rootVariantSlot, StringType.INSTANCE), new StringLiteral("doris"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(match));
        Assertions.assertTrue(exception.getMessage().contains("VARIANT root column does not support MATCH"),
                exception.getMessage());
    }

    @Test
    void testAllowsVariantSubcolumnMatch() {
        SlotReference variantSubcolumnSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList())
                .withSubPath(Arrays.asList("msg"));
        MatchAny match = new MatchAny(variantSubcolumnSlot, new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    private void invokeCheck(Expression expression) throws Throwable {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(expression), scan);
        try {
            checkChildrenMethod.invoke(checkMatchExpression, filter);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
