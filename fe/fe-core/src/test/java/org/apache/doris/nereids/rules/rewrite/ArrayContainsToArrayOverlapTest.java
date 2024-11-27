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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraysOverlap;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArrayContainsToArrayOverlapTest extends ExpressionRewriteTestHelper {

    @Test
    void testOr() {
        String sql = "select array_contains([1], 1) or array_contains([1], 2) or array_contains([1], 3);";
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(sql)
                .rewrite()
                .getPlan();
        Expression expression = plan.child(0).getExpressions().get(0).child(0);
        Assertions.assertTrue(expression instanceof ArraysOverlap);
        Assertions.assertEquals("[1]", expression.child(0).toSql());
        Assertions.assertEquals("[1, 2, 3]", expression.child(1).toSql());
    }

    @Test
    void testAnd() {
        String sql = "select array_contains([1], 1) "
                + "or array_contains([1], 2) "
                + "or array_contains([1], 3)"
                + "or array_contains([1], 4) and array_contains([1], 5);";
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(sql)
                .rewrite()
                .getPlan();
        Expression expression = plan.child(0).getExpressions().get(0).child(0);
        Assertions.assertTrue(expression instanceof Or);
        Assertions.assertTrue(expression.child(0) instanceof ArraysOverlap);
        Assertions.assertTrue(expression.child(1) instanceof And);
    }

    @Test
    void testAndOther() {
        String sql = "select bin(0) == 1 "
                + "or array_contains([1], 1) "
                + "or array_contains([1], 2) "
                + "or array_contains([1], 3) "
                + "or array_contains([1], 4) and array_contains([1], 5);";
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(sql)
                .rewrite()
                .getPlan();
        Expression expression = plan.child(0).getExpressions().get(0).child(0);
        Assertions.assertTrue(expression instanceof Or);
        Assertions.assertTrue(expression.child(0) instanceof ArraysOverlap);
        Assertions.assertTrue(expression.child(1) instanceof And);
    }

    @Test
    void testAndOverlap() {
        String sql = "select array_contains([1], 0)  "
                + "or (array_contains([1], 1) "
                + "and (array_contains([1], 2) "
                + "or array_contains([1], 3) "
                + "or array_contains([1], 4)));";
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(sql)
                .rewrite()
                .getPlan();
        Expression expression = plan.child(0).getExpressions().get(0).child(0);
        Assertions.assertEquals("(array_contains([1], 0) OR "
                        + "(array_contains([1], 1) AND arrays_overlap([1], [2, 3, 4])))",
                expression.toSql());
    }
}
