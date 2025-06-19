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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CheckMultiDistinctTest implements MemoPatternMatchSupported {
    private Plan rStudent;
    private Slot id;
    private Slot name;
    private Slot age;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        id = rStudent.getOutput().get(0);
        name = rStudent.getOutput().get(1);
        age = rStudent.getOutput().get(2);
    }

    @Test
    public void testSupportedFunctionsAllowMultiDistinct() {
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Count(true, id), "count_distinct_id"),
                new Alias(new Sum(true, age), "sum_distinct_age"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for supported functions: " + e.getMessage());
        }
    }

    @Test
    public void testCollectListMultiDistinctSupported() {
        // Test that multiple distinct CollectList operations are supported
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new CollectList(true, name), "collect_distinct_name"),
                new Alias(new CollectList(true, age), "collect_distinct_age"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for CollectList multi-distinct: " + e.getMessage());
        }
    }

    @Test
    public void testArrayAggMultiDistinctSupported() {
        // Test that multiple distinct ArrayAgg operations are supported
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new ArrayAgg(true, name), "array_agg_distinct_name"),
                new Alias(new ArrayAgg(true, age), "array_agg_distinct_age"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for ArrayAgg multi-distinct: " + e.getMessage());
        }
    }

    @Test
    public void testMixedSupportedFunctionsMultiDistinct() {
        // Test mixing different supported multi-distinct functions
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Count(true, id), "count_distinct_id"),
                new Alias(new CollectList(true, name), "collect_distinct_name"),
                new Alias(new ArrayAgg(true, age), "array_agg_distinct_age"),
                new Alias(new GroupConcat(true, new StringLiteral(","), name), "group_concat_distinct_name"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for mixed multi-distinct functions: " + e.getMessage());
        }
    }

    @Test
    public void testCollectListWithLimitMultiDistinctSupported() {
        // Test that CollectList with limit parameter supports multi-distinct
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new CollectList(true, name, new IntegerLiteral(10)), "collect_distinct_name_limit"),
                new Alias(new CollectList(true, age, new IntegerLiteral(5)), "collect_distinct_age_limit"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for CollectList with limit: " + e.getMessage());
        }
    }

    @Test
    public void testNonDistinctFunctionsStillWork() {
        // Test that non-distinct versions of the functions still work normally
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new CollectList(false, name), "collect_name"),
                new Alias(new ArrayAgg(false, age), "array_agg_age"),
                new Alias(new Count(false, id), "count_id"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // Should not throw exception - if CheckMultiDistinct passes, the plan should remain valid
        try {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyBottomUp(new CheckMultiDistinct());
            // If we reach here, no exception was thrown, which means the test passed
        } catch (Exception e) {
            Assertions.fail("CheckMultiDistinct should not throw exception for non-distinct functions: " + e.getMessage());
        }
    }

}
