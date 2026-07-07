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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class CheckMultiDistinctTest {
    private final CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);

    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student, ImmutableList.of(""));
    private final Slot id = scan.getOutput().get(0);
    private final Slot name = scan.getOutput().get(2);
    private final Slot age = scan.getOutput().get(3);
    private final GroupExpression scanGroupExpr = new GroupExpression(scan, ImmutableList.of());
    private final GroupPlan childGroup = new GroupPlan(
            new Group(GroupId.createGenerator().getNextId(), scanGroupExpr.getPlan().getLogicalProperties()));

    private List<Plan> applyCheckMultiDistinct(Plan agg) {
        return new CheckMultiDistinct().build().transform(agg, cascadesContext);
    }

    private Plan buildAgg(List<NamedExpression> outputs) {
        return new LogicalAggregate<>(Lists.newArrayList(), outputs, true, Optional.empty(), childGroup);
    }

    @Test
    public void testSupportedFunctionsAllowMultiDistinct() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Count(true, id), "count_distinct_id"),
                new Alias(new Sum(true, age), "sum_distinct_age"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testCollectListMultiDistinctSupported() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new CollectList(true, name), "collect_distinct_name"),
                new Alias(new CollectList(true, age), "collect_distinct_age"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testArrayAggMultiDistinctSupported() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new ArrayAgg(true, name), "array_agg_distinct_name"),
                new Alias(new ArrayAgg(true, age), "array_agg_distinct_age"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testMixedSupportedFunctionsMultiDistinct() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Count(true, id), "count_distinct_id"),
                new Alias(new CollectList(true, name), "collect_distinct_name"),
                new Alias(new ArrayAgg(true, age), "array_agg_distinct_age"),
                new Alias(new GroupConcat(true, name, new StringLiteral(",")), "group_concat_distinct_name"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testCollectListWithLimitMultiDistinctSupported() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new CollectList(true, name, new IntegerLiteral(10)), "collect_distinct_name_limit"),
                new Alias(new CollectList(true, age, new IntegerLiteral(5)), "collect_distinct_age_limit"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testNonDistinctFunctionsStillWork() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new CollectList(false, name), "collect_name"),
                new Alias(new ArrayAgg(false, age), "array_agg_age"),
                new Alias(new Count(false, id), "count_id"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }

    @Test
    public void testUnsupportedDistinctFunctionThrows() {
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new Avg(true, age), "avg_distinct_age"),
                new Alias(new CollectList(true, name), "collect_distinct_name"));
        Plan root = buildAgg(outputs);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> applyCheckMultiDistinct(root));
        Assertions.assertTrue(exception.getMessage().contains("can't support multi distinct"));
    }

    @Test
    public void testArrayAggDistinctComplexTypeThrows() {
        // array_agg(distinct <array-typed>) plus a second distinct argument forces the
        // multi-distinct rewrite; the set-based BE path can't dedup complex element types, so
        // CheckMultiDistinct must reject it up front with a user-facing error.
        ArrayLiteral arrayArg = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1)));
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new ArrayAgg(true, arrayArg), "array_agg_distinct_arr"),
                new Alias(new Count(true, id), "count_distinct_id"));
        Plan root = buildAgg(outputs);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> applyCheckMultiDistinct(root));
        Assertions.assertTrue(exception.getMessage().contains("does not support type"));
    }

    @Test
    public void testCollectListDistinctComplexTypeThrows() {
        ArrayLiteral arrayArg = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1)));
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new CollectList(true, arrayArg), "collect_distinct_arr"),
                new Alias(new Count(true, id), "count_distinct_id"));
        Plan root = buildAgg(outputs);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> applyCheckMultiDistinct(root));
        Assertions.assertTrue(exception.getMessage().contains("does not support type"));
    }

    @Test
    public void testSingleDistinctComplexTypeAllowed() {
        // With only one distinct argument the multi-distinct rewrite does not fire (the distinct
        // arg is pushed into the group-by key), so complex-typed array_agg(distinct) is fine.
        ArrayLiteral arrayArg = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1)));
        List<NamedExpression> outputs = Lists.newArrayList(
                new Alias(new ArrayAgg(true, arrayArg), "array_agg_distinct_arr"));
        Assertions.assertDoesNotThrow(() -> applyCheckMultiDistinct(buildAgg(outputs)));
    }
}
