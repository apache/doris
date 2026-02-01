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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Unit tests for {@link MergeAggregate}, specifically testing the fix for filtering
 * aggregate functions in mergeAggProjectAgg method.
 */
public class MergeAggregateTest {

    private MergeAggregate mergeAggregate;

    @BeforeEach
    public void setUp() {
        mergeAggregate = new MergeAggregate();
    }

    @Test
    public void testMergeAggProjectAggWithMixedExpressions() throws Exception {
        // This test verifies the fix at line 103-104 where we filter expressions
        // to only process those containing AggregateFunction.
        // The bug was that non-aggregate expressions (like SlotReference) were
        // being passed to rewriteAggregateFunction, which could cause errors.

        // Create inner aggregate: group by a, output a, sum(b) as sumBAlias
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Sum sumB = new Sum(b);
        Alias sumBAlias = new Alias(sumB, "sumBAlias");

        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());

        LogicalAggregate<Plan> innerAgg = new LogicalAggregate<>(
                ImmutableList.of(a),
                ImmutableList.of(a, sumBAlias),
                emptyRelation);

        // Create project: projects = [a as colA, sumBAlias]
        SlotReference colA = new SlotReference(
                sumBAlias.getExprId(), "colA", IntegerType.INSTANCE, true, ImmutableList.of());
        // Create a slot reference for sumBAlias from inner aggregate output
        Slot sumBSlot = sumBAlias.toSlot();
        LogicalProject<LogicalAggregate<Plan>> project = new LogicalProject<>(
                ImmutableList.of(colA, sumBSlot),
                innerAgg);

        // Create outer aggregate: group by colA, output colA, sum(sumBAlias)
        Slot col2FromProject = project.getOutput().get(0);
        Slot col1FromProject = project.getOutput().get(1);
        Sum sumSum = new Sum(col1FromProject);
        Alias sumSumAlias = new Alias(sumSum, "sumSum");

        // Outer aggregate output contains:
        // 1. colA (SlotReference - non-aggregate, should be filtered out)
        // 2. max(sumBAlias) (AggregateFunction - should be processed)
        // 3. sumBAlias (SlotReference - non-aggregate, should be filtered out)
        List<NamedExpression> outerAggOutput = ImmutableList.of(
                col2FromProject,
                sumSumAlias
        );

        LogicalAggregate<LogicalProject<LogicalAggregate<Plan>>> outerAgg = new LogicalAggregate<>(
                ImmutableList.of(col2FromProject),
                outerAggOutput,
                project);

        // Use reflection to call the private method
        Method method = mergeAggregate.getClass().getDeclaredMethod(
                "mergeAggProjectAgg", LogicalAggregate.class);
        method.setAccessible(true);

        // This should not throw an exception
        // Before the fix, non-aggregate expressions would be passed to rewriteAggregateFunction
        // which could cause errors. After the fix, only expressions containing AggregateFunction
        // are processed.
        Plan result = (Plan) method.invoke(mergeAggregate, outerAgg);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof LogicalProject);

        LogicalProject<Plan> resultProject = (LogicalProject<Plan>) result;
        Assertions.assertNotNull(resultProject.child(0));
        Assertions.assertTrue(resultProject.child(0) instanceof LogicalAggregate);

        LogicalAggregate<Plan> aggregate = (LogicalAggregate<Plan>) resultProject.child(0);
        Assertions.assertEquals(aggregate.getOutput().size(), 2);
    }
}
