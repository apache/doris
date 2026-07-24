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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizeMultiColumnDistinctCountTest implements MemoPatternMatchSupported {
    @Test
    public void testNormalizeDifferentArgumentOrder() {
        Plan student = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of());
        Slot id = student.getOutput().get(0);
        Slot name = student.getOutput().get(2);
        Alias countIdName = new Alias(new Count(true, id, name), "count_id_name");
        Alias countNameId = new Alias(new Count(true, name, id), "count_name_id");
        LogicalAggregate<Plan> root = new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of(countIdName, countNameId), student);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(
                        logicalProject(
                                logicalAggregate(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                ).when(aggregate -> aggregate.getAggregateFunctions().size() == 1)
                                        .when(aggregate -> aggregate.getOutputExpressions().size() == 1)
                        ).when(project -> project.getProjects().size() == 2)
                                .when(project -> project.getProjects().get(0).getExprId()
                                        .equals(countIdName.getExprId()))
                                .when(project -> project.getProjects().get(1).getExprId()
                                        .equals(countNameId.getExprId()))
                                .when(project -> project.getProjects().get(0).getInputSlots()
                                        .equals(project.getProjects().get(1).getInputSlots()))
                );
    }

    @Test
    public void testKeepDifferentDistinctArguments() {
        Plan student = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of());
        Slot id = student.getOutput().get(0);
        Slot gender = student.getOutput().get(1);
        Slot name = student.getOutput().get(2);
        LogicalAggregate<Plan> root = new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of(
                        new Alias(new Count(true, id, name), "count_id_name"),
                        new Alias(new Count(true, id, gender), "count_id_gender")),
                student);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matches(
                        logicalJoin(
                                logicalAggregate()
                                        .when(aggregate -> aggregate.getAggregateFunctions().size() == 1),
                                logicalAggregate()
                                        .when(aggregate -> aggregate.getAggregateFunctions().size() == 1)
                        )
                );
    }

    @Test
    public void testRemoveDuplicateDistinctArguments() {
        Plan student = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of());
        Slot id = student.getOutput().get(0);
        Slot name = student.getOutput().get(2);
        Count count = new Count(true, id, id, name);

        Count countIf = (Count) AggregateUtils.countDistinctMultiExprToCountIf(count);
        Assertions.assertEquals(ImmutableSet.of(id, name), countIf.getInputSlots());

        LogicalAggregate<Plan> root = new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of(
                        new Alias(count, "count_id_id_name"),
                        new Alias(new Count(true, name, id), "count_name_id")),
                student);
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(
                        logicalProject(
                                logicalAggregate(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                ).when(aggregate -> aggregate.getAggregateFunctions().size() == 1)
                                        .when(aggregate -> aggregate.getAggregateFunctions().iterator().next()
                                                .arity() == 2)
                        )
                );
    }
}
