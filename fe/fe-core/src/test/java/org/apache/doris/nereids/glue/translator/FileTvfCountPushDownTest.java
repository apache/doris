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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.tablefunction.CdcStreamTableValuedFunction;
import org.apache.doris.tablefunction.FileTableValuedFunction;
import org.apache.doris.tablefunction.GroupCommitTableValuedFunction;
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.HttpStreamTableValuedFunction;
import org.apache.doris.tablefunction.HttpTableValuedFunction;
import org.apache.doris.tablefunction.LocalTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FileTvfCountPushDownTest {
    private PhysicalTVFRelation tvf(boolean file) {
        return tvf(file ? FileTableValuedFunction.class : TableValuedFunctionIf.class);
    }

    private PhysicalTVFRelation tvf(Class<? extends TableValuedFunctionIf> functionClass) {
        TableValuedFunction function = Mockito.mock(TableValuedFunction.class);
        Mockito.when(function.getCatalogFunction()).thenReturn(Mockito.mock(functionClass));
        PhysicalTVFRelation scan = Mockito.mock(PhysicalTVFRelation.class);
        Mockito.when(scan.getFunction()).thenReturn(function);
        return scan;
    }

    @SuppressWarnings("unchecked")
    private PhysicalHashAggregate<Plan> aggregate(Plan child, AggregateFunction function) {
        PhysicalHashAggregate<Plan> aggregate = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(aggregate.child(0)).thenReturn(child);
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(ImmutableList.of());
        Mockito.when(aggregate.getAggregateFunctions()).thenReturn(ImmutableSet.of(function));
        Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.LOCAL_BUFFER);
        return aggregate;
    }

    @Test
    public void countStarAndCountOneThroughProject() {
        PhysicalTVFRelation scan = tvf(true);
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.doReturn(scan).when(project).child(0);
        SessionVariable session = new SessionVariable();
        Assertions.assertSame(scan, PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(scan, new Count()), session).get());
        Assertions.assertSame(scan, PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(project, new Count(new IntegerLiteral(1))), session).get());
    }

    @Test
    public void rejectRetainedAssertionsInProjects() {
        PhysicalTVFRelation scan = tvf(true);
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE);
        Alias assertion = new Alias(new AssertTrue(new GreaterThan(id, new IntegerLiteral(0)),
                new StringLiteral("positive id")), "checked");
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.doReturn(scan).when(project).child(0);
        Mockito.doReturn(ImmutableList.of(assertion)).when(project).getProjects();
        SessionVariable session = new SessionVariable();
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(project, new Count()), session).isPresent());

        // The expression can survive in an intermediate layer even when the final layer only forwards its slot.
        Mockito.doReturn(ImmutableList.of(assertion.toSlot())).when(project).getProjects();
        Mockito.doReturn(ImmutableList.of(ImmutableList.of(assertion), ImmutableList.of(assertion.toSlot())))
                .when(project).getMultiLayerProjects();
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(project, new Count()), session).isPresent());
    }

    @Test
    public void acceptAllSupportedFileTvfs() {
        SessionVariable session = new SessionVariable();
        for (Class<? extends TableValuedFunctionIf> functionClass : ImmutableList.of(
                FileTableValuedFunction.class,
                HdfsTableValuedFunction.class,
                HttpTableValuedFunction.class,
                LocalTableValuedFunction.class,
                S3TableValuedFunction.class)) {
            Assertions.assertTrue(PhysicalPlanTranslator.countPushDownFileTvf(
                    aggregate(tvf(functionClass), new Count()), session).isPresent());
        }
    }

    @Test
    public void rejectDistinctGroupedFilteredAndNonFilePlans() {
        SessionVariable session = new SessionVariable();
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(tvf(true), new Count(true, new IntegerLiteral(1))), session).isPresent());
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(tvf(false), new Count()), session).isPresent());
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(Mockito.mock(Plan.class), new Count()), session).isPresent());
        PhysicalHashAggregate<Plan> grouped = aggregate(tvf(true), new Count());
        Mockito.when(grouped.getGroupByExpressions()).thenReturn(ImmutableList.of(new IntegerLiteral(1)));
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(grouped, session).isPresent());
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(tvf(CdcStreamTableValuedFunction.class), new Count()), session).isPresent());
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(tvf(GroupCommitTableValuedFunction.class), new Count()), session).isPresent());
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(
                aggregate(tvf(HttpStreamTableValuedFunction.class), new Count()), session).isPresent());
    }

    @Test
    public void rejectMergeStageAndDisabledPushDown() {
        SessionVariable session = new SessionVariable();
        PhysicalHashAggregate<Plan> aggregate = aggregate(tvf(true), new Count());
        Mockito.when(aggregate.getAggregateParam()).thenReturn(
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT));
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(aggregate, session).isPresent());
        Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.LOCAL_BUFFER);
        session.enablePushDownNoGroupAgg = false;
        Assertions.assertFalse(PhysicalPlanTranslator.countPushDownFileTvf(aggregate, session).isPresent());
    }
}
