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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class BindSlotReferenceTest {

    @BeforeEach
    public void beforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    public void testCannotFindSlot() {
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(new UnboundSlot("foo")),
                new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(project));
        Assertions.assertEquals("Unknown column 'foo' in 'table list' in PROJECT clause", exception.getMessage());
    }

    @Test
    public void testAmbiguousSlot() {
        String qualifiedName = "internal.db.student";
        List<String> qualifier = ImmutableList.copyOf(qualifiedName.split("\\."));
        LogicalOlapScan scan1 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalOlapScan scan2 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalSubQueryAlias<LogicalOlapScan> aliasedScan2 = new LogicalSubQueryAlias<>("scan2_alias", scan2);

        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(
                JoinType.CROSS_JOIN, scan1, aliasedScan2, null);

        LogicalProject<LogicalPlan> project = new LogicalProject<>(
                ImmutableList.of(new UnboundSlot("id")), join);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(project));
        Assertions.assertTrue(exception.getMessage().contains("id is ambiguous: "));
        Assertions.assertTrue(exception.getMessage().contains("id#4"));
        Assertions.assertTrue(exception.getMessage().contains("id#0"));
    }

    /*
    select t1.id from student t1 join student t2 on t1.id=t2.id group by id;
    group_by_key bind on t1.id, not t2.id
     */
    @Test
    public void testGroupByOnJoin() {
        String qualifiedName = "internal.db.student";
        List<String> qualifier = ImmutableList.copyOf(qualifiedName.split("\\."));
        LogicalOlapScan scan1 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalSubQueryAlias<LogicalOlapScan> sub1 = new LogicalSubQueryAlias<>("t1", scan1);
        LogicalOlapScan scan2 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalSubQueryAlias<LogicalOlapScan> sub2 = new LogicalSubQueryAlias<>("t2", scan2);
        LogicalJoin<LogicalSubQueryAlias<LogicalOlapScan>, LogicalSubQueryAlias<LogicalOlapScan>> join =
                new LogicalJoin<>(JoinType.CROSS_JOIN, sub1, sub2, null);
        LogicalAggregate<?> aggregate = new LogicalAggregate<>(
                Lists.newArrayList(new UnboundSlot("id")), //group by
                Lists.newArrayList(new UnboundSlot("t1", "id")), //output
                join
        );
        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(aggregate);
        LogicalAggregate<?> plan = (LogicalAggregate<?>) ((LogicalProject<?>) checker.getCascadesContext()
                .getMemo().copyOut()).child();
        SlotReference groupByKey = (SlotReference) plan.getGroupByExpressions().get(0);
        SlotReference t1id = (SlotReference) ((LogicalJoin<?, ?>) plan.child().child(0)).left().getOutput().get(0);
        SlotReference t2id = (SlotReference) ((LogicalJoin<?, ?>) plan.child().child(0)).right().getOutput().get(0);
        Assertions.assertEquals(groupByKey.getExprId(), t1id.getExprId());
        Assertions.assertNotEquals(t1id.getExprId(), t2id.getExprId());
    }

    /*
    select count(1) from student t1 join student t2 on t1.di=t2.id group by id;
    group by key is ambiguous
     */
    @Test
    public void testGroupByOnJoinAmbiguous() {
        String qualifiedName = "internal.db.student";
        List<String> qualifier = ImmutableList.copyOf(qualifiedName.split("\\."));
        LogicalOlapScan scan1 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalSubQueryAlias<LogicalOlapScan> sub1 = new LogicalSubQueryAlias<>("t1", scan1);
        LogicalOlapScan scan2 = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                qualifier);
        LogicalSubQueryAlias<LogicalOlapScan> sub2 = new LogicalSubQueryAlias<>("t2", scan2);
        LogicalJoin<LogicalSubQueryAlias<LogicalOlapScan>, LogicalSubQueryAlias<LogicalOlapScan>> join =
                new LogicalJoin<>(JoinType.CROSS_JOIN, sub1, sub2, null);
        LogicalAggregate<LogicalJoin<?, ?>> aggregate = new LogicalAggregate<>(
                Lists.newArrayList(new UnboundSlot("id")), //group by
                Lists.newArrayList(new Alias(new Count(new IntegerLiteral(1)), "count(1)")), //output
                join
        );
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(aggregate));
        Assertions.assertTrue(exception.getMessage().contains("id is ambiguous: "));
    }
}
