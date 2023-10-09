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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.expression.CriticalColumnCollector.CollectorContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.AnalysisManager;

import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class CriticalColumnCollectorTest {

    private Plan rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
            ImmutableList.of(""));
    private Plan rScore = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score,
            ImmutableList.of(""));

    @Test
    void rewriteRoot(@Mocked Plan plan, @Mocked JobContext jobContext) {
        AtomicInteger i = new AtomicInteger();
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateColumnUsedInPredicate(Set<Slot> slotReferences) {
                i.incrementAndGet();
            }

            @Mock
            public void updateQueriedColumn(Collection<Slot> slotReferences) {
                i.incrementAndGet();
            }
        };
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        criticalColumnCollector.rewriteRoot(plan, jobContext);
        Assertions.assertEquals(2, i.get());
    }

    @Test
    void visitLogicalProject() {
        Expression expression = new GreaterThan(rStudent.getOutput().get(0), new IntegerLiteral(1));
        LogicalProject<Plan> project = new LogicalProject<>(Arrays.asList(new Alias(expression, "a")), rStudent);
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        CollectorContext context = new CollectorContext();
        criticalColumnCollector.visitLogicalProject(project, context);
        Assertions.assertEquals(1, context.queried.size());
    }

    @Test
    void visitLogicalJoin() {
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                Arrays.asList(new EqualTo(rStudent.getOutput().get(0), rScore.getOutput().get(0))),
                rStudent, rScore);
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        CollectorContext context = new CollectorContext();
        criticalColumnCollector.visitLogicalJoin(join, context);
        Assertions.assertEquals(2, context.usedInPredicate.size());
    }

    @Test
    void visitLogicalAggregate() {
        LogicalAggregate<Plan> aggregate
                = new LogicalAggregate<>(Arrays.asList(rStudent.getOutput().get(0)), true, rStudent);
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        CollectorContext context = new CollectorContext();
        criticalColumnCollector.visitLogicalAggregate(aggregate, context);
        Assertions.assertEquals(1, context.usedInPredicate.size());
    }

    @Test
    void visitLogicalHaving() {
        LogicalHaving<Plan> having = new LogicalHaving<>(new HashSet<Expression>() {
            {
                add(new GreaterThan(rStudent.getOutput().get(0), new IntegerLiteral(1)));
            }
        }, rStudent);
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        CollectorContext context = new CollectorContext();
        criticalColumnCollector.visitLogicalHaving(having, context);
        Assertions.assertEquals(1, context.usedInPredicate.size());
    }

    @Test
    void visitLogicalFilter() {
        LogicalFilter<Plan> filter = new LogicalFilter<>(new HashSet<Expression>() {
            {
                add(new GreaterThan(rStudent.getOutput().get(0), new IntegerLiteral(1)));
            }
        }, rStudent);
        CriticalColumnCollector criticalColumnCollector = new CriticalColumnCollector();
        CollectorContext context = new CollectorContext();
        criticalColumnCollector.visitLogicalFilter(filter, context);
        Assertions.assertEquals(1, context.usedInPredicate.size());
    }
}
