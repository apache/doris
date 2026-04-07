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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmSimpleScanDeltaStrategyTest extends IvmDeltaTestBase {

    private static final class TestableIvmSimpleScanDeltaStrategy extends IvmSimpleScanDeltaStrategy {
        private RewriteResult exposeRewritePlan(Plan plan) {
            return rewritePlan(plan);
        }

        private Plan exposeStripResultSink(Plan plan) {
            return stripResultSink(plan);
        }

        private Slot exposeFindSlotByName(List<Slot> slots, String name) {
            return findSlotByName(slots, name);
        }

        private Command exposeBuildInsertCommand(Plan plan, IvmDeltaRewriteContext ctx) {
            return buildInsertCommandWithDeleteSign(plan, ctx);
        }
    }

    @Test
    void testRewriteProducesInsertBundle(@Mocked MTMV mtmv) {
        new Expectations() {
            {
                mtmv.getQualifiedDbName();
                result = "test_db";
                mtmv.getName();
                result = "test_mv";
            }
        };

        LogicalOlapScan scan = buildScan();
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmSimpleScanDeltaStrategy()
                .rewrite(buildScanPlan(scan), ctx).get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);
        Assertions.assertTrue(sink.getColNames().contains(Column.DELETE_SIGN));
    }

    @Test
    void testRewritePlanInjectsDmlFactorAtScan() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(scan);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        Assertions.assertEquals(scan.getOutput().size() + 1, result.plan.getOutput().size());
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertEquals(1,
                result.plan.getOutput().stream().filter(slot -> Column.IVM_DML_FACTOR_COL.equals(slot.getName())).count());
    }

    @Test
    void testVisitLogicalProjectAppendsDmlFactor() {
        LogicalOlapScan scan = buildScan();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(project);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL,
                result.plan.getOutput().get(result.plan.getOutput().size() - 1).getName());
    }

    @Test
    void testVisitLogicalProjectPreservesExistingDmlFactor() {
        LogicalOlapScan scan = buildScan();
        ImmutableList<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .addAll(scan.getOutput())
                .add(new Alias(new TinyIntLiteral((byte) 1), Column.IVM_DML_FACTOR_COL))
                .build();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(outputs, scan);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(project);
        Assertions.assertEquals(1,
                result.plan.getOutput().stream().filter(slot -> Column.IVM_DML_FACTOR_COL.equals(slot.getName())).count());
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testVisitLogicalFilterPropagatesDmlFactor() {
        LogicalOlapScan scan = buildScan();
        Expression predicate = new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0));
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(
                new LogicalFilter<>(com.google.common.collect.ImmutableSet.of(predicate), scan));
        Assertions.assertInstanceOf(LogicalFilter.class, result.plan);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testVisitUnsupportedPlanThrows(@Mocked MTMV mtmv) {
        LogicalOlapScan left = buildScan();
        LogicalOlapScan right = buildScan();
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN, ImmutableList.of(), left, right, JoinReorderContext.EMPTY);
        LogicalResultSink<?> plan = new LogicalResultSink<>(ImmutableList.copyOf(left.getOutput()), join);

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmSimpleScanDeltaStrategy().rewrite(plan, ctx));
    }

    @Test
    void testStripResultSinkReturnsInnerPlan() {
        LogicalOlapScan scan = buildScan();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        LogicalResultSink<LogicalProject<LogicalOlapScan>> sink = new LogicalResultSink<>(
                ImmutableList.copyOf(scan.getOutput()), project);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        Assertions.assertSame(project, strategy.exposeStripResultSink(sink));
    }

    @Test
    void testFindSlotByNameReturnsMatchingSlot() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        Slot slot = strategy.exposeFindSlotByName(scan.getOutput(), scan.getOutput().get(0).getName());
        Assertions.assertEquals(scan.getOutput().get(0), slot);
    }

    @Test
    void testFindSlotByNameThrowsWhenMissing() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();

        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeFindSlotByName(scan.getOutput(), "missing_slot"));
    }

    @Test
    void testBuildInsertCommandWithDeleteSignAddsDeleteSignColumn(@Mocked MTMV mtmv) {
        new Expectations() {
            {
                mtmv.getQualifiedDbName();
                result = "test_db";
                mtmv.getName();
                result = "test_mv";
                mtmv.getInsertedColumnNames();
                result = ImmutableList.of("id");
            }
        };

        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy();
        Command command = strategy.exposeBuildInsertCommand(
                new LogicalProject<>(ImmutableList.of((NamedExpression) scan.getOutput().get(0)), scan),
                new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null));

        UnboundTableSink<?> sink = getSink((InsertIntoTableCommand) command);
        Assertions.assertEquals(ImmutableList.of("id", Column.DELETE_SIGN), sink.getColNames());
    }

    @Test
    void testRewriteBuildsDeleteSignIfExpression(@Mocked MTMV mtmv) {
        new Expectations() {
            {
                mtmv.getQualifiedDbName();
                result = "test_db";
                mtmv.getName();
                result = "test_mv";
            }
        };

        LogicalOlapScan scan = buildScan();
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmSimpleScanDeltaStrategy()
                .rewrite(buildScanPlan(scan), new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null))
                .get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);
        LogicalProject<?> sinkProject = (LogicalProject<?>) sink.child();
        NamedExpression lastExpr = sinkProject.getProjects().get(sinkProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());
    }
}
