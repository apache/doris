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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmDeltaRewriterTest extends IvmDeltaTestBase {

    @Test
    void testScanOnlyProducesInsertBundle(@Mocked MTMV mtmv) {
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
        List<IvmDeltaCommandBundle> bundles = new IvmDeltaRewriter().rewrite(buildScanPlan(scan), ctx);

        Assertions.assertEquals(1, bundles.size());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, bundles.get(0).getCommand());
    }

    @Test
    void testProjectScanProducesInsertBundle(@Mocked MTMV mtmv) {
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
        List<IvmDeltaCommandBundle> bundles = new IvmDeltaRewriter().rewrite(buildProjectScanPlan(scan), ctx);

        Assertions.assertEquals(1, bundles.size());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, bundles.get(0).getCommand());
    }

    @Test
    void testGroupedAggProducesDeleteSignSinkAndJoinPlan() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, bundle.connectContext, bundle.normalizeResult);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(bundle.normalizedPlan, ctx).get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);

        Assertions.assertEquals(mtmv.getInsertedColumnNames().size() + 1, sink.getColNames().size());
        Assertions.assertEquals(Column.DELETE_SIGN, sink.getColNames().get(sink.getColNames().size() - 1));
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertInstanceOf(LogicalFilter.class, finalProject.child());
        Assertions.assertInstanceOf(LogicalJoin.class, ((LogicalFilter<?>) finalProject.child()).child());
    }

    @Test
    void testContextRejectsNulls(@Mocked MTMV mtmv) {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmDeltaRewriteContext(null, new ConnectContext(), null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmDeltaRewriteContext(mtmv, null, null));
    }

    @Test
    void testRouteWithAggMetaUsesAggStrategy() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, bundle.connectContext, bundle.normalizeResult);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(bundle.normalizedPlan, ctx).get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertTrue(finalProject.child() instanceof LogicalFilter
                || finalProject.child() instanceof LogicalJoin);
    }

    @Test
    void testRouteWithoutAggMetaUsesScanStrategy(@Mocked MTMV mtmv) {
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
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(buildScanPlan(scan), ctx).get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);
        Plan child = sink.child();
        Assertions.assertInstanceOf(LogicalProject.class, child);
        Assertions.assertFalse(child instanceof LogicalJoin);
    }
}
