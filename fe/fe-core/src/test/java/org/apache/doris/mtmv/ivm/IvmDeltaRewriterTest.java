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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmDeltaRewriterTest {

    private LogicalOlapScan buildScan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "t1", 0);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("test_db"));
    }

    @Test
    void testScanOnlyProducesInsertBundle(@Mocked MTMV mtmv) {
        LogicalOlapScan scan = buildScan();
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(exprs, scan);
        LogicalResultSink<LogicalProject<LogicalOlapScan>> plan = new LogicalResultSink<>(exprs, project);

        new Expectations() {
            {
                mtmv.getQualifiedDbName();
                result = "test_db";
                mtmv.getName();
                result = "test_mv";
            }
        };

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext());
        List<DeltaCommandBundle> bundles = new IvmDeltaRewriter().rewrite(plan, ctx);

        Assertions.assertEquals(1, bundles.size());
        Assertions.assertEquals("t1", bundles.get(0).getBaseTableInfo().getTableName());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, bundles.get(0).getCommand());
    }

    @Test
    void testProjectScanProducesInsertBundle(@Mocked MTMV mtmv) {
        LogicalOlapScan scan = buildScan();
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalOlapScan> innerProject = new LogicalProject<>(exprs, scan);
        LogicalProject<LogicalProject<LogicalOlapScan>> outerProject = new LogicalProject<>(exprs, innerProject);
        LogicalResultSink<?> plan = new LogicalResultSink<>(exprs, outerProject);

        new Expectations() {
            {
                mtmv.getQualifiedDbName();
                result = "test_db";
                mtmv.getName();
                result = "test_mv";
            }
        };

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext());
        List<DeltaCommandBundle> bundles = new IvmDeltaRewriter().rewrite(plan, ctx);

        Assertions.assertEquals(1, bundles.size());
        Assertions.assertEquals("t1", bundles.get(0).getBaseTableInfo().getTableName());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, bundles.get(0).getCommand());
    }

    @Test
    void testUnsupportedNodeThrows(@Mocked MTMV mtmv) {
        LogicalOlapScan scan = buildScan();
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        // Nest ResultSink inside ResultSink — inner one is unsupported after stripping outer
        LogicalResultSink<?> inner = new LogicalResultSink<>(exprs, scan);
        LogicalResultSink<?> outer = new LogicalResultSink<>(exprs, inner);

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext());
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new IvmDeltaRewriter().rewrite(outer, ctx));
        Assertions.assertTrue(ex.getMessage().contains("does not yet support"));
    }

    @Test
    void testContextRejectsNulls(@Mocked MTMV mtmv) {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmDeltaRewriteContext(null, new ConnectContext()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmDeltaRewriteContext(mtmv, null));
    }
}
