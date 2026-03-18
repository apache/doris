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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.ivm.IvmContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmNormalizeMtmvPlanTest {

    // DUP_KEYS table — row-id = UuidNumeric(), non-deterministic
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testGateDisabledKeepsPlanUnchanged() {
        Plan result = new IvmNormalizeMtmvPlan().rewriteRoot(scan, newJobContext(false));
        Assertions.assertSame(scan, result);
    }

    @Test
    void testScanInjectsRowIdAtIndexZero() {
        JobContext jobContext = newJobContext(true);
        Plan result = new IvmNormalizeMtmvPlan().rewriteRoot(scan, jobContext);

        // scan is wrapped in a project
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertSame(scan, project.child());

        // first output is the row-id alias
        List<? extends Slot> outputs = project.getOutput();
        Assertions.assertEquals(scan.getOutput().size() + 1, outputs.size());
        Slot rowIdSlot = outputs.get(0);
        Assertions.assertEquals(IvmNormalizeMtmvPlan.IVM_ROW_ID_COL, rowIdSlot.getName());

        // row-id expression is UuidNumeric for DUP_KEYS
        Alias rowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertInstanceOf(UuidNumeric.class, rowIdAlias.child());

        // IvmContext records non-deterministic for DUP_KEYS
        IvmContext ivmContext = jobContext.getCascadesContext().getIvmContext().get();
        Assertions.assertEquals(1, ivmContext.getRowIdDeterminism().size());
        Assertions.assertFalse(ivmContext.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testProjectOnScanPropagatesRowId() {
        Slot slot = scan.getOutput().get(0);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(slot), scan);

        Plan result = new IvmNormalizeMtmvPlan().rewriteRoot(project, newJobContext(true));

        // outer project has row-id at index 0
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> outer = (LogicalProject<?>) result;
        Assertions.assertEquals(IvmNormalizeMtmvPlan.IVM_ROW_ID_COL, outer.getOutput().get(0).getName());
        // child is the scan-wrapping project
        Assertions.assertInstanceOf(LogicalProject.class, outer.child());
        Assertions.assertSame(scan, ((LogicalProject<?>) outer.child()).child());
    }

    @Test
    void testMowTableRowIdIsDeterministic() {
        OlapTable mowTable = PlanConstructor.newOlapTable(10, "mow", 0, KeysType.UNIQUE_KEYS);
        TableProperty tableProperty = new TableProperty(new java.util.HashMap<>());
        tableProperty.setEnableUniqueKeyMergeOnWrite(true);
        mowTable.setTableProperty(tableProperty);
        LogicalOlapScan mowScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), mowTable, ImmutableList.of("db"));

        JobContext jobContext = newJobContextForScan(mowScan, true);
        Plan result = new IvmNormalizeMtmvPlan().rewriteRoot(mowScan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(IvmNormalizeMtmvPlan.IVM_ROW_ID_COL, result.getOutput().get(0).getName());
        IvmContext ivmContext = jobContext.getCascadesContext().getIvmContext().get();
        Assertions.assertTrue(ivmContext.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testMorTableThrows() {
        // UNIQUE_KEYS without MOW (MOR) is not supported
        OlapTable morTable = PlanConstructor.newOlapTable(11, "mor", 0, KeysType.UNIQUE_KEYS);
        LogicalOlapScan morScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), morTable, ImmutableList.of("db"));

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmvPlan().rewriteRoot(morScan, newJobContextForScan(morScan, true)));
    }

    @Test
    void testAggKeyTableThrows() {
        OlapTable aggTable = PlanConstructor.newOlapTable(12, "agg", 0, KeysType.AGG_KEYS);
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("db"));

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmvPlan().rewriteRoot(aggScan, newJobContextForScan(aggScan, true)));
    }

    @Test
    void testUnsupportedPlanNodeThrows() {
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmvPlan().rewriteRoot(sort, newJobContext(true)));
    }

    @Test
    void testUnsupportedNodeAsChildThrows() {
        Slot slot = scan.getOutput().get(0);
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(slot), sort);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmvPlan().rewriteRoot(project, newJobContext(true)));
    }

    @Test
    void testNormalizedPlanStoredInIvmContext() {
        JobContext jobContext = newJobContext(true);
        Plan result = new IvmNormalizeMtmvPlan().rewriteRoot(scan, jobContext);

        IvmContext ivmContext = jobContext.getCascadesContext().getIvmContext().get();
        Assertions.assertNotNull(ivmContext.getNormalizedPlan());
        Assertions.assertSame(result, ivmContext.getNormalizedPlan());
    }

    private JobContext newJobContext(boolean enableIvmNormalRewrite) {
        return newJobContextForScan(scan, enableIvmNormalRewrite);
    }

    private JobContext newJobContextForScan(LogicalOlapScan rootScan, boolean enableIvmNormalRewrite) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIvmNormalRewrite(enableIvmNormalRewrite);
        connectContext.setSessionVariable(sessionVariable);
        StatementContext statementContext = new StatementContext(connectContext, null);
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, rootScan, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }
}
