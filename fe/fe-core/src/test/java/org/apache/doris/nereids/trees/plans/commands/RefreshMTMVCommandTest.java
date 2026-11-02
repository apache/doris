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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for REFRESH MATERIALIZED VIEW command parsing with INCREMENTAL/PARTITIONS modes.
 */
public class RefreshMTMVCommandTest {

    private static final String DECLARED_CG = "cg_declared";

    private final NereidsParser parser = new NereidsParser();

    private String originalDeployMode;
    private String originalCloudUniqueId;
    private SystemInfoService originalSystemInfo;
    private AccessControllerManager originalAccessManager;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
        originalAccessManager = Env.getCurrentEnv().getAccessManager();
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", originalAccessManager);
        // the dry run and EXPLAIN contexts install themselves as the thread-local context
        ConnectContext.remove();
    }

    private RefreshMTMVInfo extractRefreshInfo(String sql) throws Exception {
        StatementBase statementBase = parser.parseSQL(sql).get(0);
        Assertions.assertTrue(statementBase instanceof LogicalPlanAdapter,
                "Parsed statement should be LogicalPlanAdapter");
        LogicalPlan plan = ((LogicalPlanAdapter) statementBase).getLogicalPlan();
        Assertions.assertTrue(plan instanceof RefreshMTMVCommand,
                "Parsed plan should be RefreshMTMVCommand");
        Field field = RefreshMTMVCommand.class.getDeclaredField("refreshMTMVInfo");
        field.setAccessible(true);
        return (RefreshMTMVInfo) field.get(plan);
    }

    private void assertParseFails(String sql) {
        try {
            parser.parseSQL(sql);
            Assertions.fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    // TC-8-1: REFRESH MV ... INCREMENTAL can be parsed
    @Test
    public void testParseRefreshIncremental() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-2: REFRESH MV ... AUTO still parseable
    @Test
    public void testParseRefreshAuto() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertEquals(RefreshMode.AUTO, info.getRefreshMode());
    }

    // TC-8-3: REFRESH MV ... COMPLETE still parseable
    @Test
    public void testParseRefreshComplete() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.isComplete());
    }

    // TC-8-4: REFRESH MV ... PARTITIONS parsed as new refresh mode
    @Test
    public void testParseRefreshPartitionsMode() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-extra: REFRESH MV without mode should be rejected (syntax error)
    @Test
    public void testParseRefreshWithoutModeFails() throws Exception {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1");
    }

    // TC-8-extra: REFRESH MV with old-style partitionSpec still works
    @Test
    public void testParseRefreshWithPartitionSpec() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1, p2)");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertEquals(2, info.getPartitions().size());
        Assertions.assertFalse(info.allowFallback());
    }

    @Test
    public void testParseRefreshWithFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL FALLBACK");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshPartitionsFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS FALLBACK");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshCompleteFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE FALLBACK");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParsePartitionSpecFallbackFails() {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1) FALLBACK");
    }

    // TC-8-extra: isComplete() backward compat — only true for COMPLETE mode
    @Test
    public void testIsCompleteBackwardCompat() throws Exception {
        RefreshMTMVInfo completeInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertTrue(completeInfo.isComplete());

        RefreshMTMVInfo incrementalInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertFalse(incrementalInfo.isComplete());

        RefreshMTMVInfo autoInfo = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertFalse(autoInfo.isComplete());
    }

    @Test
    public void testDryRunStatementContextCarriesExcludedTriggerTables() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, true);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Set<TableNameInfo> excluded = Collections.singleton(new TableNameInfo("internal", "db", "dup_t"));
        Mockito.when(mtmv.getQuerySql()).thenReturn("SELECT 1 AS k1");
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excluded);

        StatementContext stmtCtx = command.createDryRunStatementContext(mtmv, new ConnectContext());

        Assertions.assertEquals(excluded, stmtCtx.getExcludedTriggerTables());
    }

    @Test
    public void testIncrementalDryRunParsesSetVarWithItsStatementContext() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, true);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQuerySql()).thenReturn("SELECT /*+ SET_VAR(query_timeout=10) */ 1 AS k1");
        ConnectContext internalCtx = new ConnectContext();
        internalCtx.setThreadLocalInfo();
        try {
            StatementContext stmtCtx = command.createDryRunStatementContext(mtmv, internalCtx);
            new IvmIncrRefreshManager().buildQueryPlan(mtmv);
            Assertions.assertSame(stmtCtx, internalCtx.getStatementContext());
            Assertions.assertEquals(10, internalCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testIncrementalExplainCarriesExcludedTriggerTables() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        IvmIncrRefreshManager manager = new IvmIncrRefreshManager() {
            @Override
            public InsertIntoTableCommand buildInsertCommand(MTMV mtmv) {
                return Mockito.mock(InsertIntoTableCommand.class);
            }
        };
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, manager);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Set<TableNameInfo> excluded = Collections.singleton(new TableNameInfo("internal", "db", "dup_t"));
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excluded);
        StatementContext stmtCtx = new StatementContext(new ConnectContext(), null);

        command.createRefreshCommand(mtmv, stmtCtx);

        Assertions.assertEquals(excluded, stmtCtx.getExcludedTriggerTables());
    }

    // ---- the compute group the dry run and EXPLAIN REFRESH contexts run in ----

    // Cloud mode with these compute groups, on all of which admin, the identity of both contexts,
    // holds USAGE.
    private void enterCloudMode(List<String> existingComputeGroups) {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        CloudSystemInfoService systemInfo = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfo.getCloudClusterNames()).thenReturn(existingComputeGroups);
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfo);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkCloudPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.any(PrivPredicate.class), Mockito.any(ResourceTypeEnum.class))).thenReturn(true);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", accessManager);
    }

    private MTMV newMtmvDeclaring(String declaredComputeGroup) {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getComputeGroup()).thenReturn(Optional.ofNullable(declaredComputeGroup));
        return mtmv;
    }

    private TestRefreshMTMVCommand newIncrementalCommand(boolean dryRun) throws Exception {
        return new TestRefreshMTMVCommand(extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL"), dryRun);
    }

    // The dry run plans and executes its delta query, and NereidsCoordinator looks the MV's workload
    // group up, in the compute group of this context. It must be the group the MV declares, where
    // every refresh of the MV runs, not the one admin resolves to.
    @Test
    public void testDryRunContextBindsDeclaredComputeGroup() throws Exception {
        enterCloudMode(Lists.newArrayList("cg_of_admin", DECLARED_CG));

        ConnectContext internalCtx = newIncrementalCommand(true).createDryRunConnectContext(
                newMtmvDeclaring(DECLARED_CG));

        Assertions.assertEquals(DECLARED_CG, internalCtx.getSessionVariable().getCloudCluster());
    }

    // The declared group can be dropped while the MV exists. Like a real refresh, the dry run then
    // fails with that reason instead of running the delta in another group.
    @Test
    public void testDryRunRejectsDroppedDeclaredComputeGroup() throws Exception {
        enterCloudMode(Lists.newArrayList("cg_of_admin"));
        TestRefreshMTMVCommand command = newIncrementalCommand(true);

        UserException e = Assertions.assertThrows(UserException.class,
                () -> command.createDryRunConnectContext(newMtmvDeclaring(DECLARED_CG)));

        Assertions.assertTrue(e.getMessage().contains("Compute group '" + DECLARED_CG + "' not found."),
                e.getMessage());
    }

    // EXPLAIN REFRESH only plans, but planning assigns backends, so it plans in the declared group too.
    @Test
    public void testExplainContextBindsDeclaredComputeGroup() throws Exception {
        enterCloudMode(Lists.newArrayList("cg_of_admin", DECLARED_CG));

        ConnectContext explainCtx = newIncrementalCommand(false).createExplainConnectContext(
                newMtmvDeclaring(DECLARED_CG));

        Assertions.assertEquals(DECLARED_CG, explainCtx.getSessionVariable().getCloudCluster());
    }

    // An MV that declares nothing keeps the existing resolution: neither context names a group, and
    // the dry run has nothing to re-check, even with no compute group left at all.
    @Test
    public void testContextsWithoutDeclarationKeepImplicitComputeGroup() throws Exception {
        enterCloudMode(Lists.newArrayList());
        MTMV mtmv = newMtmvDeclaring(null);

        Assertions.assertTrue(Strings.isNullOrEmpty(newIncrementalCommand(true)
                .createDryRunConnectContext(mtmv).getSessionVariable().getCloudCluster()));
        Assertions.assertTrue(Strings.isNullOrEmpty(newIncrementalCommand(false)
                .createExplainConnectContext(mtmv).getSessionVariable().getCloudCluster()));
    }

    private static class TestRefreshMTMVCommand extends RefreshMTMVCommand {
        private final IvmIncrRefreshManager manager;

        TestRefreshMTMVCommand(RefreshMTMVInfo info, boolean dryRun) {
            super(info, false, dryRun, Optional.empty());
            this.manager = new IvmIncrRefreshManager();
        }

        TestRefreshMTMVCommand(RefreshMTMVInfo info, IvmIncrRefreshManager manager) {
            super(info, false, false, Optional.empty());
            this.manager = manager;
        }

        @Override
        IvmIncrRefreshManager createIvmIncrRefreshManager() {
            return manager;
        }

        @Override
        public LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) throws Exception {
            return super.createRefreshCommand(mtmv, statementContext);
        }
    }
}
