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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.proto.Data;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;

public class AuditLogLoadLabelTxnIdTest {

    private String originalDeployMode;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
        originalDeployMode = Config.deploy_mode;
        Config.deploy_mode = "not_cloud";
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        ConnectContext.remove();
    }

    private ConnectContext createConnectContext(Env mockedEnv) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStartTime();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setEnv(mockedEnv);
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private Env createMockedEnv() {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        WorkloadRuntimeStatusMgr workloadMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);

        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(workloadMgr);
        Mockito.when(env.isMaster()).thenReturn(true);
        return env;
    }

    private Data.PQueryStatistics buildStatistics(long scanRows, long scanBytes,
            long cpuMs, long peakMemoryBytes) {
        return Data.PQueryStatistics.newBuilder()
                .setScanRows(scanRows)
                .setScanBytes(scanBytes)
                .setCpuMs(cpuMs)
                .setMaxPeakMemoryBytes(peakMemoryBytes)
                .build();
    }

    private void callLogAuditLog(ConnectContext ctx, String origStmt,
            StatementBase parsedStmt, Data.PQueryStatistics statistics) {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(ctx.getEnv());
            AuditLogHelper.logAuditLog(ctx, origStmt, parsedStmt, statistics, false);
        }
    }

    private StatementBase createStmtWithStmtType(String stmtTypeName) {
        LogicalPlanAdapter adapter = Mockito.mock(LogicalPlanAdapter.class);
        LogicalPlan logicalPlan = Mockito.mock(LogicalPlan.class);
        Mockito.when(adapter.getLogicalPlan()).thenReturn(logicalPlan);
        Mockito.when(logicalPlan.stmtType()).thenReturn(StmtType.valueOf(stmtTypeName));
        Mockito.when(adapter.isExplain()).thenReturn(false);
        return adapter;
    }

    @Test
    public void testTxnStmtTypesCaseInsensitive() {
        Set<String> txnStmtTypes = AuditLogHelper.getTxnStmtTypes();
        Assert.assertTrue(txnStmtTypes.contains("insert"));
        Assert.assertTrue(txnStmtTypes.contains("Insert"));
        Assert.assertTrue(txnStmtTypes.contains("UPDATE"));
        Assert.assertTrue(txnStmtTypes.contains("update"));
        Assert.assertTrue(txnStmtTypes.contains("delete"));
        Assert.assertTrue(txnStmtTypes.contains("Delete"));
        Assert.assertTrue(txnStmtTypes.contains("merge_into"));
        Assert.assertTrue(txnStmtTypes.contains("Merge_Into"));
    }

    @Test
    public void testTxnStmtTypesExcludesNonTxnTypes() {
        Set<String> txnStmtTypes = AuditLogHelper.getTxnStmtTypes();
        Assert.assertFalse(txnStmtTypes.contains("SELECT"));
        Assert.assertFalse(txnStmtTypes.contains("ALTER"));
        Assert.assertFalse(txnStmtTypes.contains("CREATE"));
        Assert.assertFalse(txnStmtTypes.contains("DROP"));
    }

    @Test
    public void testInsertStmtWithInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        ctx.setOrUpdateInsertResult(10001L, "insert_label_abc", "test_db", "test_tbl",
                TransactionStatus.VISIBLE, 100, 0);

        StatementBase parsedStmt = createStmtWithStmtType("INSERT");
        Data.PQueryStatistics statistics = buildStatistics(1000L, 2048L, 50L, 4096L);
        callLogAuditLog(ctx, "INSERT INTO test_tbl VALUES (1)", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("insert_label_abc", event.loadLabel);
        Assert.assertEquals(10001L, event.txnId);
        Assert.assertEquals("INSERT", event.stmtType);
        Assert.assertEquals(1000L, event.scanRows);
        Assert.assertEquals(2048L, event.scanBytes);
        Assert.assertEquals(50L, event.cpuTimeMs);
        Assert.assertEquals(4096L, event.peakMemoryBytes);
    }

    @Test
    public void testUpdateStmtWithInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        ctx.setOrUpdateInsertResult(20002L, "update_label_xyz", "test_db", "test_tbl",
                TransactionStatus.VISIBLE, 50, 0);

        StatementBase parsedStmt = createStmtWithStmtType("UPDATE");
        Data.PQueryStatistics statistics = buildStatistics(500L, 1024L, 30L, 2048L);
        callLogAuditLog(ctx, "UPDATE test_tbl SET c1=1", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("update_label_xyz", event.loadLabel);
        Assert.assertEquals(20002L, event.txnId);
        Assert.assertEquals("UPDATE", event.stmtType);
    }

    @Test
    public void testDeleteStmtWithInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        ctx.setOrUpdateInsertResult(30003L, "delete_label_def", "test_db", "test_tbl",
                TransactionStatus.VISIBLE, 10, 0);

        StatementBase parsedStmt = createStmtWithStmtType("DELETE");
        Data.PQueryStatistics statistics = buildStatistics(200L, 512L, 10L, 1024L);
        callLogAuditLog(ctx, "DELETE FROM test_tbl WHERE c1=1", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("delete_label_def", event.loadLabel);
        Assert.assertEquals(30003L, event.txnId);
        Assert.assertEquals("DELETE", event.stmtType);
    }

    @Test
    public void testMergeIntoStmtWithInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        ctx.setOrUpdateInsertResult(40004L, "merge_label_ghi", "test_db", "test_tbl",
                TransactionStatus.VISIBLE, 30, 0);

        StatementBase parsedStmt = createStmtWithStmtType("MERGE_INTO");
        Data.PQueryStatistics statistics = buildStatistics(300L, 1536L, 40L, 3072L);
        callLogAuditLog(ctx, "MERGE INTO test_tbl USING src ON ...", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("merge_label_ghi", event.loadLabel);
        Assert.assertEquals(40004L, event.txnId);
        Assert.assertEquals("MERGE_INTO", event.stmtType);
    }

    @Test
    public void testTxnStmtWithoutInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        Assert.assertNull(ctx.getInsertResult());

        StatementBase parsedStmt = createStmtWithStmtType("INSERT");
        Data.PQueryStatistics statistics = buildStatistics(0L, 0L, 0L, 0L);
        callLogAuditLog(ctx, "INSERT INTO test_tbl VALUES (1)", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("", event.loadLabel);
        Assert.assertEquals(-1L, event.txnId);
    }

    @Test
    public void testNonTxnStmtWithInsertResultNotSet() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        ctx.setOrUpdateInsertResult(99999L, "should_not_appear", "test_db", "test_tbl",
                TransactionStatus.VISIBLE, 10, 0);

        StatementBase parsedStmt = createStmtWithStmtType("SELECT");
        Data.PQueryStatistics statistics = buildStatistics(5000L, 8192L, 100L, 8192L);
        callLogAuditLog(ctx, "SELECT * FROM test_tbl", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("", event.loadLabel);
        Assert.assertEquals(-1L, event.txnId);
        Assert.assertEquals(5000L, event.scanRows);
        Assert.assertEquals(8192L, event.scanBytes);
    }

    @Test
    public void testNonTxnStmtWithoutInsertResult() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        Assert.assertNull(ctx.getInsertResult());

        StatementBase parsedStmt = createStmtWithStmtType("SELECT");
        Data.PQueryStatistics statistics = buildStatistics(100L, 256L, 5L, 512L);
        callLogAuditLog(ctx, "SELECT 1", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("", event.loadLabel);
        Assert.assertEquals(-1L, event.txnId);
    }

    @Test
    public void testAuditEventBuilderResetClearsLoadLabelAndTxnId() {
        AuditEvent.AuditEventBuilder builder = new AuditEvent.AuditEventBuilder();
        builder.setLoadLabel("some_label");
        builder.setTxnId(12345L);
        Assert.assertEquals("some_label", builder.build().loadLabel);
        Assert.assertEquals(12345L, builder.build().txnId);

        builder.reset();
        AuditEvent event = builder.build();
        Assert.assertEquals("", event.loadLabel);
        Assert.assertEquals(-1L, event.txnId);
    }

    @Test
    public void testLogAuditLogResetsBuilderBeforeSetting() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        AuditEvent.AuditEventBuilder builder = ctx.getAuditEventBuilder();
        builder.setLoadLabel("stale_label_from_previous_query");
        builder.setTxnId(99999L);

        ctx.setOrUpdateInsertResult(100L, "fresh_label", "db1", "tbl1",
                TransactionStatus.VISIBLE, 10, 0);

        StatementBase parsedStmt = createStmtWithStmtType("INSERT");
        Data.PQueryStatistics statistics = buildStatistics(50L, 1024L, 20L, 2048L);
        callLogAuditLog(ctx, "INSERT INTO tbl1 VALUES (1)", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("fresh_label", event.loadLabel);
        Assert.assertEquals(100L, event.txnId);
    }

    @Test
    public void testLogAuditLogResetClearsStaleTxnInfoForNonTxnStmt() {
        Env env = createMockedEnv();
        ConnectContext ctx = createConnectContext(env);
        AuditEvent.AuditEventBuilder builder = ctx.getAuditEventBuilder();
        builder.setLoadLabel("stale_label");
        builder.setTxnId(88888L);

        StatementBase parsedStmt = createStmtWithStmtType("SELECT");
        Data.PQueryStatistics statistics = buildStatistics(10L, 128L, 1L, 256L);
        callLogAuditLog(ctx, "SELECT 1", parsedStmt, statistics);

        AuditEvent event = ctx.getAuditEventBuilder().build();
        Assert.assertEquals("", event.loadLabel);
        Assert.assertEquals(-1L, event.txnId);
    }
}
