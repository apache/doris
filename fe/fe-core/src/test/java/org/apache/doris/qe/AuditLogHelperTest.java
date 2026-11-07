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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AuditLogHelperTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    private ConnectContext createMockContext(boolean isQuery, boolean isInternal) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStartTime();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.getState().setIsQuery(isQuery);
        ctx.getState().setInternal(isInternal);
        return ctx;
    }

    @Test
    public void testUpdateMetricsQueryOk() {
        ConnectContext ctx = createMockContext(true, false);
        ctx.getState().setOk(0, 0, "");

        long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

        Assertions.assertEquals(1, after - before);
    }

    @Test
    public void testUpdateMetricsQueryErr() {
        ConnectContext ctx = createMockContext(true, false);
        ctx.getState().setError("test error");

        long beforeAll = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long beforeErr = MetricRepo.COUNTER_QUERY_ERR.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long afterAll = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long afterErr = MetricRepo.COUNTER_QUERY_ERR.getValue();

        Assertions.assertEquals(1, afterAll - beforeAll);
        Assertions.assertEquals(1, afterErr - beforeErr);
    }

    @Test
    public void testUpdateMetricsNotQuery() {
        // INSERT scenario: isQuery = false, counters should not change
        ConnectContext ctx = createMockContext(false, false);
        ctx.getState().setOk(1, 0, "");

        long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

        Assertions.assertEquals(0, after - before);
    }

    @Test
    public void testUpdateMetricsDebugModeShortCircuit() {
        boolean original = Config.enable_bdbje_debug_mode;
        try {
            Config.enable_bdbje_debug_mode = true;

            ConnectContext ctx = createMockContext(true, false);
            ctx.getState().setOk(0, 0, "");

            long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
            AuditLogHelper.updateMetrics(ctx);
            long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

            Assertions.assertEquals(0, after - before);
        } finally {
            Config.enable_bdbje_debug_mode = original;
        }
    }

    @Test
    public void testGetCloudClusterForAuditPrefersEffectiveCluster() throws Exception {
        ConnectContext ctx = createMockContext(true, false);
        ctx.getSessionVariable().setCloudCluster("session_cluster");
        ctx.setEffectiveCloudCluster("hint_cluster");

        Assertions.assertEquals("hint_cluster", AuditLogHelper.getCloudClusterForAudit(ctx));
    }

    // A negative audit_plugin_max_sql_length made truncateByBytes throw, and logAuditLog swallowed the exception
    // together with the event: no statement other than INSERT ... VALUES was audited. It now counts as 0.
    @Test
    public void testNegativeMaxSqlLengthStillAudits() {
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        try {
            GlobalVariable.auditPluginMaxSqlLength = -1;
            Assertions.assertEquals(" ... /* truncated. audit_plugin_max_sql_length=0 */",
                    auditAndCapture("select 1").stmt);
        } finally {
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
        }
    }

    // With query_audit_always_on, a statement is truncated at the default audit_plugin_max_sql_length at the
    // earliest, whatever the variable says.
    @Test
    public void testAlwaysOnTruncatesAtTheDefaultLengthAtTheEarliest() {
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        boolean alwaysOn = Config.query_audit_always_on;
        try {
            Config.query_audit_always_on = true;
            for (int maxSqlLength : new int[] {-1, 0, 10}) {
                GlobalVariable.auditPluginMaxSqlLength = maxSqlLength;
                Assertions.assertEquals("select 1", auditAndCapture("select 1").stmt);
            }
            String head = "select '" + Strings.repeat("x", 2097152 - 8);
            String stmt = auditAndCapture(head + "tail'").stmt;
            Assertions.assertEquals(head + " ... /* truncated. audit_plugin_max_sql_length=2097152 */", stmt);
        } finally {
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            Config.query_audit_always_on = alwaysOn;
        }
    }

    // audit_plugin_max_insert_stmt_length cannot cut INSERT ... VALUES shorter either.
    @Test
    public void testAlwaysOnKeepsInsertValuesAtTheSqlLength() {
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int originalMaxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        boolean alwaysOn = Config.query_audit_always_on;
        ConnectContext ctx = new ConnectContext();
        ctx.changeDefaultCatalog("internal");
        ctx.setDatabase("db1");
        try {
            ctx.setThreadLocalInfo();
            String insert = "INSERT INTO t VALUES (1, 'a'), (2, 'b')";
            StatementBase parsed = new NereidsParser().parseSQL(insert).get(0);
            GlobalVariable.auditPluginMaxSqlLength = 0;
            GlobalVariable.auditPluginMaxInsertStmtLength = 10;

            Config.query_audit_always_on = false;
            Assertions.assertEquals(" ... /* total 2 rows, truncated. audit_plugin_max_insert_stmt_length=0 */",
                    AuditLogHelper.handleStmt(insert, parsed));
            Config.query_audit_always_on = true;
            Assertions.assertEquals(insert, AuditLogHelper.handleStmt(insert, parsed));
        } finally {
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
            Config.query_audit_always_on = alwaysOn;
            ConnectContext.remove();
        }
    }

    // Runs logAuditLog for a statement and returns the event it submitted.
    private AuditEvent auditAndCapture(String stmt) {
        ConnectContext ctx = createMockContext(false, false);
        ctx.getState().setOk();

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            AuditLogHelper.logAuditLog(ctx, stmt, null, null, true);

            ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr).submitFinishQueryToAudit(captor.capture());
            return captor.getValue();
        }
    }
}
