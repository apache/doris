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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcTvfSourceOffsetProvider;
import org.apache.doris.job.offset.s3.S3Offset;
import org.apache.doris.job.offset.s3.S3SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingInsertTaskAuditTest {
    private static final String ORIGIN_URI = "s3://bucket/input/*.csv";
    private static final String RESOLVED_URI = "s3://bucket/input/{1.csv,2.csv}";
    private static final String S3_SQL = "insert into target_table select * from s3("
            + "\"uri\" = \"" + ORIGIN_URI + "\", "
            + "\"s3.secret_key\" = \"private-value\")";

    @Test
    public void testS3RunSubmitsAuditEvent() throws Exception {
        AuditEvent auditEvent = runS3Task(null);

        Assert.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assert.assertEquals(StmtType.INSERT.name(), auditEvent.stmtType);
        Assert.assertFalse(auditEvent.stmt.contains(ORIGIN_URI));
        Assert.assertTrue(auditEvent.stmt.contains(RESOLVED_URI));
        Assert.assertFalse(auditEvent.stmt.contains("private-value"));
        Assert.assertEquals("OK", auditEvent.state);
        Assert.assertTrue(auditEvent.isInternal);
    }

    @Test
    public void testFailedS3RunSubmitsErrorAuditEvent() throws Exception {
        AuditEvent auditEvent = runS3Task(new RuntimeException("insert failed"));

        Assert.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assert.assertEquals(StmtType.INSERT.name(), auditEvent.stmtType);
        Assert.assertEquals("ERR", auditEvent.state);
        Assert.assertTrue(auditEvent.errorMessage.contains("insert failed"));
        Assert.assertTrue(auditEvent.isInternal);
    }

    @Test
    public void testCdcRunDoesNotSubmitAuditEvent() throws Exception {
        String sql = "insert into target_table select * from cdc_stream("
                + "\"type\" = \"mysql\", \"jdbc_url\" = \"jdbc:mysql://127.0.0.1:3306\", "
                + "\"table\" = \"source_table\", \"offset\" = \"latest\")";
        runTask(null, sql, Mockito.mock(JdbcTvfSourceOffsetProvider.class), Mockito.mock(Offset.class), false);
    }

    private AuditEvent runS3Task(RuntimeException commandFailure) throws Exception {
        S3Offset offset = new S3Offset();
        offset.setFileLists(RESOLVED_URI);
        return runTask(commandFailure, S3_SQL, new S3SourceOffsetProvider(), offset, true);
    }

    private AuditEvent runTask(RuntimeException commandFailure, String sql,
            SourceOffsetProvider offsetProvider, Offset offset, boolean expectAudit) throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            ConnectContext ctx = InsertTask.makeConnectContext(UserIdentity.ROOT, "test_db");
            ctx.getState().setOk();
            InsertIntoTableCommand command = Mockito.mock(InsertIntoTableCommand.class);
            if (expectAudit) {
                Map<String, String> rewrittenTvfProps = new HashMap<>();
                rewrittenTvfProps.put("uri", RESOLVED_URI);
                rewrittenTvfProps.put("s3.secret_key", "private-value");
                UnboundTVFRelation tvf = Mockito.mock(UnboundTVFRelation.class);
                Mockito.when(tvf.getProperties()).thenReturn(new Properties(rewrittenTvfProps));
                Mockito.when(command.getAllTVFRelation()).thenReturn(Collections.singletonList(tvf));
            }
            AtomicLong commandStartTime = new AtomicLong();
            Mockito.doAnswer(invocation -> {
                commandStartTime.set(ctx.getStartTime());
                if (commandFailure != null) {
                    throw commandFailure;
                }
                return null;
            }).when(command).run(Mockito.eq(ctx), Mockito.any(StmtExecutor.class));

            LogicalPlanAdapter parsedStmt = new LogicalPlanAdapter(
                    new NereidsParser().parseSingle(sql), new StatementContext());
            StmtExecutor executor = Mockito.mock(StmtExecutor.class);
            Mockito.when(executor.getParsedStmt()).thenReturn(parsedStmt);
            Mockito.when(executor.getExternalDmlAuditBackendIds()).thenReturn(Collections.emptySet());
            Mockito.when(executor.getSummaryProfile()).thenReturn(Mockito.mock(SummaryProfile.class));
            ctx.setExecutor(executor);

            StreamingInsertTask task = new StreamingInsertTask(
                    1L, 2L, sql, offsetProvider, "test_db", null,
                    Collections.emptyMap(), UserIdentity.ROOT, null);
            Deencapsulation.setField(task, "ctx", ctx);
            Deencapsulation.setField(task, "taskCommand", command);
            Deencapsulation.setField(task, "stmtExecutor", executor);
            Deencapsulation.setField(task, "runningOffset", offset);
            if (expectAudit) {
                TreeMap<Pair<Integer, Integer>, String> replacements =
                        new TreeMap<>(new Pair.PairComparator<>());
                new NereidsParser().parseForEncryption(sql, replacements);
                Deencapsulation.setField(task, "auditSql",
                        Deencapsulation.invoke(task, "getAuditSql", replacements));
            }

            if (commandFailure == null) {
                task.run();
            } else {
                Assert.assertThrows(JobException.class, task::run);
            }

            if (!expectAudit) {
                Mockito.verifyNoInteractions(statusMgr);
                return null;
            }
            ArgumentCaptor<AuditEvent> auditEventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr).submitFinishQueryToAudit(auditEventCaptor.capture());
            AuditEvent auditEvent = auditEventCaptor.getValue();
            Assert.assertTrue(commandStartTime.get() > 0);
            Assert.assertEquals(commandStartTime.get(), auditEvent.timestamp);
            return auditEvent;
        } finally {
            ConnectContext.remove();
        }
    }
}
