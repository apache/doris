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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcTvfSourceOffsetProvider;
import org.apache.doris.job.offset.s3.S3EventOffset;
import org.apache.doris.job.offset.s3.S3Offset;
import org.apache.doris.job.offset.s3.S3SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingInsertTaskAuditTest {
    private ConnectContext previousContext;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext());
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    private static final String ORIGIN_URI = "s3://bucket/input/*.csv";
    private static final String RESOLVED_URI = "s3://bucket/input/{1.csv,2.csv}";
    private static final String S3_SQL = "insert into target_table select * from s3("
            + "\"uri\" = \"" + ORIGIN_URI + "\", "
            + "\"s3.secret_key\" = \"private-value\", "
            + "\"enclose\" = \"\\\"\")";

    @Test
    public void testCdcTaskDisablesInPlaceRetry() {
        StreamingInsertTask cdcTask = new StreamingInsertTask(
                1L, 2L, "", new JdbcTvfSourceOffsetProvider(), "test_db", null,
                Collections.emptyMap(), UserIdentity.ROOT, null);
        StreamingInsertTask s3Task = new StreamingInsertTask(
                1L, 3L, "", new S3SourceOffsetProvider(), "test_db", null,
                Collections.emptyMap(), UserIdentity.ROOT, null);

        Assertions.assertTrue(cdcTask.isNoRetry());
        Assertions.assertFalse(s3Task.isNoRetry());
    }

    @Test
    public void testS3RunSubmitsAuditEvent() throws Exception {
        AuditEvent auditEvent = runS3Task(null);

        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertEquals(StmtType.INSERT.name(), auditEvent.stmtType);
        Assertions.assertFalse(auditEvent.stmt.contains(ORIGIN_URI));
        Assertions.assertTrue(auditEvent.stmt.contains(RESOLVED_URI));
        Assertions.assertFalse(auditEvent.stmt.contains("WITH LABEL"));
        Assertions.assertFalse(auditEvent.stmt.contains("private-value"));
        Assertions.assertEquals("OK", auditEvent.state);
        Assertions.assertTrue(auditEvent.isInternal);
    }

    @Test
    public void testFailedS3RunSubmitsErrorAuditEvent() throws Exception {
        AuditEvent auditEvent = runS3Task(new RuntimeException("insert failed"));

        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertEquals(StmtType.INSERT.name(), auditEvent.stmtType);
        Assertions.assertEquals("ERR", auditEvent.state);
        Assertions.assertTrue(auditEvent.errorMessage.contains("insert failed"));
        Assertions.assertFalse(auditEvent.stmt.contains("WITH LABEL"));
        Assertions.assertTrue(auditEvent.isInternal);
    }

    @Test
    public void testAuditPreservesOriginalInsertClauses() throws Exception {
        for (String target : new String[] {
                "target_table(c1)",
                "`db`.`target table` PARTITION (p0) (c1)",
                "target_table WITH LABEL old_label (c1)",
                "target_table [shuffle]",
                "target_table /* target comment */ (c1)"}) {
            AuditEvent event = runTask(null, S3_SQL.replace("target_table", target),
                    new S3SourceOffsetProvider(), new S3Offset(), true);
            Assertions.assertTrue(event.stmt.contains(target));
            Assertions.assertFalse(event.stmt.contains("WITH LABEL `1_2`"));
            Assertions.assertTrue(event.stmt.contains(RESOLVED_URI));
            Assertions.assertFalse(event.stmt.contains("private-value"));
        }
    }

    @Test
    public void testCdcRunDoesNotSubmitAuditEvent() throws Exception {
        String sql = "insert into target_table select * from cdc_stream("
                + "\"type\" = \"mysql\", \"jdbc_url\" = \"jdbc:mysql://127.0.0.1:3306\", "
                + "\"table\" = \"source_table\", \"offset\" = \"latest\")";
        runTask(null, sql, Mockito.mock(JdbcTvfSourceOffsetProvider.class), Mockito.mock(Offset.class), false);
    }

    @Test
    public void testAuditParsingFailureDoesNotFailInsert() throws Exception {
        runWithAuditPreparationFailure(true);
    }

    @Test
    public void testAuditRangeFailureDoesNotFailInsert() throws Exception {
        runWithAuditPreparationFailure(false);
    }

    private void runWithAuditPreparationFailure(boolean parserFailure) throws Exception {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState state = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(state);
        StreamingJobProperties properties = Mockito.mock(StreamingJobProperties.class);
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("s3");
        S3Offset offset = new S3Offset();
        Mockito.when(provider.getNextOffset(Mockito.eq(properties), Mockito.anyMap())).thenReturn(offset);
        InsertIntoTableCommand baseCommand = Mockito.mock(InsertIntoTableCommand.class);
        InsertIntoTableCommand taskCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(taskCommand.getLogicalQuery()).thenReturn(auditRelation());
        offset.setFileStatuses(batchFiles());
        Mockito.when(provider.rewriteTvfParams(Mockito.eq(baseCommand), Mockito.eq(offset), Mockito.anyLong()))
                .thenReturn(taskCommand);
        Mockito.doAnswer(invocation -> {
            state.setOk();
            return null;
        }).when(taskCommand).run(Mockito.eq(ctx), Mockito.any(StmtExecutor.class));

        try (MockedStatic<InsertTask> insertTask = Mockito.mockStatic(InsertTask.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                        (parser, construction) -> {
                            Mockito.when(parser.parseSingle(S3_SQL)).thenReturn(baseCommand);
                            if (parserFailure) {
                                Mockito.when(parser.parseForEncryption(Mockito.eq(S3_SQL), Mockito.anyMap()))
                                        .thenThrow(new IllegalStateException("audit parsing failed"));
                            }
                            // Otherwise leave replacements empty to exercise the range assertion.
                        });
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            insertTask.when(() -> InsertTask.makeConnectContext(UserIdentity.ROOT, "test_db")).thenReturn(ctx);
            StreamingInsertTask task = new StreamingInsertTask(1L, 2L, S3_SQL, provider, "test_db", properties,
                    Map.of("URI", ORIGIN_URI), UserIdentity.ROOT, null);
            // A retry must not audit files from an earlier attempt after preparation fails.
            Deencapsulation.setField(task, "auditSql", "stale audit SQL from a previous attempt");
            task.before();
            Assertions.assertNull(task.getAuditSql());
            Assertions.assertEquals(2, parsers.constructed().size());
            Mockito.verify(parsers.constructed().get(0)).parseSingle(S3_SQL);
            Mockito.verify(parsers.constructed().get(1)).parseForEncryption(Mockito.eq(S3_SQL), Mockito.anyMap());
            task.run();
            Assertions.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
            Assertions.assertEquals(1, executors.constructed().size());
            Mockito.verify(baseCommand, Mockito.never()).initPlan(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
            Mockito.verify(taskCommand).run(ctx, executors.constructed().get(0));
            audit.verifyNoInteractions();
        }
    }

    @Test
    public void testRewriteS3UriCaseInsensitive() {
        Map<String, String> originProperties = new HashMap<>();
        originProperties.put("URI", ORIGIN_URI);
        UnboundTVFRelation originTvf = new UnboundTVFRelation(
                new RelationId(1), "s3", new Properties(originProperties));

        S3Offset offset = new S3Offset();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            InsertIntoTableCommand originCommand = new InsertIntoTableCommand(originTvf,
                    Optional.of("original_label"), Optional.empty(), Optional.empty());
            originCommand.setJobId(42L);
            offset.setFileStatuses(batchFiles());
            InsertIntoTableCommand rewritten = new S3SourceOffsetProvider()
                    .rewriteTvfParams(originCommand, offset, 1L);

            Map<String, String> rewrittenProperties =
                    ((LogicalTVFRelation) rewritten.getLogicalQuery()).getFunction().getTVFProperties().getMap();
            Assertions.assertEquals(Optional.of("original_label"), rewritten.getLabelName());
            Assertions.assertEquals(42L, rewritten.getJobId());
            Assertions.assertEquals(1, rewrittenProperties.size());
            Assertions.assertEquals(RESOLVED_URI, rewrittenProperties.get("URI"));
        }
    }

    @Test
    public void testExactFileRewritePreservesWithClause() {
        ConnectContext.get().setDatabase("test_db");
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.isMaster()).thenReturn(false);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            InsertIntoTableCommand command = (InsertIntoTableCommand) new NereidsParser().parseSingle(
                    "with input as (select 1 as c) insert into target_table select * from s3(\"uri\"=\""
                            + ORIGIN_URI + "\") cross join input");
            S3Offset offset = new S3Offset();
            offset.setFileStatuses(batchFiles());
            InsertIntoTableCommand rewritten = new S3SourceOffsetProvider().rewriteTvfParams(command, offset, 1L);
            Optional<?> originalCte = Deencapsulation.getField(command, "cte");
            Optional<?> rewrittenCte = Deencapsulation.getField(rewritten, "cte");
            Assertions.assertTrue(originalCte.isPresent());
            Assertions.assertEquals(originalCte, rewrittenCte);
            List<LogicalTVFRelation> relations = rewritten.getLogicalQuery()
                    .collectToList(LogicalTVFRelation.class::isInstance);
            Assertions.assertEquals(1, relations.size());
            Assertions.assertEquals(RESOLVED_URI,
                    relations.get(0).getFunction().getTVFProperties().getMap().get("uri"));
            Assertions.assertEquals(batchFiles(),
                    Deencapsulation.getField(relations.get(0).getFunction(), "fileStatuses"));
        }
    }

    @Test
    public void testNotificationAuditUsesExactBatch() throws Exception {
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("s3");
        AuditEvent event = runTask(null, S3_SQL, provider,
                new S3EventOffset(List.of("input/1.csv", "input/2.csv"), batchFiles()), true);
        Assertions.assertTrue(event.stmt.contains(RESOLVED_URI));
        Assertions.assertFalse(event.stmt.contains(ORIGIN_URI));
        Assertions.assertFalse(event.stmt.contains("private-value"));
    }

    private static LogicalTVFRelation auditRelation() {
        Map<String, String> properties = Map.of(
                "uri", RESOLVED_URI, "s3.secret_key", "private-value", "enclose", "\"");
        return new LogicalTVFRelation(new RelationId(1), S3.withFiles(new Properties(properties), batchFiles()),
                ImmutableList.of());
    }

    private static List<TBrokerFileStatus> batchFiles() {
        return List.of(new TBrokerFileStatus("s3://bucket/input/1.csv", false, 10, true),
                new TBrokerFileStatus("s3://bucket/input/2.csv", false, 20, true));
    }

    private AuditEvent runS3Task(RuntimeException commandFailure) throws Exception {
        S3Offset offset = new S3Offset();
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
                Mockito.when(command.getLogicalQuery()).thenReturn(auditRelation());
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
                    Map.of("URI", ORIGIN_URI, "s3.secret_key", "private-value", "enclose", "\""),
                    UserIdentity.ROOT, null);
            Deencapsulation.setField(task, "ctx", ctx);
            Deencapsulation.setField(task, "taskCommand", command);
            Deencapsulation.setField(task, "stmtExecutor", executor);
            Deencapsulation.setField(task, "runningOffset", offset);
            if (expectAudit) {
                Deencapsulation.setField(task, "auditSql",
                        Deencapsulation.invoke(task, "buildAuditSql"));
            }

            if (commandFailure == null) {
                task.run();
            } else {
                Assertions.assertThrows(JobException.class, task::run);
            }

            if (!expectAudit) {
                Mockito.verify(statusMgr, Mockito.never())
                        .submitFinishQueryToAudit(Mockito.any(AuditEvent.class));
                return null;
            }
            ArgumentCaptor<AuditEvent> auditEventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr).submitFinishQueryToAudit(auditEventCaptor.capture());
            AuditEvent auditEvent = auditEventCaptor.getValue();
            Assertions.assertTrue(commandStartTime.get() > 0);
            Assertions.assertEquals(commandStartTime.get(), auditEvent.timestamp);
            return auditEvent;
        } finally {
            ConnectContext.remove();
        }
    }
}
