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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.Principal;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.commands.ExecuteActionCommand;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteActionFactory;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.LimitUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class IcebergRemoveOrphanFilesActionTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("orphan_action");
        createTable("CREATE TABLE orphan_action.inventory (file_path VARCHAR(2048), last_modified DATETIMEV2(6)) "
                + "DUPLICATE KEY(file_path) DISTRIBUTED BY HASH(file_path) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1')");
        createView("CREATE VIEW orphan_action.inventory_view AS SELECT file_path,last_modified "
                + "FROM orphan_action.inventory");
        createView("CREATE VIEW orphan_action.inventory_one AS SELECT file_path,last_modified "
                + "FROM orphan_action.inventory LIMIT 1");
        executeSql("CREATE USER 'orphan_unprivileged'");
        executeSql("CREATE ROLE orphan_session_reader");
        executeSql("GRANT SELECT_PRIV ON internal.orphan_action.inventory_view TO ROLE 'orphan_session_reader'");
    }

    @Test
    void validatesDefaultsAndAllTenParameters() throws Exception {
        ParsedAction defaults = new ParsedAction(Collections.emptyMap());
        defaults.parse();
        Assertions.assertEquals(false, defaults.value("dry_run"));
        Assertions.assertEquals(false, defaults.value("stream_results"));
        Assertions.assertEquals(false, defaults.value("prefix_listing"));
        Assertions.assertEquals(PrefixMismatchMode.ERROR, defaults.value("prefix_mismatch_mode"));
        Assertions.assertEquals(1, defaults.getResultSchema().size());
        Assertions.assertEquals("orphan_file_location", defaults.getResultSchema().get(0).getName());
        Assertions.assertFalse(defaults.getResultSchema().get(0).isAllowNull());
        ParsedAction all = new ParsedAction(properties(
                "older_than", "2020-01-01 12:00:00.123456", "location", "s3://bucket/table",
                "dry_run", "true", "max_concurrent_deletes", "4", "file_list_view", "db.files",
                "equal_schemes", "{\"s3a,s3n\":\"s3\"}", "equal_authorities", "{\"old\":\"new\"}",
                "prefix_mismatch_mode", "ignore", "prefix_listing", "true", "stream_results", "true"));
        all.parse();
        Assertions.assertEquals(PrefixMismatchMode.IGNORE, all.value("prefix_mismatch_mode"));
        Assertions.assertEquals(4, all.value("max_concurrent_deletes"));
        Assertions.assertInstanceOf(IcebergRemoveOrphanFilesAction.class, IcebergExecuteActionFactory.createAction(
                "remove_orphan_files", Collections.emptyMap(), Optional.empty(), Optional.empty(),
                Mockito.mock(IcebergExternalTable.class)));
    }

    @Test
    void rejectsUnknownArgumentsInvalidTypesAndFilters() {
        for (Map<String, String> options : Arrays.asList(properties("unknown", "1"), properties("dry_run", "yes"),
                properties("prefix_listing", "1"), properties("stream_results", "1"),
                properties("max_concurrent_deletes", "0"), properties("max_concurrent_deletes", "-1"),
                properties("older_than", "not a timestamp"), properties("prefix_mismatch_mode", "NONE"),
                properties("equal_authorities", "{\"x\":null}"), properties("equal_schemes", "[]"))) {
            Assertions.assertThrows(UserException.class, () -> new ParsedAction(options).parse(), options.toString());
        }
        IcebergRemoveOrphanFilesAction where = new IcebergRemoveOrphanFilesAction(Collections.emptyMap(),
                Optional.empty(), Optional.of(new NereidsParser().parseExpression("id=1")));
        Assertions.assertThrows(UserException.class, where::validateIcebergAction);
    }

    @Test
    void retentionUsesSessionTimezoneAndStrict24HourBoundary() throws Exception {
        long now = Instant.parse("2026-09-11T00:00:00Z").toEpochMilli();
        ZoneId utc = ZoneId.of("UTC");
        Assertions.assertEquals(now - TimeUnit.DAYS.toMillis(3),
                IcebergRemoveOrphanFilesAction.retentionCutoff(null, utc, now));
        Assertions.assertEquals(now - TimeUnit.DAYS.toMillis(1), IcebergRemoveOrphanFilesAction.retentionCutoff(
                "2026-09-10 00:00:00", utc, now));
        Assertions.assertEquals(now - TimeUnit.DAYS.toMillis(1), IcebergRemoveOrphanFilesAction.retentionCutoff(
                "2026-09-10 00:00:00.0009999", utc, now));
        Assertions.assertThrows(AnalysisException.class, () -> IcebergRemoveOrphanFilesAction.retentionCutoff(
                "2026-09-10 00:00:00.001", utc, now));
        Assertions.assertEquals(now - TimeUnit.DAYS.toMillis(1), IcebergRemoveOrphanFilesAction.retentionCutoff(
                "2026-09-10 08:00:00", ZoneId.of("Asia/Shanghai"), now));
        Assertions.assertEquals(Instant.parse("2026-09-09T23:59:59.999999Z"),
                IcebergRemoveOrphanFilesAction.timestamp("2026-09-09 23:59:59.999999", utc));
    }

    @Test
    void equivalenceMapsAndIdentifiersAreParsedWithoutExecutingSqlFragments() throws Exception {
        Assertions.assertEquals(properties("s3a", "s3", "s3n", "s3"),
                IcebergRemoveOrphanFilesAction.parseEquivalences("{\"s3a, s3n\":\" s3 \"}"));
        for (String bad : Arrays.asList("null", "[]", "{", "{\"x\":1}", "{\"x\":{}}")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesAction.parseEquivalences(bad));
        }
        Assertions.assertEquals("SELECT `file_path`, `last_modified` FROM `catalog`.`db`.`a``b`",
                IcebergRemoveOrphanFilesAction.fileListSql("catalog.db.`a``b`"));
        for (String bad : Arrays.asList("db.t WHERE true", "db.t; DROP TABLE db.t", "now()", "a.b.c.d", "t.*")) {
            Assertions.assertThrows(AnalysisException.class, () -> IcebergRemoveOrphanFilesAction.fileListSql(bad));
        }
        Assertions.assertDoesNotThrow(() -> IcebergRemoveOrphanFilesAction.validateFileListTypes(
                Arrays.asList(Type.STRING, Type.DATETIMEV2)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesAction.validateFileListTypes(
                Arrays.asList(Type.STRING, Type.BIGINT)));
    }

    @Test
    void fileListSchemaIsCheckedAfterNormalViewAnalysisAndBeforeExecution() {
        StmtExecutor executor = new StmtExecutor(connectContext,
                "SELECT file_path,last_modified FROM orphan_action.inventory_view");
        AtomicBoolean validated = new AtomicBoolean();
        RuntimeException error = Assertions.assertThrows(RuntimeException.class, () -> executor.executeInternalQuery(() -> {
            IcebergRemoveOrphanFilesAction.validateFileListTypes(executor.getReturnTypes());
            validated.set(true);
            throw new IllegalStateException("stop before starting query");
        }, rows -> {
            throw new AssertionError("must not fetch rows");
        }));
        Assertions.assertTrue(validated.get());
        Assertions.assertTrue(error.getMessage().contains("stop before starting query"));
    }

    @Test
    void fileListQueryDoesNotElevateTheCallerToAdmin() throws Exception {
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("orphan_unprivileged", "%");
        ConnectContext child = createCtx(user, "127.0.0.1");
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(child)) {
            StmtExecutor executor = new StmtExecutor(child,
                    "SELECT file_path,last_modified FROM orphan_action.inventory_view");
            AtomicBoolean reachedExecution = new AtomicBoolean();
            RuntimeException error = Assertions.assertThrows(RuntimeException.class, () -> executor.executeInternalQuery(
                    () -> reachedExecution.set(true), rows -> { }));
            Assertions.assertFalse(reachedExecution.get());
            Assertions.assertTrue(error.getMessage().toLowerCase().contains("denied"), error.getMessage());
        }
    }

    @Test
    void outerExecutorCancellationReachesTheActiveAction() throws Exception {
        ExecuteAction action = Mockito.mock(ExecuteAction.class);
        StmtExecutor executor = new StmtExecutor(connectContext, "ALTER TABLE catalog.db.tbl "
                + "EXECUTE remove_orphan_files()");
        executor.setExecuteAction(action);
        Status reason = new Status(TStatusCode.CANCELLED, "test cancellation");
        executor.cancel(reason);
        Mockito.verify(action).cancel(reason);
        executor.setExecuteAction(null);
        executor.cancel(new Status(TStatusCode.TIMEOUT, "late cancellation"));
        Mockito.verify(action, Mockito.times(1)).cancel(Mockito.any());
    }

    @Test
    void forwardedCancellationBeforeParsingPreventsFileIo() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, new OriginStatement(
                "ALTER TABLE catalog.db.tbl EXECUTE remove_orphan_files()", 0), true);
        executor.cancel(new Status(TStatusCode.CANCELLED, "cancel before proxy parsing"));
        ParsedAction action = new ParsedAction(Collections.emptyMap());
        action.parse();
        executor.setExecuteAction(action);
        IcebergExternalTable target = Mockito.mock(IcebergExternalTable.class);
        Assertions.assertThrows(IllegalStateException.class, () -> action.execute(target));
        Mockito.verify(target, Mockito.never()).getWritableIcebergTable();
    }

    @Test
    void onlyOrphanRemovalUsesLocalRouting() {
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, command("remove_orphan_files").toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, command("REMOVE_ORPHAN_FILES").toRedirectStatus());
        for (String action : Arrays.asList("expire_snapshots", "rewrite_data_files", "rewrite_manifests")) {
            Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, command(action).toRedirectStatus());
        }
    }

    @Test
    void oldForwardedAndProxyPreparedRequestsAreRejectedBeforeLookup() throws Exception {
        ConnectContext proxy = new ConnectContext(null, true, connectContext.getSessionId());
        ExecuteActionCommand command = command("remove_orphan_files");
        // Prepared proxy execution can create a non-proxy executor, while retaining a proxy context.
        StmtExecutor executor = new StmtExecutor(proxy, new LogicalPlanAdapter(command, new StatementContext()), true);
        Assertions.assertFalse(executor.isProxy());
        Assertions.assertTrue(proxy.isProxy());
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () -> command.run(proxy, executor));
        Assertions.assertTrue(error.getMessage().contains("submitted directly"));
    }

    @Test
    void unreadableFeRejectsLocalRemovalBeforeCatalogAccess() {
        Env unreadable = Mockito.mock(Env.class);
        ExecuteActionCommand command = command("remove_orphan_files");
        StmtExecutor executor = new StmtExecutor(connectContext,
                new LogicalPlanAdapter(command, new StatementContext()));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentEnv).thenReturn(unreadable);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(connectContext, executor));
            Assertions.assertTrue(error.getMessage().contains("requires a readable FE"));
            Mockito.verify(unreadable, Mockito.never()).getCatalogMgr();
        }
    }

    @Test
    void readableFollowerExecutesWithoutRefreshJournal() throws Exception {
        Env follower = Mockito.mock(Env.class);
        Mockito.when(follower.canRead()).thenReturn(true);
        Mockito.when(follower.isMaster()).thenReturn(false);
        CatalogMgr manager = Mockito.mock(CatalogMgr.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(follower.getCatalogMgr()).thenReturn(manager);
        Mockito.doReturn(catalog).when(manager).getCatalog("catalog");
        Mockito.doReturn(database).when(catalog).getDbNullable("db");
        Mockito.doReturn(table).when(database).getTableNullable("tbl");
        ExecuteAction action = Mockito.mock(ExecuteAction.class);
        Mockito.when(action.isSupported(table)).thenReturn(true);
        ExecuteActionCommand command = command("remove_orphan_files");
        StmtExecutor executor = new StmtExecutor(connectContext,
                new LogicalPlanAdapter(command, new StatementContext()));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteActionFactory> factory = Mockito.mockStatic(ExecuteActionFactory.class)) {
            env.when(Env::getCurrentEnv).thenReturn(follower);
            factory.when(() -> ExecuteActionFactory.createAction(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.any(), Mockito.any(), Mockito.same(table))).thenReturn(action);
            command.run(connectContext, executor);
            Mockito.verify(action).execute(table);
            Mockito.verify(follower, Mockito.never()).getEditLog();
            executor.cancel(new Status(TStatusCode.CANCELLED, "after completion"));
            Mockito.verify(action, Mockito.never()).cancel(Mockito.any());
        }
    }

    private ExecuteActionCommand command(String action) {
        return (ExecuteActionCommand) new NereidsParser().parseSingle(
                "ALTER TABLE catalog.db.tbl EXECUTE " + action + "()");
    }

    @Test
    void childPreservesSessionInputsButIgnoresImplicitOutputRestrictions() throws Exception {
        ConnectContext parent = createCtx(UserIdentity.ROOT, "127.0.0.1");
        parent.setConnectionId(731);
        parent.getUserVars().put("keep", new StringLiteral("s3://bucket/keep"));
        parent.setAuthenticatedRoles(Collections.singleton("orphan_session_reader"));
        Principal principal = Mockito.mock(Principal.class);
        parent.setAuthenticatedPrincipal(principal);
        parent.setIsTempUser(true);
        TUniqueId previous = new TUniqueId(100, 200);
        parent.setQueryId(previous);
        parent.setQueryId(new TUniqueId(300, 400));
        VariableMgr.setVar(parent.getSessionVariable(), new SetVar(SessionVariable.SQL_SELECT_LIMIT,
                new StringLiteral("1")));
        VariableMgr.setVar(parent.getSessionVariable(), new SetVar(SessionVariable.DEFAULT_ORDER_BY_LIMIT,
                new StringLiteral("2")));
        parent.getSessionVariable().dryRunQuery = true;
        ConnectContext child = IcebergRemoveOrphanFilesAction.fileListContext(parent);
        Assertions.assertEquals(-1, child.getSessionVariable().getSqlSelectLimit());
        Assertions.assertEquals(-1, child.getSessionVariable().getDefaultOrderByLimit());
        Assertions.assertFalse(child.getSessionVariable().dryRunQuery);
        Assertions.assertEquals(1, parent.getSessionVariable().getSqlSelectLimit());
        Assertions.assertEquals(2, parent.getSessionVariable().getDefaultOrderByLimit());
        Assertions.assertTrue(parent.getSessionVariable().dryRunQuery);
        Assertions.assertEquals(731, child.getConnectionId());
        Assertions.assertEquals(previous, child.getLastQueryId());
        Assertions.assertNotSame(previous, child.getLastQueryId());
        Assertions.assertNull(child.queryId());
        Assertions.assertEquals(parent.getAuthenticatedRoles(), child.getAuthenticatedRoles());
        Assertions.assertSame(principal, child.getAuthenticatedPrincipal());
        Assertions.assertTrue(child.getIsTempUser());
        Assertions.assertEquals("s3://bucket/keep", child.getUserVars().get("keep").getStringValue());
        child.getUserVars().clear();
        Assertions.assertTrue(parent.getUserVars().containsKey("keep"));
    }

    @Test
    void sessionVariablesAndIdentifiersFoldToTheCallersValues() throws Exception {
        ConnectContext parent = createCtx(UserIdentity.ROOT, "127.0.0.1");
        parent.setConnectionId(731);
        parent.getUserVars().put("keep", new StringLiteral("s3://bucket/keep"));
        parent.setQueryId(new TUniqueId(100, 200));
        parent.setQueryId(new TUniqueId(300, 400));
        ConnectContext child = IcebergRemoveOrphanFilesAction.fileListContext(parent);
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(child)) {
            for (String sql : Collections.singletonList("SELECT @keep, connection_id(), last_query_id()")) {
                child.getState().reset();
                child.setQueryId(parent.getLastQueryId());
                child.resetQueryId();
                StmtExecutor executor = new StmtExecutor(child, sql);
                AtomicReference<String> plan = new AtomicReference<>();
                Assertions.assertThrows(RuntimeException.class, () -> executor.executeInternalQuery(() -> {
                    plan.set(executor.planner().getExplainString(new ExplainOptions(false, false, false)));
                    throw new IllegalStateException("stop after planning");
                }, rows -> { }));
                Assertions.assertNotNull(plan.get());
                Assertions.assertTrue(plan.get().contains("s3://bucket/keep"), plan.get());
                if (sql.contains("connection_id")) {
                    Assertions.assertTrue(plan.get().contains("731"), plan.get());
                    Assertions.assertTrue(plan.get().contains(DebugUtil.printId(parent.getLastQueryId())), plan.get());
                }
            }
        }
    }

    @Test
    void sessionGrantedRoleCanReadInventoryInTheChild() throws Exception {
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("orphan_unprivileged", "%");
        ConnectContext parent = createCtx(user, "127.0.0.1");
        parent.setAuthenticatedRoles(Collections.singleton("orphan_session_reader"));
        ConnectContext child = IcebergRemoveOrphanFilesAction.fileListContext(parent);
        AtomicBoolean authorized = new AtomicBoolean();
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(child)) {
            StmtExecutor executor = new StmtExecutor(child,
                    "SELECT file_path,last_modified FROM orphan_action.inventory_view");
            RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                    () -> executor.executeInternalQuery(() -> {
                        authorized.set(true);
                        throw new IllegalStateException("authorized, stop before BE execution");
                    }, rows -> { }));
            Assertions.assertTrue(authorized.get(), error.getMessage());
        }
    }

    @Test
    void explicitLimitViewDeliversItsNonemptyEosBatch() throws Exception {
        RowBatch batch = resultBatch("s3://bucket/table/orphan", "2019-01-01 00:00:00");
        LimitUtils.cancelIfReachLimit(batch, 1, 1, reason -> { });
        Assertions.assertTrue(batch.isEos());
        List<ResultRow> rows = new ArrayList<>();
        runWithCoordinator(batch, "SELECT file_path,last_modified FROM orphan_action.inventory_one", rows::addAll);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("s3://bucket/table/orphan", rows.get(0).get(0));
    }

    @Test
    void emptyEosCompletesAndConsumerFailureCancelsRemainingWork() throws Exception {
        RowBatch empty = new RowBatch();
        empty.setEos(true);
        runWithCoordinator(empty, "SELECT file_path,last_modified FROM orphan_action.inventory_view",
                rows -> Assertions.fail("empty EOS has no rows"));
        connectContext.getState().reset();
        runWithCoordinator(resultBatch("s3://bucket/table/orphan", "2019-01-01 00:00:00"),
                "SELECT file_path,last_modified FROM orphan_action.inventory_view", rows -> {
                    throw new IllegalStateException("candidate spool failed");
                });
    }

    private void runWithCoordinator(RowBatch batch, String sql, Consumer<List<ResultRow>> consume) throws Exception {
        EnvFactory factory = Mockito.spy(EnvFactory.getInstance());
        AtomicReference<Coordinator> coordinator = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            Coordinator real = (Coordinator) invocation.callRealMethod();
            Coordinator mocked = Mockito.spy(real);
            Mockito.doNothing().when(mocked).exec();
            Mockito.doNothing().when(mocked).close();
            Mockito.doNothing().when(mocked).cancel(Mockito.any(Status.class));
            Mockito.doReturn(batch).when(mocked).getNext();
            coordinator.set(mocked);
            return mocked;
        }).when(factory).createCoordinator(Mockito.any(ConnectContext.class), Mockito.any(Planner.class),
                Mockito.nullable(StatsErrorEstimator.class));
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            StmtExecutor executor = new StmtExecutor(connectContext, sql);
            if (batch.isEos()) {
                executor.executeInternalQuery(() -> { }, consume);
                Mockito.verify(coordinator.get(), Mockito.never()).cancel(Mockito.any());
            } else {
                RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                        () -> executor.executeInternalQuery(() -> { }, consume));
                Assertions.assertTrue(error.getMessage().contains("candidate spool failed"), error.getMessage());
                Mockito.verify(coordinator.get()).cancel(Mockito.any());
            }
            Mockito.verify(coordinator.get()).close();
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.queryId()));
        }
    }

    private RowBatch resultBatch(String... values) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        for (String value : values) {
            serializer.writeLenEncodedString(value);
        }
        TResultBatch result = new TResultBatch();
        result.addToRows(serializer.toByteBuffer());
        RowBatch batch = new RowBatch();
        batch.setBatch(result);
        batch.setEos(false);
        return batch;
    }

    @Test
    void explicitOffsetAndRegionCutoffsKeepSparkDstSemantics(@TempDir Path directory) throws Exception {
        ZoneId newYork = ZoneId.of("America/New_York");
        connectContext.getSessionVariable().setTimeZone(newYork.getId());
        Assertions.assertEquals(Instant.parse("2025-11-02T06:30:00Z"),
                IcebergRemoveOrphanFilesAction.timestamp("2025-11-02 01:30:00-05:00", newYork));
        Assertions.assertEquals(Instant.parse("2025-11-02T05:30:00Z"),
                IcebergRemoveOrphanFilesAction.timestamp("2025-11-02 01:30:00-04:00", newYork));
        Assertions.assertEquals(Instant.parse("2025-11-02T06:30:00Z"),
                IcebergRemoveOrphanFilesAction.timestamp("2025-11-02 06:30:00Z", newYork));
        Assertions.assertEquals(Instant.parse("2025-11-02T05:30:00Z"),
                IcebergRemoveOrphanFilesAction.timestamp("2025-11-02 01:30:00", newYork));
        for (String text : Arrays.asList("2025-03-09 02:30:00", "2025-03-09 02:30:00 America/New_York")) {
            Assertions.assertEquals(Instant.parse("2025-03-09T07:30:00Z"),
                    IcebergRemoveOrphanFilesAction.timestamp(text, newYork));
        }
        Table table = new HadoopTables(new Configuration()).create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                directory.resolve("table").toString());
        Path orphan = Files.write(directory.resolve("table/orphan.parquet"), new byte[] {1});
        Files.setLastModifiedTime(orphan, FileTime.from(Instant.parse("2025-11-02T06:00:00Z")));
        long cutoff = IcebergRemoveOrphanFilesAction.retentionCutoff("2025-11-02 01:30:00-05:00", newYork,
                Instant.parse("2026-09-11T00:00:00Z").toEpochMilli());
        List<List<String>> candidates = new IcebergOrphanFiles(table, new Configuration(), table.location(), cutoff,
                Collections.emptyMap(), Collections.emptyMap(), PrefixMismatchMode.ERROR, () -> { },
                new ExecutionAuthenticator() {}, directory.resolve("spool")).execute(null, false, true, false, null);
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList(
                new org.apache.hadoop.fs.Path(orphan.toUri()).toString())), candidates);
    }

    @Test
    void fileListBatchesFilterNullsLocationAndTimestampWithoutChangingCaller(@TempDir Path directory) throws Exception {
        Table iceberg = new HadoopTables(new Configuration()).create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                directory.resolve("table").toString());
        IcebergExternalTable target = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(target.getWritableIcebergTable()).thenReturn(iceberg);
        Mockito.when(target.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getConfiguration()).thenReturn(new Configuration());
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        connectContext.setStartTime();
        String oldPath = iceberg.location() + "/orphan";
        ParsedAction action = new ParsedAction(properties("file_list_view", "orphan_action.inventory",
                "older_than", "2020-01-01", "dry_run", "true", "stream_results", "true"));
        action.parse();
        AtomicBoolean validated = new AtomicBoolean();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (query, construction) -> {
                    ConnectContext child = (ConnectContext) construction.arguments().get(0);
                    Assertions.assertEquals(connectContext.getCurrentUserIdentity(), child.getCurrentUserIdentity());
                    Assertions.assertEquals(connectContext.getDefaultCatalog(), child.getDefaultCatalog());
                    Mockito.when(query.getReturnTypes()).thenReturn(Arrays.asList(Type.STRING, Type.DATETIMEV2));
                    Mockito.doAnswer(invocation -> {
                        ((Runnable) invocation.getArgument(0)).run();
                        validated.set(true);
                        Consumer<List<ResultRow>> consume = invocation.getArgument(1);
                        consume.accept(Arrays.asList(new ResultRow(Arrays.asList(oldPath, "2019-01-01")),
                                new ResultRow(Arrays.asList(oldPath, "2020-01-01")),
                                new ResultRow(Arrays.asList(null, "2019-01-01")),
                                new ResultRow(Arrays.asList(oldPath, null)),
                                new ResultRow(Arrays.asList("/outside/location", "2019-01-01"))));
                        return null;
                    }).when(query).executeInternalQuery(Mockito.any(Runnable.class), Mockito.any());
                })) {
            ResultSet result = action.execute(target);
            Assertions.assertEquals(Collections.singletonList(Collections.singletonList(oldPath)), result.getResultRows());
        }
        Assertions.assertTrue(validated.get());
        Assertions.assertEquals(connectContext, ConnectContext.get());
    }

    private static Map<String, String> properties(String... pairs) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    @Override
    protected void runBeforeEach() {
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.getState().reset();
        connectContext.setExecutor(null);
        connectContext.setStartTime();
        connectContext.setStatementContext(new StatementContext());
    }

    private static final class ParsedAction extends IcebergRemoveOrphanFilesAction {
        private ParsedAction(Map<String, String> properties) {
            super(properties, Optional.empty(), Optional.empty());
        }

        private void parse() throws UserException {
            namedArguments.validate(properties);
            validateIcebergAction();
        }

        private Object value(String name) {
            return namedArguments.getValue(name);
        }
    }
}
