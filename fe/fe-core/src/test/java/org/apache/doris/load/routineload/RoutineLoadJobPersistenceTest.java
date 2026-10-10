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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.ColumnRefExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MapLiteral;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RoutineLoadJobPersistenceTest {
    private MockedStatic<Env> envMock;
    private Env env;
    private ConnectContext previousContext;
    private EditLog editLog;
    private RoutineLoadManager manager;
    private TxnStateCallbackFactory callbackFactory;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext.remove();
        envMock = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        editLog = Mockito.mock(EditLog.class);
        manager = new RoutineLoadManager();
        callbackFactory = new TxnStateCallbackFactory();
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getRoutineLoadManager()).thenReturn(manager);
        Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(Mockito.mock(RoutineLoadTaskScheduler.class));
        Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testCreateJournalBeforePublishingJob(LoadDataSourceType dataSourceType) throws Exception {
        RoutineLoadJob job = createJob(dataSourceType);
        Mockito.doAnswer(invocation -> {
            // Exercise scheduling at the old race window, before the create record is serialized.
            for (RoutineLoadJob visibleJob : manager.getRoutineLoadJobByState(EnumSet.of(JobState.NEED_SCHEDULE))) {
                setSourceProgress(visibleJob, dataSourceType);
                visibleJob.divideRoutineLoadJob(1);
            }
            byte[] record = serialize(invocation.getArgument(0));
            Assertions.assertEquals(JobState.NEED_SCHEDULE.name(), serializedState(record));
            Assertions.assertNull(manager.getJob(job.getId()));
            Assertions.assertNull(callbackFactory.getCallback(job.getId()));
            Assertions.assertEquals(0, job.getSizeOfRoutineLoadTaskInfoList());
            return null;
        }).when(editLog).logCreateRoutineLoadJob(job);

        manager.addRoutineLoadJob(job, "db", "tbl");

        Mockito.verify(editLog).logCreateRoutineLoadJob(job);
        Assertions.assertSame(job, manager.getJob(job.getId()));
        Assertions.assertSame(job, callbackFactory.getCallback(job.getId()));
        setSourceProgress(job, dataSourceType);
        job.divideRoutineLoadJob(1);
        Assertions.assertEquals(JobState.RUNNING, job.getState());
        Assertions.assertEquals(1, job.getSizeOfRoutineLoadTaskInfoList());
    }

    @Test
    public void testImageRestoresLoadDefinitionFromOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1001L, "image_job", 8001L,
                9001L, "127.0.0.1:9092", "image_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.image_job ON stale_table "
                + "COLUMNS TERMINATED BY '|', "
                + "COLUMNS(source_col, mapped_col = source_col + 1), "
                + "PRECEDING FILTER source_col > 1, WHERE mapped_col <= 10 "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"image_topic\")", 0);
        job.setRoutineLoadDesc(new RoutineLoadDesc(new Separator(",", ","), null,
                Lists.newArrayList(new ImportColumnDesc("wrong_column")),
                null, null, null, null, LoadTask.MergeType.APPEND, null));

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);

        Assertions.assertEquals("|", job.getColumnSeparator().getSeparator());
        Assertions.assertEquals(2, job.getColumnExprDescs().descs.size());
        Assertions.assertEquals("source_col", job.getColumnExprDescs().descs.get(0).getColumnName());
        Assertions.assertEquals("mapped_col", job.getColumnExprDescs().descs.get(1).getColumnName());
        Assertions.assertNotNull(job.getPrecedingFilter());
        Assertions.assertNotNull(job.getWhereExpr());
    }

    @Test
    public void testAlterReplayMergesCurrentDefinitionIntoOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(2001L, "alter_job", 8001L,
                9001L, "127.0.0.1:9092", "alter_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.alter_job ON current_table WITH MERGE "
                + "COLUMNS TERMINATED BY ',', "
                + "COLUMNS(source_col, mapped_col = source_col + 1), "
                + "PRECEDING FILTER source_col > 1, WHERE mapped_col < 100, "
                + "PARTITION(p1), DELETE ON delete_flag = 1, ORDER BY seq_col "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"alter_topic\")", 0);

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR alter_job COLUMNS TERMINATED BY '|', WHERE mapped_col < 50", 0));
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR alter_job "
                        + "PRECEDING FILTER content MATCH_ANY 'hello' USING ANALYZER 'english'", 0));
        String propertyOnlyOrigin = job.origStmt.originStmt;
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR alter_job PROPERTIES (\"max_error_number\" = \"10\")", 0));
        Assertions.assertEquals(propertyOnlyOrigin, job.origStmt.originStmt);

        Assertions.assertTrue(job.origStmt.originStmt.startsWith("CREATE ROUTINE LOAD"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("COLUMNS TERMINATED BY '|'"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("COLUMNS("));
        Assertions.assertTrue(job.origStmt.originStmt.contains("WHERE"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("PRECEDING FILTER"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("USING ANALYZER"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("PARTITION(`p1`)"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("DELETE ON"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("ORDER BY `seq_col`"));
        Assertions.assertTrue(job.origStmt.originStmt.contains("WITH MERGE"));

        JsonObject expectedProperties = JsonParser.parseString(job.jobPropertiesToJsonString()).getAsJsonObject();
        RoutineLoadJob restored = imageRoundTrip(job);
        JsonObject restoredProperties = JsonParser.parseString(restored.jobPropertiesToJsonString()).getAsJsonObject();
        for (String key : Lists.newArrayList("column_separator", "precedingFilter",
                "whereExpr", "partitions", "delete", "sequence_col", "merge_type")) {
            Assertions.assertEquals(expectedProperties.get(key), restoredProperties.get(key), key);
        }
        Assertions.assertTrue(restoredProperties.get("columnToColumnExpr").getAsString().contains("mapped_col="));
        Assertions.assertEquals(job.origStmt.originStmt, restored.origStmt.originStmt);
    }

    @Test
    public void testReplayUsesStatementIndexAndQuotesReservedTableName() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(3001L, "order_job", 8001L,
                9001L, "127.0.0.1:9092", "order_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.order_job ON `order` "
                + "COLUMNS TERMINATED BY ',' "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"order_topic\")", 0);

        OriginStatement multiStatementAlter = new OriginStatement(
                "ALTER ROUTINE LOAD FOR order_job PROPERTIES (\"max_error_number\" = \"10\");"
                        + "ALTER ROUTINE LOAD FOR order_job COLUMNS TERMINATED BY '|'", 1);
        mockCatalog("order");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(multiStatementAlter, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);

        Assertions.assertEquals("|", job.getColumnSeparator().getSeparator());
        Assertions.assertTrue(job.origStmt.originStmt.contains(" ON `order` "));
        Assertions.assertEquals(Long.toString(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES),
                job.sessionVariables.get(SessionVariable.SQL_MODE));
    }

    @Test
    public void testHexSeparatorSurvivesCanonicalCreateAndImageRoundTrip() throws Exception {
        KafkaRoutineLoadJob job = newPausedJob(4001L, "hex_separator_job");
        job.origStmt = createOriginStatement("hex_separator_job", "COLUMNS TERMINATED BY ','");

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR hex_separator_job COLUMNS TERMINATED BY '\\x01'", 0));
        Assertions.assertEquals(1, job.getColumnSeparator().getSeparator().charAt(0));
        Assertions.assertEquals("\\x01", job.getColumnSeparator().getOriSeparator());
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);

        Assertions.assertEquals(1, job.getColumnSeparator().getSeparator().charAt(0));
        Assertions.assertEquals("\\x01", job.getColumnSeparator().getOriSeparator());
    }

    @Test
    public void testTabSeparatorSurvivesCanonicalCreateAndImageRoundTrip() throws Exception {
        KafkaRoutineLoadJob job = newPausedJob(4002L, "tab_separator_job");
        job.origStmt = createOriginStatement("tab_separator_job", "COLUMNS TERMINATED BY ','");

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR tab_separator_job COLUMNS TERMINATED BY '\\t'", 0));
        Assertions.assertEquals("\t", job.getColumnSeparator().getSeparator());
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);

        Assertions.assertEquals("\t", job.getColumnSeparator().getSeparator());
        Assertions.assertEquals("\\t", job.getColumnSeparator().getOriSeparator());
    }

    @Test
    public void testBackslashLiteralSurvivesCanonicalCreateAndImageRoundTrip() throws Exception {
        KafkaRoutineLoadJob job = newPausedJob(5001L, "backslash_literal_job");
        job.origStmt = createOriginStatement("backslash_literal_job", "WHERE text1 = 'old'");

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR backslash_literal_job WHERE text1 = 'A\\\\nB'", 0));
        String expectedWhere = getWhereSql(job);
        Assertions.assertTrue(expectedWhere.contains("\\n"));
        Assertions.assertFalse(expectedWhere.contains("\n"));
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        Assertions.assertEquals(expectedWhere, getWhereSql(job));
    }

    @Test
    public void testAlterSqlModeSurvivesCanonicalCreateAndImageRoundTrip() throws Exception {
        KafkaRoutineLoadJob job = newPausedJob(5002L, "no_backslash_literal_job");
        job.origStmt = createOriginStatement("no_backslash_literal_job", "WHERE text1 = 'old'");

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR no_backslash_literal_job WHERE text1 = 'A\\nB'", 0),
                SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        String expectedWhere = getWhereSql(job);
        Assertions.assertTrue(expectedWhere.contains("\\n"));
        Assertions.assertFalse(expectedWhere.contains("\n"));
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        Assertions.assertEquals(expectedWhere, getWhereSql(job));

        Assertions.assertEquals(Long.toString(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES),
                job.sessionVariables.get(SessionVariable.SQL_MODE));
    }

    @Test
    public void testJsonFunctionLiteralSurvivesCanonicalCreateAndImageRoundTrip() throws Exception {
        KafkaRoutineLoadJob job = newPausedJob(5003L, "json_function_literal_job");
        job.origStmt = createOriginStatement("json_function_literal_job", "WHERE text1 = 'old'");

        mockCatalog("current_table");
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        job.replayLoadDefinition(new OriginStatement(
                "ALTER ROUTINE LOAD FOR json_function_literal_job "
                        + "WHERE json_object('key', 'A\\\\nB') IS NOT NULL", 0));
        String expectedWhere = getWhereSql(job);
        Assertions.assertTrue(expectedWhere.contains("json_object"));
        Assertions.assertTrue(expectedWhere.contains("\\n"));
        Assertions.assertFalse(expectedWhere.contains("\n"));
        job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        Assertions.assertEquals(expectedWhere, getWhereSql(job));
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testLikeEscapeSurvivesAlterReplayAndImage(LoadDataSourceType dataSourceType) throws Exception {
        assertExpressionSurvivesAlterReplayAndImage(dataSourceType, "WHERE city LIKE 'A!_%' ESCAPE '!'", job -> {
            FunctionCallExpr like = (FunctionCallExpr) job.getWhereExpr();
            Assertions.assertEquals("like", like.getFnName().getFunction());
            Assertions.assertEquals(3, like.getChildren().size());
            Assertions.assertEquals("A!_%", ((LiteralExpr) like.getChild(1)).getStringValue());
            Assertions.assertEquals("!", ((LiteralExpr) like.getChild(2)).getStringValue());
        });
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testMapLiteralSurvivesAlterReplayAndImage(LoadDataSourceType dataSourceType) throws Exception {
        assertExpressionSurvivesAlterReplayAndImage(dataSourceType,
                "COLUMNS(mapped = element_at({'a': 1, 'b': 2}, 'b'))", job -> {
                    List<MapLiteral> maps = Lists.newArrayList();
                    job.getColumnExprDescs().descs.get(0).getExpr().collect(MapLiteral.class, maps);
                    Assertions.assertEquals(1, maps.size());
                    MapLiteral map = maps.get(0);
                    Assertions.assertEquals(4, map.getChildren().size());
                    Assertions.assertEquals("a", ((LiteralExpr) map.getChild(0)).getStringValue());
                    Assertions.assertEquals(1, ((LiteralExpr) map.getChild(1)).getLongValue());
                    Assertions.assertEquals("b", ((LiteralExpr) map.getChild(2)).getStringValue());
                    Assertions.assertEquals(2, ((LiteralExpr) map.getChild(3)).getLongValue());
                });
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testQuotedLambdaSurvivesAlterReplayAndImage(LoadDataSourceType dataSourceType) throws Exception {
        String columns = "COLUMNS(mapped = array_map(`x-y` -> `x-y` + 1, [1, 2]), "
                + "paired = array_map((`x``y`, `select`) -> `x``y` + `select`, [1, 2], [3, 4]))";
        assertExpressionSurvivesAlterReplayAndImage(dataSourceType, columns, job -> {
            assertLambdaNames(job.getColumnExprDescs().descs.get(0).getExpr(), List.of("x-y"));
            assertLambdaNames(job.getColumnExprDescs().descs.get(1).getExpr(), List.of("x`y", "select"));
        });
    }

    private static void assertLambdaNames(Expr expression, List<String> expectedNames) {
        List<LambdaFunctionExpr> lambdas = Lists.newArrayList();
        expression.collect(LambdaFunctionExpr.class, lambdas);
        Assertions.assertEquals(1, lambdas.size());
        Assertions.assertEquals(expectedNames, lambdas.get(0).getNames());
        List<ColumnRefExpr> references = Lists.newArrayList();
        lambdas.get(0).getSlotExprs().get(0).collect(ColumnRefExpr.class, references);
        Assertions.assertEquals(expectedNames, references.stream().map(ColumnRefExpr::getName)
                .collect(Collectors.toList()));
    }

    private void assertExpressionSurvivesAlterReplayAndImage(LoadDataSourceType dataSourceType, String loadClause,
            Consumer<RoutineLoadJob> assertExpression) throws Exception {
        mockCatalog("current_table");
        RoutineLoadJob initial = newPausedJob(dataSourceType, 5004L, "expression_job");
        initial.origStmt = createOriginStatement(dataSourceType, "expression_job", loadClause);
        RoutineLoadJob leader = imageRoundTrip(initial);
        RoutineLoadJob follower = imageRoundTrip(initial);
        Assertions.assertEquals(JobState.PAUSED, leader.getState());
        assertExpression.accept(leader);

        // Change only the separator: every existing expression must survive rebuilding the full CREATE.
        OriginStatement alterStatement = new OriginStatement(
                "ALTER ROUTINE LOAD FOR expression_job COLUMNS TERMINATED BY '|'", 0);
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Maps.newHashMap());
        Mockito.when(command.hasLoadProperty()).thenReturn(true);
        Mockito.when(command.getOriginStatement()).thenReturn(alterStatement);
        Mockito.when(command.getSqlMode()).thenReturn(SqlModeHelper.MODE_DEFAULT);
        Mockito.when(command.getRoutineLoadDesc()).thenReturn(new RoutineLoadDesc(new Separator("|", "|"), null,
                null, null, null, null, null, LoadTask.MergeType.APPEND, null));
        ConnectContext ctx = new ConnectContext();
        ctx.setDatabase("legacy_db");
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        try {
            ctx.setThreadLocalInfo();
            leader.modifyProperties(command);
        } finally {
            ctx.cleanup();
        }

        ArgumentCaptor<AlterRoutineLoadJobOperationLog> logCaptor =
                ArgumentCaptor.forClass(AlterRoutineLoadJobOperationLog.class);
        Mockito.verify(editLog).logAlterRoutineLoadJob(logCaptor.capture());
        AlterRoutineLoadJobOperationLog log;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialize(logCaptor.getValue())))) {
            log = AlterRoutineLoadJobOperationLog.read(in);
        }
        follower.replayModifyProperties(log);
        Assertions.assertEquals(leader.origStmt.originStmt, follower.origStmt.originStmt);
        for (RoutineLoadJob job : List.of(leader, follower, imageRoundTrip(leader), imageRoundTrip(follower))) {
            Assertions.assertEquals(JobState.PAUSED, job.getState());
            Assertions.assertEquals("|", job.getColumnSeparator().getSeparator());
            assertExpression.accept(job);
        }
    }

    @ParameterizedTest
    @MethodSource("replayFailures")
    public void testUnreplayableAlterCancelsJobWithoutStoppingReplay(LoadDataSourceType dataSourceType,
            ReplayFailure failure) throws Exception {
        RoutineLoadJob job = newPausedJob(dataSourceType, 6001L, "unreplayable_job");
        job.origStmt = createOriginStatement(dataSourceType, "unreplayable_job", "COLUMNS TERMINATED BY ','");
        job.setRoutineLoadDesc(new RoutineLoadDesc(new Separator(",", ","), null, null,
                null, null, null, null, LoadTask.MergeType.APPEND, null));
        String previousOrigin = job.origStmt.originStmt;
        manager.replayCreateRoutineLoadJob(job);
        Assertions.assertSame(job, callbackFactory.getCallback(job.getId()));

        Database database = mockCatalog("current_table");
        InternalCatalog catalog = env.getInternalCatalog();
        if (failure == ReplayFailure.MISSING_DATABASE) {
            Mockito.when(catalog.getDb(8001L)).thenReturn(Optional.empty());
            Mockito.when(catalog.getDbOrMetaException(8001L))
                    .thenThrow(new MetaNotFoundException("unknown database 8001"));
        } else if (failure == ReplayFailure.MISSING_TABLE) {
            Mockito.when(database.getTableOrMetaException(9001L))
                    .thenThrow(new MetaNotFoundException("unknown table 9001"));
        }
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(job.getId(),
                Maps.newHashMap(), null, new OriginStatement(failure.alterSql, 0), SqlModeHelper.MODE_DEFAULT,
                null);

        manager.replayAlterRoutineLoadJob(log);

        Assertions.assertEquals(JobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.cancelReason.getMsg().contains("FE replay alter routine load failed"),
                job.cancelReason.getMsg());
        Assertions.assertNull(callbackFactory.getCallback(job.getId()));
        Assertions.assertEquals(previousOrigin, job.origStmt.originStmt);
        Assertions.assertEquals(",", job.getColumnSeparator().getSeparator());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCloudSourceReplayFailureCancelsOnlyAlterWithLoadDefinition(boolean hasLoadDefinition)
            throws Exception {
        RoutineLoadJob job = newPausedJob(6002L, "cloud_replay_job");
        job.origStmt = createOriginStatement("cloud_replay_job", "COLUMNS TERMINATED BY ','");
        String previousOrigin = job.origStmt.originStmt;
        manager.replayCreateRoutineLoadJob(job);
        AlterRoutineLoadJobOperationLog log = cloudSourceAlterLog(job, hasLoadDefinition);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.resetRLProgress(Mockito.any())).thenThrow(new RpcException("MS", "unavailable"));

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<MetaServiceProxy> proxyMock = Mockito.mockStatic(MetaServiceProxy.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            proxyMock.when(MetaServiceProxy::getInstance).thenReturn(proxy);

            Assertions.assertDoesNotThrow(() -> manager.replayAlterRoutineLoadJob(log));

            Mockito.verify(proxy).resetRLProgress(Mockito.argThat(request ->
                    request.getDbId() == job.getDbId() && request.getJobId() == job.getId()));
            Assertions.assertEquals(previousOrigin, job.origStmt.originStmt);
            Assertions.assertNull(job.getWhereExpr());
            if (hasLoadDefinition) {
                Assertions.assertEquals(JobState.CANCELLED, job.getState());
                Assertions.assertTrue(job.cancelReason.getMsg().contains("unavailable"));
                Assertions.assertTrue(job.getEndTimestamp() > 0);
                Assertions.assertNull(callbackFactory.getCallback(job.getId()));
                manager.replayChangeRoutineLoadJob(new RoutineLoadOperation(job.getId(), JobState.NEED_SCHEDULE));
                Assertions.assertEquals(JobState.CANCELLED, job.getState());
                Assertions.assertNull(callbackFactory.getCallback(job.getId()));
            } else {
                // Old and source-only journals have no load definition and keep their previous behavior.
                Assertions.assertEquals(JobState.PAUSED, job.getState());
                Assertions.assertNull(job.cancelReason);
                Assertions.assertEquals(-1, job.getEndTimestamp());
                Assertions.assertSame(job, callbackFactory.getCallback(job.getId()));
            }

            RoutineLoadJob otherJob = newPausedJob(6003L, "other_job");
            manager.replayCreateRoutineLoadJob(otherJob);
            manager.replayChangeRoutineLoadJob(new RoutineLoadOperation(otherJob.getId(), JobState.NEED_SCHEDULE));
            Assertions.assertEquals(JobState.NEED_SCHEDULE, otherJob.getState());
            Mockito.verifyNoInteractions(editLog);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Cloud.MetaServiceCode.class, names = {"OK", "ROUTINE_LOAD_PROGRESS_NOT_FOUND"})
    public void testCloudSourceReplayAppliesLoadDefinition(Cloud.MetaServiceCode code) throws Exception {
        RoutineLoadJob job = newPausedJob(6002L, "cloud_replay_job");
        job.origStmt = createOriginStatement("cloud_replay_job", "COLUMNS TERMINATED BY ','");
        manager.replayCreateRoutineLoadJob(job);
        mockCatalog("current_table");
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getDefaultCloudCluster(Mockito.anyString())).thenReturn("test_compute_group");
        CloudSystemInfoService systemInfo = Mockito.mock(CloudSystemInfoService.class);
        envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        Mockito.when(systemInfo.getCloudClusterNames()).thenReturn(Lists.newArrayList("test_compute_group"));
        AlterRoutineLoadJobOperationLog log = cloudSourceAlterLog(job, true);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.resetRLProgress(Mockito.any())).thenReturn(Cloud.ResetRLProgressResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(code)).build());

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<MetaServiceProxy> proxyMock = Mockito.mockStatic(MetaServiceProxy.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            proxyMock.when(MetaServiceProxy::getInstance).thenReturn(proxy);

            manager.replayAlterRoutineLoadJob(log);

            Mockito.verify(proxy).resetRLProgress(Mockito.any());
            Assertions.assertEquals(JobState.PAUSED, job.getState());
            Assertions.assertEquals("127.0.0.2:9092", ((KafkaRoutineLoadJob) job).getBrokerList());
            Assertions.assertNotNull(job.getWhereExpr());
            Assertions.assertTrue(job.origStmt.originStmt.contains("WHERE"));
            Assertions.assertNull(job.cancelReason);
            Assertions.assertSame(job, callbackFactory.getCallback(job.getId()));
            manager.replayChangeRoutineLoadJob(new RoutineLoadOperation(job.getId(), JobState.NEED_SCHEDULE));
            Assertions.assertEquals(JobState.NEED_SCHEDULE, job.getState());
            Mockito.verifyNoInteractions(editLog);
        }
    }

    private AlterRoutineLoadJobOperationLog cloudSourceAlterLog(RoutineLoadJob job, boolean hasLoadDefinition)
            throws Exception {
        KafkaDataSourceProperties sourceProperties = new KafkaDataSourceProperties(
                Map.of("kafka_broker_list", "127.0.0.2:9092"));
        sourceProperties.setAlter(true);
        sourceProperties.setTimezone("Asia/Shanghai");
        sourceProperties.analyze();
        OriginStatement alterStatement = hasLoadDefinition
                ? new OriginStatement("ALTER ROUTINE LOAD FOR cloud_replay_job WHERE c1 = 'new' "
                        + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.2:9092\")", 0)
                : null;
        return new AlterRoutineLoadJobOperationLog(job.getId(), Maps.newHashMap(), sourceProperties,
                alterStatement, hasLoadDefinition ? SqlModeHelper.MODE_DEFAULT : null);
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testFailedAlterLeavesJobAndJournalUnchanged(LoadDataSourceType dataSourceType) throws Exception {
        RoutineLoadJob job = newPausedJob(dataSourceType, 7001L, "atomic_alter_job");
        job.origStmt = createOriginStatement(dataSourceType, "atomic_alter_job", "COLUMNS TERMINATED BY ','");
        job.setRoutineLoadDesc(new RoutineLoadDesc(new Separator(",", ","), null, null,
                null, null, null, null, LoadTask.MergeType.APPEND, null));
        long previousMaxErrorNum = job.maxErrorNum;
        Map<String, String> previousJobProperties = Maps.newHashMap(job.jobProperties);
        Map<String, String> previousSessionVariables = Maps.newHashMap(job.sessionVariables);
        String previousOrigin = job.origStmt.originStmt;

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "10");
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(jobProperties);
        Mockito.when(command.getDataSourceProperties()).thenReturn(null);
        Mockito.when(command.hasLoadProperty()).thenReturn(true);
        Mockito.when(command.getRoutineLoadDesc()).thenReturn(new RoutineLoadDesc(new Separator("|", "|"), null,
                null, null, null, null, null, LoadTask.MergeType.APPEND, null));
        Mockito.when(command.getOriginStatement()).thenReturn(new OriginStatement(
                "ALTER ROUTINE LOAD FOR atomic_alter_job COLUMNS TERMINATED BY '|', "
                        + "PROPERTIES (\"max_error_number\" = \"10\")", 0));
        Mockito.when(command.getSqlMode()).thenReturn(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        Mockito.when(command.getSessionVariables()).thenReturn(Maps.newHashMap());
        // The table is dropped after the ALTER was analyzed, so the persisted CREATE statement can not be built.
        Database database = mockCatalog("current_table");
        Mockito.when(database.getTableOrMetaException(9001L))
                .thenThrow(new MetaNotFoundException("unknown table 9001"));

        Assertions.assertThrows(MetaNotFoundException.class, () -> job.modifyProperties(command));

        Assertions.assertEquals(previousMaxErrorNum, job.maxErrorNum);
        Assertions.assertEquals(previousJobProperties, job.jobProperties);
        Assertions.assertEquals(previousSessionVariables, job.sessionVariables);
        Assertions.assertEquals(",", job.getColumnSeparator().getSeparator());
        Assertions.assertEquals(previousOrigin, job.origStmt.originStmt);
        Mockito.verify(editLog, Mockito.never()).logAlterRoutineLoadJob(Mockito.any());
    }

    private static Stream<Arguments> replayFailures() {
        return Stream.of(LoadDataSourceType.KAFKA, LoadDataSourceType.KINESIS)
                .flatMap(dataSourceType -> Stream.of(ReplayFailure.values())
                        .map(failure -> Arguments.of(dataSourceType, failure)));
    }

    private RoutineLoadJob createJob(LoadDataSourceType dataSourceType) {
        if (dataSourceType == LoadDataSourceType.KINESIS) {
            return new KinesisRoutineLoadJob(1L, "job", 1L, 1L, "us-east-1", "stream", UserIdentity.ADMIN);
        }
        return new KafkaRoutineLoadJob(1L, "job", 1L, 1L, "127.0.0.1:9092", "topic", UserIdentity.ADMIN);
    }

    private void setSourceProgress(RoutineLoadJob job, LoadDataSourceType dataSourceType) {
        if (dataSourceType == LoadDataSourceType.KINESIS) {
            job.progress = new KinesisProgress(Map.of("shard-0", "100"));
            Deencapsulation.setField(job, "openKinesisShards", Lists.newArrayList("shard-0"));
        } else {
            job.progress = new KafkaProgress(Map.of(0, 100L));
            Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(0));
        }
    }

    private byte[] serialize(Writable value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            value.write(out);
        }
        return bytes.toByteArray();
    }

    private String serializedState(byte[] record) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(record))) {
            return JsonParser.parseString(Text.readString(in)).getAsJsonObject().get("st").getAsString();
        }
    }

    private static KafkaRoutineLoadJob newPausedJob(long jobId, String jobName) {
        return (KafkaRoutineLoadJob) newPausedJob(LoadDataSourceType.KAFKA, jobId, jobName);
    }

    private static RoutineLoadJob newPausedJob(LoadDataSourceType dataSourceType, long jobId, String jobName) {
        RoutineLoadJob job = dataSourceType == LoadDataSourceType.KINESIS
                ? new KinesisRoutineLoadJob(jobId, jobName, 8001L, 9001L, "us-east-1", "persistence_stream",
                        UserIdentity.ADMIN)
                : new KafkaRoutineLoadJob(jobId, jobName, 8001L, 9001L, "127.0.0.1:9092", "persistence_topic",
                        UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        return job;
    }

    private static OriginStatement createOriginStatement(String jobName, String loadClause) {
        return createOriginStatement(LoadDataSourceType.KAFKA, jobName, loadClause);
    }

    private static OriginStatement createOriginStatement(LoadDataSourceType dataSourceType, String jobName,
            String loadClause) {
        String dataSource = dataSourceType == LoadDataSourceType.KINESIS
                ? " FROM KINESIS (\"aws.region\" = \"us-east-1\", \"kinesis_stream\" = \"persistence_stream\")"
                : " FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                        + "\"kafka_topic\" = \"persistence_topic\")";
        return new OriginStatement("CREATE ROUTINE LOAD legacy_db." + jobName + " ON current_table "
                + loadClause + dataSource, 0);
    }

    private static String getWhereSql(RoutineLoadJob job) {
        return JsonParser.parseString(job.jobPropertiesToJsonString()).getAsJsonObject()
                .get("whereExpr").getAsString();
    }

    // Stub db 8001 and table 9001 on the shared Env mock, and return the database so tests can break it.
    private Database mockCatalog(String tableName) throws Exception {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getFunctionRegistry()).thenReturn(new FunctionRegistry());
        // MATCH ... USING ANALYZER resolves the analyzer name through the index policy manager.
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(new IndexPolicyMgr());
        // Function binding checks the database privilege before it looks up UDFs.
        Mockito.when(env.getAccessManager()).thenReturn(Mockito.mock(AccessControllerManager.class));
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getDb(8001L)).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDb("legacy_db")).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDbOrMetaException(8001L)).thenReturn(database);
        Mockito.when(catalog.getDbOrAnalysisException("legacy_db")).thenReturn(database);
        Mockito.when(database.getName()).thenReturn("legacy_db");
        Mockito.when(database.getFullName()).thenReturn("legacy_db");
        Mockito.when(database.getTableOrMetaException(9001L)).thenReturn(table);
        Mockito.when(database.getTableOrAnalysisException(tableName)).thenReturn(table);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);
        Mockito.when(table.getFullSchema()).thenReturn(Lists.newArrayList());
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return database;
    }

    private static RoutineLoadJob imageRoundTrip(RoutineLoadJob job) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            job.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return RoutineLoadJob.read(in);
        }
    }

    private enum ReplayFailure {
        // The persisted ALTER statement can not be parsed any more.
        UNPARSEABLE_STATEMENT("ALTER ROUTINE LOAD FOR unreplayable_job COLUMNS TERMINATED BY"),
        // The ALTER parses but its expression no longer analyzes, as after an upgrade changed the analysis rules.
        UNANALYZABLE_EXPRESSION("ALTER ROUTINE LOAD FOR unreplayable_job WHERE no_such_function(c1) > 1"),
        // The database or table was dropped by the time the ALTER is replayed.
        MISSING_DATABASE("ALTER ROUTINE LOAD FOR unreplayable_job COLUMNS TERMINATED BY '|'"),
        MISSING_TABLE("ALTER ROUTINE LOAD FOR unreplayable_job COLUMNS TERMINATED BY '|'");

        private final String alterSql;

        ReplayFailure(String alterSql) {
            this.alterSql = alterSql;
        }
    }
}
