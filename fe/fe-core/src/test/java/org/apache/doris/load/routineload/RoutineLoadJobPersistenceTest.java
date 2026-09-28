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

import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

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
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(jobId, jobName, 8001L,
                9001L, "127.0.0.1:9092", "persistence_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        return job;
    }

    private static OriginStatement createOriginStatement(String jobName, String loadClause) {
        return new OriginStatement("CREATE ROUTINE LOAD legacy_db." + jobName + " ON current_table "
                + loadClause + " FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"persistence_topic\")", 0);
    }

    private static String getWhereSql(RoutineLoadJob job) {
        return JsonParser.parseString(job.jobPropertiesToJsonString()).getAsJsonObject()
                .get("whereExpr").getAsString();
    }

    // Stub db 8001 and table 9001 on the shared Env mock.
    private void mockCatalog(String tableName) throws Exception {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getFunctionRegistry()).thenReturn(new FunctionRegistry());
        // MATCH ... USING ANALYZER resolves the analyzer name through the index policy manager.
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(new IndexPolicyMgr());
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
}
