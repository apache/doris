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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.trees.plans.commands.AlterJobCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingInsertJobSchemaPersistenceTest {
    private final List<byte[]> journals = new ArrayList<>();
    private final List<Writable> payloads = new ArrayList<>();
    private MockedStatic<Env> envMock;
    private boolean oldBatchLog;
    private boolean failNextWrite;

    @BeforeEach
    public void setUp() throws Exception {
        oldBatchLog = Config.enable_batch_editlog;
        Config.enable_batch_editlog = true;
        envMock = Mockito.mockStatic(Env.class);
        Env env = Mockito.mock(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        Mockito.when(catalog.getDbOrAnalysisException("schema_persistence_test"))
                .thenReturn(new Database(9002L, "schema_persistence_test"));
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
        Mockito.when(transactions.getCallbackFactory()).thenReturn(new TxnStateCallbackFactory());

        EditLog editLog = Mockito.mock(EditLog.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(1).when(editLog).getNumEditStreams();
        Deencapsulation.setField(editLog, "numTransactions", new AtomicLong());
        Deencapsulation.setField(editLog, "totalTimeTransactions", new AtomicLong());
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Answer<Long> capture = invocation -> {
            short op = invocation.getArgument(0);
            Writable payload = invocation.getArgument(1);
            if (failNextWrite) {
                failNextWrite = false;
                throw new IllegalStateException("injected journal failure");
            }
            // Exercise serialization on a different thread while commitOffset still holds the job lock.
            byte[] bytes = CompletableFuture.supplyAsync(() -> encodeJournal(op, payload)).get(10, TimeUnit.SECONDS);
            journals.add(bytes);
            payloads.add(payload);
            return (long) journals.size();
        };
        Mockito.doAnswer(capture).when(editLog).logEditWithQueue(Mockito.anyShort(), Mockito.any(Writable.class));
        Journal journal = Mockito.mock(Journal.class);
        Mockito.doAnswer(capture).when(journal).write(Mockito.anyShort(), Mockito.any(Writable.class));
        Deencapsulation.setField(editLog, "journal", journal);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_batch_editlog = oldBatchLog;
        envMock.close();
    }

    @Test
    public void testClientSchemaUpdatesAreWrittenAndNullIsOmitted() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        job.commitOffset(request("200", null));
        job.commitOffset(request("300", "schema-A"));
        job.commitOffset(request("400", "schema-B"));
        job.commitOffset(request("500", null));

        Assertions.assertEquals("schema-A", loggedSchema(0));
        Assertions.assertNull(loggedSchema(1));
        Assertions.assertEquals("schema-A", loggedSchema(2));
        Assertions.assertEquals("schema-B", loggedSchema(3));
        Assertions.assertNull(loggedSchema(4));
        Assertions.assertEquals("schema-B", ((JdbcSourceOffsetProvider) job.offsetProvider).getTableSchemas());
        Assertions.assertArrayEquals(journals.get(0),
                encodeJournal(OperationType.OP_UPDATE_SCHEDULER_JOB, payloads.get(0)));
    }

    @Test
    public void testFullJournalEncodingMatchesJobWriter() throws Exception {
        StreamingInsertJob job = newJob();
        ((JdbcSourceOffsetProvider) job.offsetProvider).setTableSchemas("schema-A");
        job.setOffsetProviderPersist(job.offsetProvider.getPersistInfo());
        byte[] expected = encodeJournal(OperationType.OP_UPDATE_SCHEDULER_JOB, job);

        job.logUpdateOperation(job.getOffsetProviderPersist());

        Assertions.assertArrayEquals(expected, journals.get(0));
    }

    @Test
    public void testCheckpointAndFailoverRetainSchemaWithoutScheduling() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        job.commitOffset(request("200", null));
        job.commitOffset(request("300", "schema-B"));
        job.commitOffset(request("400", null));
        JobManager<StreamingInsertJob, Map<Object, Object>> manager = new JobManager<>();
        Map<Long, StreamingInsertJob> jobs = Deencapsulation.getField(manager, "jobMap");
        jobs.put(9001L, newJob());
        for (byte[] journal : journals) {
            manager.replayUpdateJob(readJournal(journal));
        }

        ByteArrayOutputStream image = new ByteArrayOutputStream();
        manager.write(new DataOutputStream(image));
        JobManager<StreamingInsertJob, Map<Object, Object>> restored = new JobManager<>();
        restored.readFields(new DataInputStream(new ByteArrayInputStream(image.toByteArray())));
        StreamingInsertJob recovered = restored.getJob(9001L);
        assertState(recovered, "schema-B", "400");

        job.commitOffset(request("500", null));
        restored.replayUpdateJob(readJournal(journals.get(4)));
        assertState(recovered, "schema-B", "500");
        recovered.replayOffsetProviderIfNeed();
        Assertions.assertEquals("schema-B", ((JdbcSourceOffsetProvider) recovered.offsetProvider).getTableSchemas());
        assertState(recovered, "schema-B", "500");
        Deencapsulation.setField(recovered, "runningStreamTask", new NoopTask());
        recovered.commitOffset(request("600", null));
        Assertions.assertNull(loggedSchema(5));
    }

    @Test
    public void testFailureRetainsSchemaForSameSchemaAndNullRetries() throws Exception {
        StreamingInsertJob job = newJob();
        for (String retrySchema : new String[] {"schema-A", null}) {
            failNextWrite = true;
            String schema = retrySchema == null ? "schema-B" : "schema-A";
            Assertions.assertThrows(IllegalStateException.class, () -> job.commitOffset(request("100", schema)));
            job.commitOffset(request("200", retrySchema));
            Assertions.assertEquals(schema, loggedSchema(journals.size() - 1));
            job.commitOffset(request("300", null));
            Assertions.assertNull(loggedSchema(journals.size() - 1));
        }
    }

    @Test
    public void testSchemaChangeBypassesSnapshotThrottle() throws Exception {
        StreamingInsertJob job = newJob();
        JdbcSourceOffsetProvider provider = (JdbcSourceOffsetProvider) job.offsetProvider;
        for (int i = 0; i < 3; i++) {
            String splitId = "table:" + i;
            provider.getRemainingSplits().add(new SnapshotSplit(splitId, "db.table",
                    Collections.singletonList("id"), new Object[] {1L}, new Object[] {2L}, null));
            CommitOffsetRequest request = request("100", i == 1 ? "schema-A" : null);
            request.setOffset("[{\"splitId\":\"" + splitId + "\",\"lsn\":\"100\"}]");
            job.commitOffset(request);
        }
        Assertions.assertEquals(2, journals.size());
        Assertions.assertEquals("schema-A", loggedSchema(1));
    }

    @Test
    public void testOrdinaryJournalSizeDoesNotGrowWithSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "x".repeat(1024)));
        job.commitOffset(request("200", null));
        int smallJournalSize = journals.get(1).length;
        job.commitOffset(request("300", "x".repeat(1024 * 1024)));
        job.commitOffset(request("400", null));
        Assertions.assertTrue(journals.get(2).length > 1024 * 1024);
        Assertions.assertTrue(journals.get(3).length < 10_000);
        Assertions.assertTrue(Math.abs(smallJournalSize - journals.get(3).length) < 100);
    }

    @Test
    public void testFullJobStateSurvivesCompactOffsetJournal() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));

        job.commitOffset(request("200", null));
        StreamingTaskTxnCommitAttachment attachment = new StreamingTaskTxnCommitAttachment(
                9001L, 1001L, 0, 0, 0, 0, 0, request("300", null).getOffset());
        Deencapsulation.invoke(job, "updateCloudJobStatisticAndOffset", attachment, true);
        job.logUpdateOperation();

        Assertions.assertNull(loggedSchema(1));
        Assertions.assertEquals("schema-A", loggedSchema(2));
        assertState(job, "schema-A", "300");
        assertState(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), StreamingInsertJob.class),
                "schema-A", "300");
    }

    @Test
    public void testCloudOffsetRecoveryBeforeProviderRestoreKeepsSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        StreamingInsertJob recovered = readJournal(journals.get(0));
        Assertions.assertNull(((JdbcSourceOffsetProvider) recovered.offsetProvider).getTableSchemas());

        StreamingTaskTxnCommitAttachment attachment = new StreamingTaskTxnCommitAttachment(
                9001L, 1001L, 0, 0, 0, 0, 0, request("200", null).getOffset());
        Deencapsulation.invoke(recovered, "updateCloudJobStatisticAndOffset", attachment, true);
        assertState(recovered, "schema-A", "200");
        recovered.replayOffsetProviderIfNeed();
        Assertions.assertEquals("schema-A", ((JdbcSourceOffsetProvider) recovered.offsetProvider).getTableSchemas());
    }

    @Test
    public void testDirectJournalOmitsUnchangedSchema() throws Exception {
        Config.enable_batch_editlog = false;
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        job.commitOffset(request("200", null));
        Assertions.assertEquals("schema-A", loggedSchema(0));
        Assertions.assertNull(loggedSchema(1));
        JsonObject persisted = GsonUtils.GSON.fromJson(loggedJob(1).get("opp").getAsString(), JsonObject.class);
        Assertions.assertFalse(persisted.has("ts"));
        StreamingInsertJob recovered = readJournal(journals.get(0));
        recovered.replayOnUpdated(readJournal(journals.get(1)));
        assertState(recovered, "schema-A", "200");
    }

    @Test
    public void testRecordWithoutSchemaRetainsPreviousSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        StreamingInsertJob recovered = readJournal(journals.get(0));
        newJob().commitOffset(request("200", null));
        recovered.replayOnUpdated(readJournal(journals.get(1)));
        assertState(recovered, "schema-A", "200");
    }

    @Test
    public void testFullImageAndAlterOffsetKeepSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        job.commitOffset(request("200", null));
        job.setJobStatus(JobStatus.PAUSED);
        job.alterJob(new AlterJobCommand(job.getJobName(),
                Map.of(StreamingJobProperties.OFFSET_PROPERTY, "{\"lsn\":\"300\"}"),
                null, null, null, Collections.emptyMap(), Collections.emptyMap()));
        ByteArrayOutputStream image = new ByteArrayOutputStream();
        job.write(new DataOutputStream(image));
        StreamingInsertJob restored = (StreamingInsertJob) AbstractJob.readFields(
                new DataInputStream(new ByteArrayInputStream(image.toByteArray())));
        assertState(restored, "schema-A", "300");
        StreamingInsertJob follower = readJournal(journals.get(0));
        follower.replayOnUpdated(readJournal(journals.get(2)));
        assertState(follower, "schema-A", "300");
    }

    @Test
    public void testAlterPausedJobAfterImageRecoveryKeepsSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        job.setJobStatus(JobStatus.PAUSED);
        ByteArrayOutputStream image = new ByteArrayOutputStream();
        job.write(new DataOutputStream(image));
        StreamingInsertJob recovered = (StreamingInsertJob) AbstractJob.readFields(
                new DataInputStream(new ByteArrayInputStream(image.toByteArray())));

        recovered.alterJob(new AlterJobCommand(recovered.getJobName(),
                Map.of(StreamingJobProperties.OFFSET_PROPERTY, "{\"lsn\":\"200\"}"),
                null, null, null, Collections.emptyMap(), Collections.emptyMap()));
        assertState(recovered, "schema-A", "200");
        image.reset();
        recovered.write(new DataOutputStream(image));
        assertState((StreamingInsertJob) AbstractJob.readFields(
                new DataInputStream(new ByteArrayInputStream(image.toByteArray()))), "schema-A", "200");

        recovered.setJobStatus(JobStatus.RUNNING);
        recovered.replayOffsetProviderIfNeed();
        Assertions.assertEquals("schema-A", ((JdbcSourceOffsetProvider) recovered.offsetProvider).getTableSchemas());
        Deencapsulation.setField(recovered, "runningStreamTask", new NoopTask());
        recovered.commitOffset(request("300", null));
        assertState(recovered, "schema-A", "300");
    }

    @Test
    public void testLegacyOffsetJournalRetainsPreviousSchema() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        StreamingInsertJob recovered = readJournal(journals.get(0));
        StreamingInsertJob legacyUpdate = newJob();
        legacyUpdate.setJobStatus(JobStatus.PAUSED);
        legacyUpdate.getProperties().put(StreamingJobProperties.OFFSET_PROPERTY, "{\"lsn\":\"200\"}");

        recovered.replayOnUpdated(legacyUpdate);

        assertState(recovered, "schema-A", "200");
    }

    @Test
    public void testStatusJournalAfterRecoveryRetainsSchemaWithoutScheduling() throws Exception {
        StreamingInsertJob job = newJob();
        job.commitOffset(request("100", "schema-A"));
        StreamingInsertJob recovered = readJournal(journals.get(0));
        recovered.setJobStatus(JobStatus.PAUSED);
        recovered.logUpdateOperation();
        Assertions.assertEquals("schema-A", loggedSchema(1));

        StreamingInsertJob follower = readJournal(journals.get(0));
        follower.replayOnUpdated(readJournal(journals.get(1)));
        assertState(follower, "schema-A", "100");
        ByteArrayOutputStream image = new ByteArrayOutputStream();
        follower.write(new DataOutputStream(image));
        assertState((StreamingInsertJob) AbstractJob.readFields(
                new DataInputStream(new ByteArrayInputStream(image.toByteArray()))), "schema-A", "100");
    }

    private static StreamingInsertJob newJob() {
        StreamingInsertJob job = new StreamingInsertJob();
        job.setJobId(9001L);
        job.setJobName("schema_persistence_test");
        job.setCurrentDbName("schema_persistence_test");
        job.setJobStatus(JobStatus.RUNNING);
        Deencapsulation.setField(job, "dataSourceType", DataSourceType.POSTGRES);
        Deencapsulation.setField(job, "properties", new HashMap<String, String>());
        Map<String, String> sourceProperties = new HashMap<>(
                Map.of(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://127.0.0.1:5432/source_db"));
        Deencapsulation.setField(job, "sourceProperties", sourceProperties);
        Deencapsulation.setField(job, "targetProperties", new HashMap<String, String>());
        Deencapsulation.setField(job, "runningStreamTask", new NoopTask());
        job.offsetProvider = new JdbcSourceOffsetProvider(9001L, DataSourceType.POSTGRES, sourceProperties);
        return job;
    }

    private static CommitOffsetRequest request(String lsn, String schema) {
        CommitOffsetRequest request = new CommitOffsetRequest();
        request.setTaskId(1001L);
        request.setOffset("[{\"splitId\":\"binlog-split\",\"lsn\":\"" + lsn + "\"}]");
        request.setTableSchemas(schema);
        return request;
    }

    private static byte[] encodeJournal(short op, Writable payload) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeShort(op);
            payload.write(out);
            return bytes.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static StreamingInsertJob readJournal(byte[] bytes) throws Exception {
        JournalEntity entity = new JournalEntity();
        entity.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
        Assertions.assertEquals(OperationType.OP_UPDATE_SCHEDULER_JOB, entity.getOpCode());
        return (StreamingInsertJob) entity.getData();
    }

    private JsonObject loggedJob(int index) throws Exception {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(journals.get(index)));
        Assertions.assertEquals(OperationType.OP_UPDATE_SCHEDULER_JOB, in.readShort());
        return GsonUtils.GSON.fromJson(Text.readString(in), JsonObject.class);
    }

    private String loggedSchema(int index) throws Exception {
        JsonObject offset = GsonUtils.GSON.fromJson(loggedJob(index).get("opp").getAsString(), JsonObject.class);
        return offset.has("ts") ? offset.get("ts").getAsString() : null;
    }

    private static void assertState(StreamingInsertJob job, String schema, String lsn) {
        JdbcSourceOffsetProvider state = GsonUtils.GSON.fromJson(
                job.getOffsetProviderPersist(), JdbcSourceOffsetProvider.class);
        Assertions.assertEquals(schema, state.getTableSchemas());
        Assertions.assertEquals(lsn, state.getBinlogOffsetPersist().get("lsn"));
    }

    private static class NoopTask extends StreamingMultiTblTask {
        NoopTask() {
            super(9001L, 1001L, null, null, null, null, null,
                    new StreamingJobProperties(new HashMap<>()), null, null);
            Deencapsulation.setField(this, "status", TaskStatus.RUNNING);
        }

        @Override
        public void successCallback(CommitOffsetRequest offsetRequest) {
        }
    }
}
