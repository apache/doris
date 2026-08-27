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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.common.DdlException;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Persistence coverage for the Lance index job infrastructure: the manager image
 * write/read round trip (including the fence-index and quota rebuild in
 * gsonPostProcess), the journal mounting point for op code 500
 * ({@link OperationType#OP_LANCE_INDEX_JOB_UPSERT}) through
 * {@link JournalEntity#write}/{@link JournalEntity#readFields}, the single-record Gson
 * round trip field by field, and the bounded-text rejection at construction time.
 */
public class LanceIndexJobManagerPersistTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";

    @Test
    public void managerImageRoundtripRebuildsDerivedFenceAndQuota() throws Exception {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(1L, "IdxPending"), 100, 100, 100);
        source.createJob(newCreateJob(2L, "IdxRunning"), 100, 100, 100);
        source.markRunning(2L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, 9999L);
        source.createJob(newCreateJob(3L, "IdxCommitted"), 100, 100, 100);
        source.markRunning(3L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, 9999L);
        source.completeWithResult(3L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, "ok", false));
        source.createJob(newCreateJob(4L, "IdxUnknown"), 100, 100, 100);
        source.markRunning(4L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, 9999L);
        source.completeWithResult(4L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, "lost", false));
        source.replayUpsertJob(forceReleasedUnknownJob(5L, "idxforced"));

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        source.write(new DataOutputStream(byteStream));
        LanceIndexJobManager loaded =
                LanceIndexJobManager.read(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

        Assertions.assertEquals(source.getJobCount(), loaded.getJobCount());
        for (long jobId = 1L; jobId <= 5L; jobId++) {
            Assertions.assertEquals(GsonUtils.GSON.toJson(source.getJob(jobId)),
                    GsonUtils.GSON.toJson(loaded.getJob(jobId)), "job " + jobId);
        }

        // The derived fence index survived: unresolved jobs still fence their names.
        for (long jobId = 1L; jobId <= 4L; jobId++) {
            Assertions.assertTrue(loaded.isFenceHeld(loaded.getJob(jobId).fenceKey()), "fence of job " + jobId);
        }
        Assertions.assertFalse(loaded.isFenceHeld(loaded.getJob(5L).fenceKey()));
        Assertions.assertEquals(4L, loaded.getQuota().getGlobalCount());
        Assertions.assertEquals(4L, loaded.getQuota().getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(4, loaded.getUnresolvedJobs().size());
        Assertions.assertEquals(1, loaded.getJobsNeedingRefresh().size());

        // The fence is enforceable after the image load: a conflict is rejected by the fence
        // CAS, which precedes any journal write.
        Assertions.assertThrows(DdlException.class,
                () -> loaded.createJob(newCreateJob(9L, "idxunknown"), 100, 100, 100));

        // And a FORCE-released name is free again once the loaded image is rebuilt through replay.
        TestManager rebuilt = new TestManager();
        for (long jobId = 1L; jobId <= 5L; jobId++) {
            rebuilt.replayUpsertJob(loaded.getJob(jobId));
        }
        rebuilt.createJob(newCreateJob(9L, "IdxForced"), 100, 100, 100);
        Assertions.assertEquals(5L, rebuilt.getQuota().getGlobalCount());
    }

    @Test
    public void emptyManagerRoundtripStaysEmpty() throws Exception {
        TestManager source = new TestManager();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        source.write(new DataOutputStream(byteStream));
        LanceIndexJobManager loaded =
                LanceIndexJobManager.read(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

        Assertions.assertEquals(0, loaded.getJobCount());
        Assertions.assertEquals(0L, loaded.getQuota().getGlobalCount());
        Assertions.assertTrue(loaded.getUnresolvedJobs().isEmpty());
    }

    @Test
    public void imageLoadKeepsTheSmallerJobIdOnAFenceCollision() {
        // Only a corrupt image can hold two unresolved jobs on the same fence key;
        // gsonPostProcess keeps the smaller job id (Gson auto-runs it after fromJson).
        String jobJson = "\"jid\":%d,\"rev\":0,\"cid\":" + CATALOG_ID
                + ",\"prv\":\"" + LanceIndexFenceKey.PROVIDER_DIRECTORY + "\",\"loc\":\"" + LOCATOR
                + "\",\"din\":\"IdxA\",\"nin\":\"idxa\",\"ms\":\"PENDING\",\"rs\":\"NOT_REQUIRED\"";
        String json = "{\"jobs\":{\"2\":{" + String.format(jobJson, 2L) + "},"
                + "\"1\":{" + String.format(jobJson, 1L) + "}}}";
        LanceIndexJobManager loaded = GsonUtils.GSON.fromJson(json, LanceIndexJobManager.class);

        Assertions.assertNotNull(loaded.getJob(1L));
        Assertions.assertTrue(loaded.isFenceHeld(loaded.getJob(1L).fenceKey()));
        // Both unresolved jobs still charge the quota.
        Assertions.assertEquals(2L, loaded.getQuota().getGlobalCount());
        // The fence conflict message names the surviving smaller job id.
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> loaded.createJob(newCreateJob(9L, "idxa"), 100, 100, 100));
        Assertions.assertTrue(exception.getMessage().contains("unresolved job 1"));
    }

    @Test
    public void schemaContractEqualityIsOrderSensitive() {
        LanceIndexSchemaContract ordered = contract();
        LanceIndexSchemaContract sameOrder = contract();
        List<LanceIndexSchemaContract.IndexedField> reversed = new ArrayList<>();
        reversed.add(ordered.getFields().get(1));
        reversed.add(ordered.getFields().get(0));
        LanceIndexSchemaContract reordered = new LanceIndexSchemaContract(reversed);

        Assertions.assertEquals(ordered, sameOrder);
        Assertions.assertEquals(ordered.hashCode(), sameOrder.hashCode());
        // The same fields in a different order are a different contract.
        Assertions.assertNotEquals(ordered, reordered);
    }

    @Test
    public void journalEntityRoundtripUsesOpCode500() throws Exception {
        LanceIndexJob job = fullyPopulatedJob();

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteStream);
        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setData(job);
        journalEntity.setOpCode(OperationType.OP_LANCE_INDEX_JOB_UPSERT);
        journalEntity.write(output);
        output.flush();

        JournalEntity replayed = new JournalEntity();
        replayed.readFields(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

        Assertions.assertEquals(OperationType.OP_LANCE_INDEX_JOB_UPSERT, replayed.getOpCode());
        Assertions.assertEquals(500, replayed.getOpCode());
        Assertions.assertTrue(replayed.getData() instanceof LanceIndexJob);
        Assertions.assertEquals(GsonUtils.GSON.toJson(job), GsonUtils.GSON.toJson(replayed.getData()));
    }

    @Test
    public void journalEntityStreamPreservesRecordOrder() throws Exception {
        List<LanceIndexJob> jobs = new ArrayList<>();
        jobs.add(fullyPopulatedJob());
        LanceIndexJob second = newCreateJob(77L, "IdxSecond");
        second.setMutationState(LanceIndexJobMutationState.PENDING);
        jobs.add(second);

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteStream);
        for (LanceIndexJob job : jobs) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(job);
            journalEntity.setOpCode(OperationType.OP_LANCE_INDEX_JOB_UPSERT);
            journalEntity.write(output);
        }
        output.flush();

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        for (LanceIndexJob expected : jobs) {
            JournalEntity replayed = new JournalEntity();
            replayed.readFields(input);
            Assertions.assertEquals(OperationType.OP_LANCE_INDEX_JOB_UPSERT, replayed.getOpCode());
            Assertions.assertEquals(GsonUtils.GSON.toJson(expected), GsonUtils.GSON.toJson(replayed.getData()));
        }
        Assertions.assertEquals(0, input.available());
    }

    @Test
    public void jobStreamRoundtripPreservesAllFields() throws Exception {
        LanceIndexJob job = fullyPopulatedJob();

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        job.write(new DataOutputStream(byteStream));
        LanceIndexJob restored =
                LanceIndexJob.read(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

        assertSameJobFields(job, restored);
    }

    @Test
    public void jobGsonRoundtripPreservesAllFields() {
        LanceIndexJob job = fullyPopulatedJob();
        LanceIndexJob restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), LanceIndexJob.class);
        assertSameJobFields(job, restored);
    }

    @Test
    public void boundedTextFieldsRejectOverflow() {
        String overMessage = StringUtils.repeat("m", LanceIndexJobResult.MAX_MESSAGE_BYTES + 1);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, overMessage, false));
        // Multibyte characters count as UTF-8 bytes, not chars.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, StringUtils.repeat("é", 513), false));
        new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK, LanceIndexJobCompletionReason.NONE,
                StringUtils.repeat("m", LanceIndexJobResult.MAX_MESSAGE_BYTES), false);

        LanceIndexJob job = newCreateJob(1L, "IdxA");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> job.setPropertiesJson(StringUtils.repeat("p", LanceIndexJob.MAX_PROPERTIES_JSON_BYTES + 1)));
        job.setPropertiesJson(StringUtils.repeat("p", LanceIndexJob.MAX_PROPERTIES_JSON_BYTES));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> job.setForceNote(StringUtils.repeat("n", LanceIndexJob.MAX_FORCE_TEXT_BYTES + 1)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> job.setForceWarning(StringUtils.repeat("w", LanceIndexJob.MAX_FORCE_TEXT_BYTES + 1)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> job.setNormalizedIndexName(
                        StringUtils.repeat("n", LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES + 1)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> job.setDisplayIndexName(
                        StringUtils.repeat("d", LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES + 1)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> newCreateJob(2L, StringUtils.repeat("d", LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES + 1)));
    }

    @Test
    public void resultRejectsNullResultCode() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new LanceIndexJobResult(null, LanceIndexJobCompletionReason.NONE, "msg", false));
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static LanceIndexSchemaContract contract() {
        List<LanceIndexSchemaContract.IndexedField> fields = new ArrayList<>();
        fields.add(new LanceIndexSchemaContract.IndexedField(1L, "v", "fixed_size_list[float;192]",
                false, 192, "float", false));
        fields.add(new LanceIndexSchemaContract.IndexedField(2L, "s", "decimal(10,2)", true, null, null, null));
        return new LanceIndexSchemaContract(fields);
    }

    /**
     * A job with every durable field populated, including result, schema contract,
     * dispatch identity, and the FORCE audit fields.
     */
    private static LanceIndexJob fullyPopulatedJob() {
        LanceIndexJob job = new LanceIndexJob(42L, "creator", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR, "IdxΩ", "idxω",
                LanceIndexJobMutationType.REPLACE, true, false, "IVF_PQ", "v",
                "{\"num_partitions\":\"256\"}", 99L, contract());
        job.setRevision(7L);
        job.setCreateTimeMs(111L);
        job.setUpdateTimeMs(222L);
        job.setMutationState(LanceIndexJobMutationState.RUNNING);
        job.setRefreshState(LanceIndexJobRefreshState.RUNNING);
        job.setResult(new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_IO,
                LanceIndexJobCompletionReason.NONE, "io error", true));
        job.setBackendId(BACKEND_ID);
        job.setBeProcessEpoch(BE_EPOCH);
        job.setInvocationId(INVOCATION_ID);
        job.setDeadlineMs(123456L);
        job.setPossibleLiveOwned(true);
        job.setTerminationProof(LanceIndexTerminationProof.NONE);
        job.setForceActor("admin");
        job.setForceTimeMs(777L);
        job.setForceNote("note");
        job.setForceWarning("warning");
        return job;
    }

    private static LanceIndexJob forceReleasedUnknownJob(long jobId, String normalizedName) {
        String json = "{\"jid\":" + jobId + ",\"cr\":\"tester\",\"rev\":2,\"cid\":" + CATALOG_ID
                + ",\"dbn\":\"db1\",\"tbn\":\"tbl1\",\"prv\":\"" + LanceIndexFenceKey.PROVIDER_DIRECTORY
                + "\",\"loc\":\"" + LOCATOR + "\",\"din\":\"" + normalizedName + "\",\"nin\":\"" + normalizedName
                + "\",\"mt\":\"CREATE\",\"ms\":\"UNKNOWN\",\"rs\":\"NOT_REQUIRED\",\"fr\":true}";
        return GsonUtils.GSON.fromJson(json, LanceIndexJob.class);
    }

    private static void assertSameJobFields(LanceIndexJob expected, LanceIndexJob actual) {
        Assertions.assertEquals(expected.getJobId(), actual.getJobId());
        Assertions.assertEquals(expected.getCreator(), actual.getCreator());
        Assertions.assertEquals(expected.getRevision(), actual.getRevision());
        Assertions.assertEquals(expected.getCreateTimeMs(), actual.getCreateTimeMs());
        Assertions.assertEquals(expected.getUpdateTimeMs(), actual.getUpdateTimeMs());
        Assertions.assertEquals(expected.getCatalogId(), actual.getCatalogId());
        Assertions.assertEquals(expected.getDbName(), actual.getDbName());
        Assertions.assertEquals(expected.getTableName(), actual.getTableName());
        Assertions.assertEquals(expected.getProvider(), actual.getProvider());
        Assertions.assertEquals(expected.getNormalizedLocator(), actual.getNormalizedLocator());
        Assertions.assertEquals(expected.getDisplayIndexName(), actual.getDisplayIndexName());
        Assertions.assertEquals(expected.getNormalizedIndexName(), actual.getNormalizedIndexName());
        Assertions.assertEquals(expected.getMutationType(), actual.getMutationType());
        Assertions.assertEquals(expected.isIfNotExists(), actual.isIfNotExists());
        Assertions.assertEquals(expected.isIfExists(), actual.isIfExists());
        Assertions.assertEquals(expected.getIndexType(), actual.getIndexType());
        Assertions.assertEquals(expected.getColumnName(), actual.getColumnName());
        Assertions.assertEquals(expected.getPropertiesJson(), actual.getPropertiesJson());
        Assertions.assertEquals(expected.getAdmittedDatasetVersion(), actual.getAdmittedDatasetVersion());
        Assertions.assertEquals(expected.getSchemaContract(), actual.getSchemaContract());
        Assertions.assertEquals(expected.getMutationState(), actual.getMutationState());
        Assertions.assertEquals(expected.getRefreshState(), actual.getRefreshState());
        Assertions.assertEquals(expected.getResult().getResultCode(), actual.getResult().getResultCode());
        Assertions.assertEquals(expected.getResult().getCompletionReason(), actual.getResult().getCompletionReason());
        Assertions.assertEquals(expected.getResult().getSanitizedMessage(), actual.getResult().getSanitizedMessage());
        Assertions.assertEquals(expected.getResult().isExternalMetadataAdvanced(),
                actual.getResult().isExternalMetadataAdvanced());
        Assertions.assertEquals(expected.getBackendId(), actual.getBackendId());
        Assertions.assertEquals(expected.getBeProcessEpoch(), actual.getBeProcessEpoch());
        Assertions.assertEquals(expected.getInvocationId(), actual.getInvocationId());
        Assertions.assertEquals(expected.getDeadlineMs(), actual.getDeadlineMs());
        Assertions.assertEquals(expected.isPossibleLiveOwned(), actual.isPossibleLiveOwned());
        Assertions.assertEquals(expected.getTerminationProof(), actual.getTerminationProof());
        Assertions.assertEquals(expected.isForceReleased(), actual.isForceReleased());
        Assertions.assertEquals(expected.getForceActor(), actual.getForceActor());
        Assertions.assertEquals(expected.getForceTimeMs(), actual.getForceTimeMs());
        Assertions.assertEquals(expected.getForceNote(), actual.getForceNote());
        Assertions.assertEquals(expected.getForceWarning(), actual.getForceWarning());
        Assertions.assertEquals(expected.fenceKey(), actual.fenceKey());
        Assertions.assertEquals(expected.getTableQuotaKey(), actual.getTableQuotaKey());
    }

    /**
     * Edit-log seam: captures every durable record instead of writing the journal.
     */
    private static class TestManager extends LanceIndexJobManager {
        @Override
        protected void writeEditLog(LanceIndexJob job) {
            // No journal in a pure persistence unit test.
        }
    }
}
