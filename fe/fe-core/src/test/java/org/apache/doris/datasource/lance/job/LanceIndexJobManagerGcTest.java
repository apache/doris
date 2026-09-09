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
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Retention-GC coverage for {@link LanceIndexJobManager#removeResolvedJobsOlderThan}
 * and {@link LanceIndexJobManager#replayRemoveJob}, with both edit-log seams captured
 * in memory. The pinned invariants: only resolved records are removed (a
 * force-released UNKNOWN and a refresh-DONE terminal job alike), an unresolved record
 * is never removed no matter its age (fail-closed, including corrupt identity-less
 * records); the retention clock is the durable update time, which the force release
 * bumps onto the force time; one clean round writes exactly one batch removal record
 * and removes oldest first, capped per round; a replayed removal is idempotent,
 * tolerates unknown ids, and lifts the corrupt admission blocker together with the
 * job; and a removed job never appears in a later image.
 */
public class LanceIndexJobManagerGcTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;

    @Test
    public void expiredResolvedJobsAreRemovedThroughOneBatchRecord() throws DdlException {
        TestManager manager = new TestManager();
        // Family (a): a force-released UNKNOWN job.
        createRunAndLoseResult(manager, 1L, "IdxForce");
        Assertions.assertTrue(manager.forceRelease(1L, 2L, "admin", "note", "warning"));
        // Family (b): a terminal job whose refresh ran to DONE.
        manager.createJob(newCreateJob(2L, "IdxDone"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(2L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertTrue(manager.markRefreshRunning(2L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(2L, 3L));
        // A live unresolved job shares the round and must survive it.
        manager.createJob(newCreateJob(3L, "IdxPending"), 100, 100, 100);

        // The force release bumps the durable update time onto the force time: that is
        // the retention clock base, so a forced job ages from its release, not earlier.
        LanceIndexJob forced = manager.getJob(1L);
        Assertions.assertEquals(forced.getForceTimeMs().longValue(), forced.getUpdateTimeMs());

        // A negative keep window expires every resolved record regardless of its age.
        List<Long> removed = manager.removeResolvedJobsOlderThan(-1L, 1024);
        Assertions.assertEquals(2, removed.size());
        Assertions.assertTrue(removed.containsAll(Arrays.asList(1L, 2L)));

        // The whole round is durable through exactly one batch removal record.
        Assertions.assertEquals(1, manager.removeLog.size());
        Assertions.assertEquals(removed, manager.removeLog.get(0));

        // Memory converges with the journal: the removed jobs are gone, the survivor
        // keeps its fence and quota charge.
        Assertions.assertNull(manager.getJob(1L));
        Assertions.assertNull(manager.getJob(2L));
        Assertions.assertNotNull(manager.getJob(3L));
        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(3L).fenceKey()));

        // A follow-up round with nothing expired writes nothing.
        Assertions.assertTrue(manager.removeResolvedJobsOlderThan(-1L, 1024).isEmpty());
        Assertions.assertEquals(1, manager.removeLog.size());
    }

    @Test
    public void unexpiredResolvedJobsSurvive() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxForce");
        Assertions.assertTrue(manager.forceRelease(1L, 2L, "admin", "note", "warning"));

        // A generous keep window (the production default of seven days) retains the
        // just-resolved record and writes no journal.
        Assertions.assertTrue(manager.removeResolvedJobsOlderThan(7 * 24 * 3600 * 1000L, 1024).isEmpty());
        Assertions.assertTrue(manager.removeLog.isEmpty());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertNotNull(stored);
        Assertions.assertTrue(stored.isForceReleased());
    }

    @Test
    public void unresolvedJobsAreNeverRemovedRegardlessOfAge() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxPending"), 100, 100, 100);
        manager.createJob(newCreateJob(2L, "IdxRunning"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        createRunAndLoseResult(manager, 3L, "IdxUnknown");
        // A terminal job whose refresh is still REQUIRED is unresolved too.
        manager.createJob(newCreateJob(4L, "IdxCommitted"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(4L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(4L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        // A corrupt identity-less record is unresolved by construction.
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"UNKNOWN\"}", LanceIndexJob.class));
        Assertions.assertEquals(Collections.singletonList(5L), manager.getCorruptUnresolvedJobIds());

        // Even with a negative keep window, nothing unresolved is ever collected.
        Assertions.assertTrue(manager.removeResolvedJobsOlderThan(-1L, 1024).isEmpty());
        Assertions.assertTrue(manager.removeLog.isEmpty());
        Assertions.assertEquals(5, manager.getJobCount());
        Assertions.assertEquals(Collections.singletonList(5L), manager.getCorruptUnresolvedJobIds());
        Assertions.assertEquals(4L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void retentionAgesFromTheDurableUpdateTime() {
        TestManager manager = new TestManager();
        // Pinned timestamps on replayed resolved records: job 1 aged out at the epoch,
        // job 2 shares the ancient create time but was updated just now.
        manager.replayUpsertJob(resolvedRecordWithTimes(1L, 0L, 0L));
        manager.replayUpsertJob(resolvedRecordWithTimes(2L, 0L, System.currentTimeMillis()));

        List<Long> removed = manager.removeResolvedJobsOlderThan(60_000L, 1024);

        // Job 2 survives although its create time is as old: the clock base is the
        // durable update time, bumped by every durable transition.
        Assertions.assertEquals(Collections.singletonList(1L), removed);
        Assertions.assertNull(manager.getJob(1L));
        Assertions.assertNotNull(manager.getJob(2L));
    }

    @Test
    public void removeRoundsAreCappedOldestFirst() {
        TestManager manager = new TestManager();
        // Distinct ages plus an update-time tie, replayed out of order on purpose.
        manager.replayUpsertJob(resolvedRecordWithTimes(9L, 0L, 100L));
        manager.replayUpsertJob(resolvedRecordWithTimes(4L, 0L, 100L));
        manager.replayUpsertJob(resolvedRecordWithTimes(7L, 0L, 300L));

        // Oldest first; the update-time tie breaks by the smaller job id.
        Assertions.assertEquals(Collections.singletonList(4L), manager.removeResolvedJobsOlderThan(50L, 1));
        Assertions.assertEquals(Collections.singletonList(9L), manager.removeResolvedJobsOlderThan(50L, 1));
        Assertions.assertEquals(Collections.singletonList(7L), manager.removeResolvedJobsOlderThan(50L, 10));
        Assertions.assertTrue(manager.removeResolvedJobsOlderThan(50L, 10).isEmpty());

        // Each non-empty round wrote exactly one batch record; memory is empty.
        Assertions.assertEquals(3, manager.removeLog.size());
        Assertions.assertEquals(0, manager.getJobCount());
    }

    @Test
    public void replayRemoveJobIsIdempotentTolerantAndLiftsTheCorruptBlocker() {
        TestManager manager = new TestManager();
        // Tolerant of null, empty, and ids that were never present.
        manager.replayRemoveJob(null);
        manager.replayRemoveJob(Collections.emptyList());
        manager.replayRemoveJob(Collections.singletonList(404L));
        Assertions.assertEquals(0, manager.getJobCount());

        // The corrupt admission blocker leaves together with its job record.
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"UNKNOWN\"}", LanceIndexJob.class));
        Assertions.assertEquals(Collections.singletonList(5L), manager.getCorruptUnresolvedJobIds());
        manager.replayRemoveJob(Arrays.asList(5L, 404L));
        Assertions.assertNull(manager.getJob(5L));
        Assertions.assertTrue(manager.getCorruptUnresolvedJobIds().isEmpty());

        // Replaying the same removal again is a no-op.
        manager.replayRemoveJob(Collections.singletonList(5L));
        Assertions.assertEquals(0, manager.getJobCount());
        // Replay itself never writes the journal.
        Assertions.assertTrue(manager.editLog.isEmpty());
        Assertions.assertTrue(manager.removeLog.isEmpty());
    }

    @Test
    public void imageRoundtripDropsRemovedJobs() throws Exception {
        TestManager source = new TestManager();
        createRunAndLoseResult(source, 1L, "IdxGone");
        Assertions.assertTrue(source.forceRelease(1L, 2L, "admin", "note", "warning"));
        source.createJob(newCreateJob(2L, "IdxStay"), 100, 100, 100);
        Assertions.assertEquals(Collections.singletonList(1L), source.removeResolvedJobsOlderThan(-1L, 1024));

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        source.write(new DataOutputStream(byteStream));
        LanceIndexJobManager loaded =
                LanceIndexJobManager.read(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

        // The removed record is absent from the image; the unresolved survivor keeps
        // its fence and quota through the gsonPostProcess rebuild.
        Assertions.assertEquals(1, loaded.getJobCount());
        Assertions.assertNull(loaded.getJob(1L));
        Assertions.assertNotNull(loaded.getJob(2L));
        Assertions.assertEquals(1L, loaded.getQuota().getGlobalCount());
        Assertions.assertTrue(loaded.isFenceHeld(loaded.getJob(2L).fenceKey()));
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static LanceIndexJobResult result(LanceIndexJobResultCode code) {
        return new LanceIndexJobResult(code, LanceIndexJobCompletionReason.NONE, "sanitized message", false);
    }

    /**
     * Drives a job to a durable UNKNOWN at revision 2 through the lifecycle channel:
     * create (rev0) + dispatch (rev1) + an ambiguous result (rev2), fence/quota held.
     */
    private static void createRunAndLoseResult(TestManager manager, long jobId, String displayName)
            throws DdlException {
        manager.createJob(newCreateJob(jobId, displayName), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(jobId, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NO_TRUSTED_RESULT)));
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, manager.getJob(jobId).getMutationState());
    }

    /**
     * Builds a resolved record (COMMITTED with refresh DONE) carrying explicit durable
     * timestamps, as replay input for the retention predicate.
     */
    private static LanceIndexJob resolvedRecordWithTimes(long jobId, long createTimeMs, long updateTimeMs) {
        String json = "{\"jid\":" + jobId + ",\"cr\":\"tester\",\"rev\":2,\"ctm\":" + createTimeMs
                + ",\"utm\":" + updateTimeMs + ",\"cid\":" + CATALOG_ID
                + ",\"dbn\":\"db1\",\"tbn\":\"tbl1\",\"ms\":\"COMMITTED\",\"rs\":\"DONE\"}";
        LanceIndexJob job = GsonUtils.GSON.fromJson(json, LanceIndexJob.class);
        Assertions.assertFalse(job.isUnresolved());
        return job;
    }

    /**
     * Edit-log seams: capture every durable record instead of writing the journal.
     */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();
        private final List<List<Long>> removeLog = new ArrayList<>();

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
        }

        @Override
        protected void writeRemoveLog(List<Long> jobIds) {
            removeLog.add(new ArrayList<>(jobIds));
        }
    }
}
