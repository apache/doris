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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Coverage for the two read-side queries added to {@link LanceIndexJobManager} for the
 * admission slice: {@link LanceIndexJobManager#getAllJobsSnapshot()} (the data source of
 * SHOW LANCE INDEX JOBS: every job, copies only, ordered by job id) and
 * {@link LanceIndexJobManager#hasUnresolvedJobsForCatalog(long)} (the catalog DDL guard
 * probe: any job still holding its fence and unresolved quota for the catalog).
 */
public class LanceIndexJobManagerQueryTest {
    private static final long CATALOG_ID = 10L;
    private static final long OTHER_CATALOG_ID = 20L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;

    @Test
    public void snapshotReturnsEveryJobOrderedByJobId() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(3L, "IdxC", CATALOG_ID), 100, 100, 100);
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        manager.createJob(newCreateJob(2L, "IdxB", OTHER_CATALOG_ID), 100, 100, 100);

        List<LanceIndexJob> snapshot = manager.getAllJobsSnapshot();
        Assertions.assertEquals(3, snapshot.size());
        Assertions.assertEquals(1L, snapshot.get(0).getJobId());
        Assertions.assertEquals(2L, snapshot.get(1).getJobId());
        Assertions.assertEquals(3L, snapshot.get(2).getJobId());
    }

    @Test
    public void snapshotCopiesAreIsolatedFromStoredRecords() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);

        LanceIndexJob copy = manager.getAllJobsSnapshot().get(0);
        copy.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        copy.setDisplayIndexName("mutated");
        copy.setRevision(99L);

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals("IdxA", stored.getDisplayIndexName());
        Assertions.assertEquals(0L, stored.getRevision());

        // A later snapshot is unaffected by mutations of an earlier one.
        LanceIndexJob fresh = manager.getAllJobsSnapshot().get(0);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, fresh.getMutationState());
        Assertions.assertEquals("IdxA", fresh.getDisplayIndexName());
        Assertions.assertEquals(0L, fresh.getRevision());
    }

    @Test
    public void hasUnresolvedJobsForCatalogScopesByCatalogId() throws Exception {
        TestManager manager = new TestManager();
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(OTHER_CATALOG_ID));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(999L));
    }

    @Test
    public void releasedTerminalJobDoesNotCountAsUnresolved() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, "ok", false)));
        // COMMITTED with refresh REQUIRED still holds the fence and quota.
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        // A released terminal job remains visible in the full snapshot.
        Assertions.assertEquals(1, manager.getAllJobsSnapshot().size());
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED,
                manager.getAllJobsSnapshot().get(0).getMutationState());
    }

    @Test
    public void unresolvedJobInAnotherCatalogDoesNotLeak() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", OTHER_CATALOG_ID), 100, 100, 100);
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(OTHER_CATALOG_ID));
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName, long catalogId) {
        return new LanceIndexJob(jobId, "tester", catalogId, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static class TestManager extends LanceIndexJobManager {
        @Override
        protected void writeEditLog(LanceIndexJob job) {
            // No journal in a pure query unit test.
        }
    }
}
