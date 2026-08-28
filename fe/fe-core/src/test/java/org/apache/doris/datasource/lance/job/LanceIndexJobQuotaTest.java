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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit coverage for the three-level unresolved-job quota counters (table/locator,
 * catalog, global): the "current &lt; limit" boundary at each level, rejection of
 * non-positive limits, release recovery with underflow clamping, and rebuild
 * equivalence with live counting. The "which jobs count" semantics are
 * {@link LanceIndexJob#isUnresolved()}; the rebuild-side composition is pinned here too.
 */
public class LanceIndexJobQuotaTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR_A = "s3://bucket/table-a";
    private static final String LOCATOR_B = "s3://bucket/table-b";

    @Test
    public void tryAcquireChargesAllThreeLevels() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        LanceIndexJob job = newJob(1L, CATALOG_ID, LOCATOR_A);

        Assertions.assertTrue(quota.tryAcquire(job, 5, 5, 5));
        Assertions.assertEquals(1L, quota.getGlobalCount());
        Assertions.assertEquals(1L, quota.getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(1L, quota.getTableCount(job.getTableQuotaKey()));
    }

    @Test
    public void tableLimitRejectsTheNextJobExactlyAtLimit() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        Assertions.assertTrue(quota.tryAcquire(newJob(1L, CATALOG_ID, LOCATOR_A), 2, 100, 100));
        Assertions.assertTrue(quota.tryAcquire(newJob(2L, CATALOG_ID, LOCATOR_A), 2, 100, 100));

        LanceIndexJob third = newJob(3L, CATALOG_ID, LOCATOR_A);
        Assertions.assertFalse(quota.tryAcquire(third, 2, 100, 100));
        // A rejected acquire charges nothing at any level.
        Assertions.assertEquals(2L, quota.getGlobalCount());
        Assertions.assertEquals(2L, quota.getTableCount(third.getTableQuotaKey()));

        // The limit is per table/locator identity: another table still has room.
        Assertions.assertTrue(quota.tryAcquire(newJob(4L, CATALOG_ID, LOCATOR_B), 2, 100, 100));
    }

    @Test
    public void catalogLimitRejectsAcrossTables() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        Assertions.assertTrue(quota.tryAcquire(newJob(1L, CATALOG_ID, LOCATOR_A), 100, 2, 100));
        Assertions.assertTrue(quota.tryAcquire(newJob(2L, CATALOG_ID, LOCATOR_B), 100, 2, 100));
        Assertions.assertFalse(quota.tryAcquire(newJob(3L, CATALOG_ID, LOCATOR_A), 100, 2, 100));

        // Another catalog is a separate level.
        Assertions.assertTrue(quota.tryAcquire(newJob(4L, 20L, LOCATOR_A), 100, 2, 100));
    }

    @Test
    public void globalLimitRejectsAcrossCatalogs() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        Assertions.assertTrue(quota.tryAcquire(newJob(1L, CATALOG_ID, LOCATOR_A), 100, 100, 2));
        Assertions.assertTrue(quota.tryAcquire(newJob(2L, 20L, LOCATOR_A), 100, 100, 2));
        Assertions.assertFalse(quota.tryAcquire(newJob(3L, 30L, LOCATOR_B), 100, 100, 2));
        Assertions.assertEquals(2L, quota.getGlobalCount());
    }

    @Test
    public void nonPositiveLimitIsRejectedWithoutACharge() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        LanceIndexJob job = newJob(1L, CATALOG_ID, LOCATOR_A);
        Assertions.assertFalse(quota.tryAcquire(job, 0, 1, 1));
        Assertions.assertFalse(quota.tryAcquire(job, 1, 0, 1));
        Assertions.assertFalse(quota.tryAcquire(job, 1, 1, 0));
        Assertions.assertFalse(quota.tryAcquire(job, -1, 1, 1));
        Assertions.assertEquals(0L, quota.getGlobalCount());
    }

    @Test
    public void releaseRecoversCapacityAtAllLevels() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        LanceIndexJob first = newJob(1L, CATALOG_ID, LOCATOR_A);
        Assertions.assertTrue(quota.tryAcquire(first, 1, 1, 1));
        Assertions.assertFalse(quota.tryAcquire(newJob(2L, CATALOG_ID, LOCATOR_A), 1, 1, 1));

        quota.release(first);
        Assertions.assertEquals(0L, quota.getGlobalCount());
        Assertions.assertEquals(0L, quota.getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(0L, quota.getTableCount(first.getTableQuotaKey()));

        Assertions.assertTrue(quota.tryAcquire(newJob(3L, CATALOG_ID, LOCATOR_A), 1, 1, 1));
    }

    @Test
    public void releaseUnderflowClampsAtZero() {
        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        // Releasing a job that was never charged must not fail and must not go negative.
        quota.release(newJob(1L, CATALOG_ID, LOCATOR_A));
        Assertions.assertEquals(0L, quota.getGlobalCount());
        Assertions.assertEquals(0L, quota.getCatalogCount(CATALOG_ID));
        Assertions.assertTrue(quota.tryAcquire(newJob(2L, CATALOG_ID, LOCATOR_A), 1, 1, 1));
    }

    @Test
    public void rebuildMatchesIncrementalCounting() {
        List<LanceIndexJob> unresolved = new ArrayList<>();
        unresolved.add(newJob(1L, CATALOG_ID, LOCATOR_A));
        unresolved.add(newJob(2L, CATALOG_ID, LOCATOR_A));
        unresolved.add(newJob(3L, CATALOG_ID, LOCATOR_B));
        unresolved.add(newJob(4L, 20L, LOCATOR_A));

        LanceIndexJobQuota incremental = new LanceIndexJobQuota();
        for (LanceIndexJob job : unresolved) {
            Assertions.assertTrue(incremental.tryAcquire(job, 100, 100, 100));
        }
        LanceIndexJobQuota rebuilt = new LanceIndexJobQuota();
        rebuilt.rebuild(unresolved);

        Assertions.assertEquals(incremental.getGlobalCount(), rebuilt.getGlobalCount());
        Assertions.assertEquals(incremental.getCatalogCount(CATALOG_ID), rebuilt.getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(incremental.getCatalogCount(20L), rebuilt.getCatalogCount(20L));
        Assertions.assertEquals(incremental.getTableCount(unresolved.get(0).getTableQuotaKey()),
                rebuilt.getTableCount(unresolved.get(0).getTableQuotaKey()));
        Assertions.assertEquals(incremental.getTableCount(unresolved.get(2).getTableQuotaKey()),
                rebuilt.getTableCount(unresolved.get(2).getTableQuotaKey()));

        rebuilt.rebuild(Collections.<LanceIndexJob>emptyList());
        Assertions.assertEquals(0L, rebuilt.getGlobalCount());
        Assertions.assertEquals(0L, rebuilt.getCatalogCount(CATALOG_ID));
    }

    @Test
    public void rebuildCountsUnresolvedJobsOnly() {
        List<LanceIndexJob> jobs = new ArrayList<>();
        // Active and unforced-unknown jobs count.
        jobs.add(jobInState(1L, LanceIndexJobMutationState.PENDING, LanceIndexJobRefreshState.NOT_REQUIRED, false));
        jobs.add(jobInState(2L, LanceIndexJobMutationState.RUNNING, LanceIndexJobRefreshState.NOT_REQUIRED, false));
        jobs.add(jobInState(3L, LanceIndexJobMutationState.UNKNOWN, LanceIndexJobRefreshState.NOT_REQUIRED, false));
        jobs.add(jobInState(4L, LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.REQUIRED, false));
        jobs.add(jobInState(5L, LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.RUNNING, false));
        jobs.add(jobInState(6L, LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.FAILED, false));
        jobs.add(jobInState(7L, LanceIndexJobMutationState.NOT_COMMITTED, LanceIndexJobRefreshState.FAILED, false));
        // Resolved jobs do not count: forced UNKNOWN and terminal jobs with a settled refresh.
        jobs.add(jobInState(8L, LanceIndexJobMutationState.UNKNOWN, LanceIndexJobRefreshState.NOT_REQUIRED, true));
        jobs.add(jobInState(9L, LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.DONE, false));
        jobs.add(jobInState(10L, LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.NOT_REQUIRED, false));
        jobs.add(jobInState(11L, LanceIndexJobMutationState.NOT_COMMITTED, LanceIndexJobRefreshState.NOT_REQUIRED,
                false));

        List<LanceIndexJob> unresolved = new ArrayList<>();
        for (LanceIndexJob job : jobs) {
            if (job.isUnresolved()) {
                unresolved.add(job);
            }
        }
        Assertions.assertEquals(7, unresolved.size());

        LanceIndexJobQuota quota = new LanceIndexJobQuota();
        quota.rebuild(unresolved);
        Assertions.assertEquals(7L, quota.getGlobalCount());
        Assertions.assertEquals(7L, quota.getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(7L, quota.getTableCount(unresolved.get(0).getTableQuotaKey()));
    }

    @Test
    public void tableQuotaKeyIdentityAndLocatorHiding() {
        LanceIndexJobQuota.TableQuotaKey key = new LanceIndexJobQuota.TableQuotaKey(CATALOG_ID, LOCATOR_A);
        Assertions.assertEquals(new LanceIndexJobQuota.TableQuotaKey(CATALOG_ID, LOCATOR_A), key);
        Assertions.assertEquals(new LanceIndexJobQuota.TableQuotaKey(CATALOG_ID, LOCATOR_A).hashCode(), key.hashCode());
        Assertions.assertNotEquals(new LanceIndexJobQuota.TableQuotaKey(20L, LOCATOR_A), key);
        Assertions.assertNotEquals(new LanceIndexJobQuota.TableQuotaKey(CATALOG_ID, LOCATOR_B), key);
        Assertions.assertFalse(key.toString().contains(LOCATOR_A));
    }

    private static LanceIndexJob newJob(long jobId, long catalogId, String locator) {
        return new LanceIndexJob(jobId, "tester", catalogId, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, locator,
                "idx" + jobId, "idx" + jobId,
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 1L, null);
    }

    private static LanceIndexJob jobInState(long jobId, LanceIndexJobMutationState mutationState,
            LanceIndexJobRefreshState refreshState, boolean forceReleased) {
        LanceIndexJob job = newJob(jobId, CATALOG_ID, LOCATOR_A);
        job.setMutationState(mutationState);
        job.setRefreshState(refreshState);
        job.setForceReleased(forceReleased);
        return job;
    }
}
