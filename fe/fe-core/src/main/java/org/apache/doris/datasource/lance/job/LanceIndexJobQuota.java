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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Three-level unresolved-job quota counters: per persisted table/locator
 * identity, per catalog, and globally. An unresolved job is any job that still
 * holds its same-name fence (PENDING/RUNNING, an unforced UNKNOWN, or a known
 * terminal job whose required refresh is not DONE); active-plus-UNKNOWN counts
 * stay bounded at every level.
 *
 * <p>This class only counts. Callers hold the manager write lock, so no
 * internal synchronization exists. Config-gated limits are resolved by the
 * admission layer and passed in as positive finite values. The counters are rebuilt from the durable jobs after
 * replay/image load and are never persisted themselves.
 */
public class LanceIndexJobQuota {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobQuota.class);

    private long globalCount;
    private final Map<Long, Long> catalogCounts = new HashMap<>();
    private final Map<TableQuotaKey, Long> tableCounts = new HashMap<>();

    /**
     * Check every level whose limit is positive, then increment all three
     * levels. Returns false (and increments nothing) when any enforced level
     * is full: "current &lt; limit" must hold at every level.
     *
     * <p>The manager admission path uses {@link #hasCapacity} plus
     * {@link #charge} instead: the check and the charge deliberately straddle
     * the edit-log write. This self-contained variant is reserved for direct
     * use by the later admission slice and by tests.
     */
    public boolean tryAcquire(LanceIndexJob job, long tableLimit, long catalogLimit, long globalLimit) {
        if (!hasCapacity(job, tableLimit, catalogLimit, globalLimit)) {
            return false;
        }
        charge(job);
        return true;
    }

    /**
     * Pure check variant of {@link #tryAcquire}: true when incrementing would
     * not exceed any positive finite limit. A non-positive limit is rejected.
     */
    public boolean hasCapacity(LanceIndexJob job, long tableLimit, long catalogLimit, long globalLimit) {
        Objects.requireNonNull(job, "job");
        if (tableLimit <= 0 || catalogLimit <= 0 || globalLimit <= 0) {
            return false;
        }
        if (globalCount >= globalLimit) {
            return false;
        }
        if (getCatalogCount(job.getCatalogId()) >= catalogLimit) {
            return false;
        }
        return getTableCount(job.getTableQuotaKey()) < tableLimit;
    }

    /**
     * Increment all three levels unconditionally. Used when a durable record
     * (already admitted, or replayed) is applied to memory.
     */
    public void charge(LanceIndexJob job) {
        globalCount++;
        catalogCounts.merge(job.getCatalogId(), 1L, Long::sum);
        tableCounts.merge(job.getTableQuotaKey(), 1L, Long::sum);
    }

    /**
     * Decrement all three levels. Underflow is clamped at zero and warned
     * about: it indicates a bookkeeping bug, not a reason to fail replay.
     */
    public void release(LanceIndexJob job) {
        if (globalCount <= 0) {
            LOG.warn("lance index job quota global underflow on release of job {}", job.getJobId());
        } else {
            globalCount--;
        }
        decrement(catalogCounts, job.getCatalogId(), job.getJobId());
        decrement(tableCounts, job.getTableQuotaKey(), job.getJobId());
    }

    private static <K> void decrement(Map<K, Long> counts, K key, long jobId) {
        Long current = counts.get(key);
        if (current == null || current <= 0) {
            LOG.warn("lance index job quota underflow on release of job {} at key {}", jobId, key);
            counts.remove(key);
            return;
        }
        if (current == 1L) {
            counts.remove(key);
        } else {
            counts.put(key, current - 1);
        }
    }

    /**
     * Reset and recount from the unresolved durable jobs (replay / image load).
     */
    public void rebuild(Iterable<LanceIndexJob> unresolvedJobs) {
        globalCount = 0;
        catalogCounts.clear();
        tableCounts.clear();
        for (LanceIndexJob job : unresolvedJobs) {
            charge(job);
        }
    }

    public long getGlobalCount() {
        return globalCount;
    }

    public long getCatalogCount(long catalogId) {
        return catalogCounts.getOrDefault(catalogId, 0L);
    }

    public long getTableCount(TableQuotaKey key) {
        return tableCounts.getOrDefault(key, 0L);
    }

    /**
     * Per persisted table/locator identity: (catalogId, normalizedLocator).
     */
    public static final class TableQuotaKey {
        private final long catalogId;
        private final String normalizedLocator;

        public TableQuotaKey(long catalogId, String normalizedLocator) {
            this.catalogId = catalogId;
            this.normalizedLocator = Objects.requireNonNull(normalizedLocator, "normalizedLocator");
        }

        public long getCatalogId() {
            return catalogId;
        }

        public String getNormalizedLocator() {
            return normalizedLocator;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableQuotaKey)) {
                return false;
            }
            TableQuotaKey that = (TableQuotaKey) o;
            return catalogId == that.catalogId && normalizedLocator.equals(that.normalizedLocator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, normalizedLocator);
        }

        /**
         * Deliberately omits the locator, like {@link LanceIndexFenceKey#toString()}.
         */
        @Override
        public String toString() {
            return "TableQuotaKey{catalogId=" + catalogId + '}';
        }
    }
}
