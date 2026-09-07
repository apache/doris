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

package org.apache.doris.nereids.stats;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Global service for hbo plan stats. manager, including:
 * - HboPlanStatisticsProvider instance: hbo plan stats. cache (learned entries)
 * - HboPlanInfoProvider instance: plan info for runtime stats. identification
 * - pinned hbo statistics: manually injected statistics via the {@code HBO SET STATISTICS}
 *   statement, keyed by the hbo fingerprint; pinned entries take precedence over learned ones
 *   and are never overwritten or evicted by the automatic profile-based collection.
 *
 * <p>Pinned statistics are per-FE in memory; when
 * {@code Config.hbo_persist_pinned_to_internal_db} is enabled they are additionally written
 * through to {@code __internal_schema.hbo_statistics} and each FE loads the table into memory
 * once, lazily on first use. The in-memory cache stays authoritative: loaded entries are merged
 * with putIfAbsent, so entries injected while persistence was off (or concurrently) win over the
 * stored snapshot. A FE never refreshes entries SET by another FE during its lifetime; the
 * snapshot is only taken at load time.
 */
public class HboPlanStatisticsManager {
    private static final Logger LOG = LogManager.getLogger(HboPlanStatisticsManager.class);

    private static final long LOAD_RETRY_INTERVAL_MS = 30_000L;

    private HboPlanStatisticsProvider hboPlanStatisticsProvider;
    private HboPlanInfoProvider hboPlanInfoProvider;
    private final Cache<String, PinnedHboStatistics> pinnedPlanStatistics = Caffeine.newBuilder()
            .maximumSize(Math.max(Config.hbo_pinned_stats_cache_num, 0))
            .build();
    // whether pinned statistics have been loaded from the internal table (only when persistence
    // is enabled); planner/command threads access it concurrently
    private volatile boolean hboPinnedLoaded = false;
    // serializes the memory phases of the lazy load, SET and DELETE against each other; the
    // best-effort persistence SQL of SET/DELETE runs outside this lock
    private final Object pinnedLoadLock = new Object();
    // backoff timestamp after a failed load attempt, so a not-ready internal schema does not
    // trigger a DDL+SELECT retry for every group of every query (guarded by pinnedLoadLock)
    private long lastLoadFailedMs = 0L;

    public HboPlanStatisticsManager() {
        hboPlanStatisticsProvider = new MemoryHboPlanStatisticsProvider();
        hboPlanInfoProvider = new HboPlanInfoProvider();
    }

    public HboPlanStatisticsProvider getHboPlanStatisticsProvider() {
        return hboPlanStatisticsProvider;
    }

    public HboPlanInfoProvider getHboPlanInfoProvider() {
        return hboPlanInfoProvider;
    }

    /**
     * Inject (or overwrite) a pinned statistics entry for the given hbo fingerprint.
     * @param fingerprint hbo fingerprint (simplified group struct info sha256)
     * @param rows output row count that overrides the optimizer estimation
     * @param nodeType optional node kind recorded for diagnostics, may be null/empty
     */
    public void putPinnedPlanStatistics(String fingerprint, long rows, String nodeType) {
        putPinnedPlanStatistics(fingerprint, rows, nodeType, "");
    }

    /**
     * Inject (or overwrite) a pinned statistics entry.
     * @param structCanonical optional human-readable simplified struct info canonical string
     */
    public void putPinnedPlanStatistics(String fingerprint, long rows, String nodeType, String structCanonical) {
        // LRU bounded by Config.hbo_pinned_stats_cache_num; pinned entries are otherwise never
        // expired automatically and are only removed by HBO DELETE STATISTICS or eviction
        long createTimeMs = System.currentTimeMillis();
        synchronized (pinnedLoadLock) {
            pinnedPlanStatistics.put(fingerprint,
                    new PinnedHboStatistics(fingerprint, rows, nodeType, structCanonical, createTimeMs));
        }
        if (persistenceEnabled()) {
            HboStatisticsStore.persist(fingerprint, rows, nodeType, structCanonical, createTimeMs);
        }
    }

    public Optional<PinnedHboStatistics> getPinnedPlanStatistics(String fingerprint) {
        ensurePinnedLoaded();
        return Optional.ofNullable(pinnedPlanStatistics.getIfPresent(fingerprint));
    }

    /**
     * Remove a pinned statistics entry. Learned entries of the same key (if any) are left
     * untouched here; callers may invalidate them explicitly.
     */
    public void removePinnedPlanStatistics(String fingerprint) {
        synchronized (pinnedLoadLock) {
            pinnedPlanStatistics.invalidate(fingerprint);
        }
        if (persistenceEnabled()) {
            HboStatisticsStore.delete(fingerprint);
        }
    }

    public Map<String, PinnedHboStatistics> getAllPinnedPlanStatistics() {
        ensurePinnedLoaded();
        return Collections.unmodifiableMap(pinnedPlanStatistics.asMap());
    }

    private void ensurePinnedLoaded() {
        if (hboPinnedLoaded || !persistenceEnabled()) {
            return;
        }
        synchronized (pinnedLoadLock) {
            if (hboPinnedLoaded || !persistenceEnabled()) {
                return;
            }
            long now = System.currentTimeMillis();
            if (now - lastLoadFailedMs < LOAD_RETRY_INTERVAL_MS) {
                return;
            }
            List<PinnedHboStatistics> loaded = HboStatisticsStore.loadAll();
            if (loaded == null) {
                lastLoadFailedMs = now;
                return;
            }
            for (PinnedHboStatistics pinned : loaded) {
                // entries injected while persistence was off, or concurrently with the load,
                // must win over the stored snapshot: the in-memory cache stays authoritative
                pinnedPlanStatistics.asMap().putIfAbsent(pinned.getFingerprint(), pinned);
            }
            hboPinnedLoaded = true;
            LOG.info("loaded {} hbo pinned statistics entries from internal table", loaded.size());
        }
    }

    private static boolean persistenceEnabled() {
        return Config.hbo_persist_pinned_to_internal_db && FeConstants.enableInternalSchemaDb;
    }

    /**
     * A manually injected hbo statistics entry.
     */
    public static class PinnedHboStatistics {
        private final String fingerprint;
        private final long rows;
        private final String nodeType;
        private final String structCanonical;
        private final long createTime;

        PinnedHboStatistics(String fingerprint, long rows, String nodeType, String structCanonical,
                long createTime) {
            this.fingerprint = fingerprint;
            this.rows = rows;
            this.nodeType = nodeType == null ? "" : nodeType;
            this.structCanonical = structCanonical == null ? "" : structCanonical;
            this.createTime = createTime;
        }

        public String getFingerprint() {
            return fingerprint;
        }

        public long getRows() {
            return rows;
        }

        public String getNodeType() {
            return nodeType;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getStructCanonical() {
            return structCanonical;
        }
    }
}
