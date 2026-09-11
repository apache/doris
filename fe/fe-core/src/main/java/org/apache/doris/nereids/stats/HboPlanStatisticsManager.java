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
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * stored snapshot, and a DELETE whose DB removal failed while the load was still pending is
 * tombstoned so the pending load cannot resurrect it. Once the load succeeds, rows that were
 * actually seen in the loaded snapshot and suppressed by a tombstone are best-effort removed
 * from the table again, so they do not reappear after this FE restarts (unless that removal
 * fails too); a tombstoned row that the load did not see (e.g. hidden by replica read lag) is
 * not re-removed and may reappear after a FE restart. A FE never refreshes entries SET by
 * another FE during its lifetime: under this per-FE weak consistency model a tombstone can
 * therefore also suppress (and, if it was part of the loaded snapshot, remove) a row that
 * another FE re-created before this FE's snapshot, while a row created after the snapshot
 * survives until this FE restarts and reloads.
 */
public class HboPlanStatisticsManager {
    private static final Logger LOG = LogManager.getLogger(HboPlanStatisticsManager.class);

    private static final long LOAD_RETRY_INTERVAL_MS = 30_000L;

    /**
     * How a manually injected (pinned) row count is applied.
     */
    public enum PinnedType {
        /** unconditional override once the fingerprint matches. */
        EXACT,
        /**
         * only applied while the optimizer's own filter estimate is in the pathological
         * "extremely small" regime (see {@code Config.hbo_filter_small_ratio}), so a stale
         * injection can not override a healthy estimate.
         */
        FILTER_SMALL;

        /** Parse a user supplied type name, returns null when unknown. */
        public static PinnedType fromName(String name) {
            if (name == null) {
                return null;
            }
            for (PinnedType type : values()) {
                if (type.name().equalsIgnoreCase(name)) {
                    return type;
                }
            }
            return null;
        }
    }

    /**
     * Which literal mode produced the fingerprint a pinned entry is keyed by. It is unknown at
     * injection time (the statement only carries the fingerprint) and is bound lazily to the mode
     * that actually matched on the first application, purely for diagnostics / {@code HBO SHOW}.
     */
    public enum FingerprintKind {
        UNKNOWN,
        WITH_LITERAL,
        NO_LITERAL
    }

    private HboPlanStatisticsProvider hboPlanStatisticsProvider;
    private HboPlanInfoProvider hboPlanInfoProvider;
    // LRU bound follows the sibling hbo caches (see HboPlanInfoProvider): a non-positive
    // hbo_pinned_stats_cache_num disables the bound instead of silently disabling injection
    private final Cache<String, PinnedHboStatistics> pinnedPlanStatistics =
            (Config.hbo_pinned_stats_cache_num > 0
                    ? Caffeine.newBuilder().maximumSize(Config.hbo_pinned_stats_cache_num)
                    : Caffeine.newBuilder())
                    .build();
    // whether pinned statistics have been loaded from the internal table (only when persistence
    // is enabled); planner/command threads access it concurrently
    private volatile boolean hboPinnedLoaded = false;
    // serializes the memory phases of the lazy load, SET and DELETE against each other (including
    // the best-effort persistence SQL of DELETE, so no load can observe a row in between a
    // DELETE's memory invalidation and its DB removal)
    private final Object pinnedLoadLock = new Object();
    // backoff timestamp after a failed load attempt, so a not-ready internal schema does not
    // trigger a DDL+SELECT retry for every group of every query (guarded by pinnedLoadLock)
    private long lastLoadFailedMs = 0L;
    // fingerprints whose DELETE could not be removed from the internal table while a lazy load
    // is still pending (hboPinnedLoaded false); the pending or retrying load must not resurrect
    // them into memory. Cleared after a successful load, at which point the rows actually seen
    // in the loaded snapshot are best-effort removed from the table again; a tombstoned row the
    // load did not see (e.g. replica read lag) is not re-removed and may reappear after a FE
    // restart. Under the per-FE weak consistency model a tombstone can also suppress/remove a
    // row that another FE re-created before this FE's snapshot (see class javadoc).
    // (guarded by pinnedLoadLock)
    private final Set<String> pendingLoadTombstones = new HashSet<>();

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
     * Inject (or overwrite) a pinned statistics entry applied unconditionally.
     * @param fingerprint hbo fingerprint (simplified group struct info sha256)
     * @param rows output row count that overrides the optimizer estimation
     * @param structCanonical optional human-readable simplified struct info canonical string
     */
    public void putPinnedPlanStatistics(String fingerprint, long rows, String structCanonical) {
        putPinnedPlanStatistics(fingerprint, rows, PinnedType.EXACT, structCanonical);
    }

    /**
     * Inject (or overwrite) a pinned statistics entry.
     * @param type how the injected row count is applied (unconditional vs filter-small guard)
     * @param structCanonical optional human-readable simplified struct info canonical string
     */
    public void putPinnedPlanStatistics(String fingerprint, long rows, PinnedType type, String structCanonical) {
        // LRU bounded by Config.hbo_pinned_stats_cache_num; pinned entries are otherwise never
        // expired automatically and are only removed by HBO DELETE STATISTICS or eviction
        long createTimeMs = System.currentTimeMillis();
        synchronized (pinnedLoadLock) {
            pinnedPlanStatistics.put(fingerprint,
                    new PinnedHboStatistics(fingerprint, rows, type, structCanonical, createTimeMs));
            // a SET after a failed DELETE re-creates the entry: drop the deletion intent so a
            // pending load does not skip the re-created row
            pendingLoadTombstones.remove(fingerprint);
            // persist under the same lock as DELETE's DB removal, so concurrent same-key
            // SET/DELETE serialize their DB writes in the same order as their memory updates
            // and, when the best-effort writes succeed, the stored row matches the last memory
            // writer (a failed write only logs a warning and may reappear after a FE restart)
            if (persistenceEnabled()) {
                HboStatisticsStore.persist(fingerprint, rows, type, structCanonical, createTimeMs);
            }
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
            if (persistenceEnabled()) {
                // the DB removal runs under the lock so a lazy load can never SELECT the row in
                // the window between the memory invalidation and its DB removal (M-1 R1)
                if (!HboStatisticsStore.delete(fingerprint) && !hboPinnedLoaded) {
                    // the row is still stored and a pending/retrying load would resurrect it
                    // into memory: remember the deletion intent (M-1 R2)
                    pendingLoadTombstones.add(fingerprint);
                }
            }
        }
    }

    /**
     * Inject a learned entry (used by {@code HBO SET LEARNED STATISTICS}) so that the learned
     * lookup path can be exercised without waiting for a real profile publish. The injected entry
     * carries no input table statistics and therefore matches by fingerprint alone.
     */
    public void putLearnedPlanStatistics(String fingerprint, long rows) {
        PlanStatistics planStatistics = new PlanStatistics(0, rows, rows, rows, rows,
                rows, rows, rows, rows, 0, 0, 1);
        RecentRunsPlanStatistics runs = new RecentRunsPlanStatistics(
                Lists.newArrayList(new RecentRunsPlanStatisticsEntry(planStatistics, Lists.newArrayList())));
        hboPlanStatisticsProvider.putHboPlanStatsByFingerprint(fingerprint, runs);
    }

    /** Remove a learned entry by fingerprint. */
    public void removeLearnedPlanStatistics(String fingerprint) {
        hboPlanStatisticsProvider.removeHboPlanStats(fingerprint);
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
            List<String> suppressedByTombstone = new ArrayList<>();
            for (PinnedHboStatistics pinned : loaded) {
                // entries injected while persistence was off, or concurrently with the load,
                // must win over the stored snapshot, and entries deleted while the load was
                // still pending must not be resurrected: the in-memory cache stays authoritative
                if (pendingLoadTombstones.contains(pinned.getFingerprint())) {
                    suppressedByTombstone.add(pinned.getFingerprint());
                } else {
                    pinnedPlanStatistics.asMap().putIfAbsent(pinned.getFingerprint(), pinned);
                }
            }
            hboPinnedLoaded = true;
            if (!suppressedByTombstone.isEmpty()) {
                // the load succeeded, so the internal table is reachable: retry the failed DB
                // removals (idempotent) for exactly the rows that were seen in the loaded
                // snapshot, so they do not reappear after this FE restarts. Rows another FE
                // created after the snapshot are not touched; rows it created before the
                // snapshot may be removed (see class javadoc)
                int failed = 0;
                for (String fingerprint : suppressedByTombstone) {
                    if (!HboStatisticsStore.delete(fingerprint)) {
                        failed++;
                    }
                }
                if (failed > 0) {
                    LOG.warn("failed to remove {} of {} tombstoned hbo pinned statistics rows; "
                            + "the rows will reappear after the next FE restart", failed,
                            suppressedByTombstone.size());
                }
            }
            int tombstonesNotSeen = pendingLoadTombstones.size() - suppressedByTombstone.size();
            if (tombstonesNotSeen > 0) {
                // mostly benign (the row is already gone), but under replica read lag the row may
                // still be stored without this FE knowing; it then reappears after a FE restart
                LOG.info("{} tombstoned hbo pinned statistics rows were not seen in the loaded "
                        + "snapshot and may still be stored in the table; they may reappear after "
                        + "the next FE restart", tombstonesNotSeen);
            }
            pendingLoadTombstones.clear();
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
        private final PinnedType type;
        private final String structCanonical;
        private final long createTime;
        // which literal mode matched this entry; bound lazily on first application (diagnostics)
        private volatile FingerprintKind fingerprintKind = FingerprintKind.UNKNOWN;

        PinnedHboStatistics(String fingerprint, long rows, PinnedType type, String structCanonical,
                long createTime) {
            this.fingerprint = fingerprint;
            this.rows = rows;
            this.type = type == null ? PinnedType.EXACT : type;
            this.structCanonical = structCanonical == null ? "" : structCanonical;
            this.createTime = createTime;
        }

        /** Bind the fingerprint kind observed on the first successful application (idempotent). */
        public void bindFingerprintKind(FingerprintKind kind) {
            if (fingerprintKind == FingerprintKind.UNKNOWN && kind != null) {
                fingerprintKind = kind;
            }
        }

        public FingerprintKind getFingerprintKind() {
            return fingerprintKind;
        }

        public PinnedType getType() {
            return type;
        }

        public String getFingerprint() {
            return fingerprint;
        }

        public long getRows() {
            return rows;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getStructCanonical() {
            return structCanonical;
        }
    }
}
