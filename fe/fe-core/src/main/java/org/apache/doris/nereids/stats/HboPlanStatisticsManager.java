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
import java.util.Map;
import java.util.Optional;

/**
 * Global service for hbo plan stats. manager, including:
 * - HboPlanStatisticsProvider instance: hbo plan stats. cache (learned entries)
 * - HboPlanInfoProvider instance: plan info for runtime stats. identification
 * - pinned hbo statistics: manually injected statistics via the {@code HBO SET STATISTICS}
 *   statement, keyed by the hbo fingerprint; pinned entries take precedence over learned ones
 *   and are never overwritten or evicted by the automatic profile-based collection.
 */
public class HboPlanStatisticsManager {
    private static final Logger LOG = LogManager.getLogger(HboPlanStatisticsManager.class);

    private HboPlanStatisticsProvider hboPlanStatisticsProvider;
    private HboPlanInfoProvider hboPlanInfoProvider;
    private final Cache<String, PinnedHboStatistics> pinnedPlanStatistics = Caffeine.newBuilder()
            .maximumSize(Math.max(Config.hbo_pinned_stats_cache_num, 0))
            .build();
    // whether pinned statistics have been loaded from the internal table (only when persistence
    // is enabled); single-threaded planner/command access makes this flag safe enough
    private volatile boolean hboPinnedLoaded = false;

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
        pinnedPlanStatistics.put(fingerprint,
                new PinnedHboStatistics(fingerprint, rows, nodeType, structCanonical, createTimeMs));
        if (Config.hbo_persist_pinned_to_internal_db && FeConstants.enableInternalSchemaDb) {
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
        pinnedPlanStatistics.invalidate(fingerprint);
        if (Config.hbo_persist_pinned_to_internal_db && FeConstants.enableInternalSchemaDb) {
            HboStatisticsStore.delete(fingerprint);
        }
    }

    public Map<String, PinnedHboStatistics> getAllPinnedPlanStatistics() {
        ensurePinnedLoaded();
        return Collections.unmodifiableMap(pinnedPlanStatistics.asMap());
    }

    private void ensurePinnedLoaded() {
        if (hboPinnedLoaded || !Config.hbo_persist_pinned_to_internal_db
                || !FeConstants.enableInternalSchemaDb) {
            return;
        }
        try {
            for (PinnedHboStatistics pinned : HboStatisticsStore.loadAll()) {
                pinnedPlanStatistics.put(pinned.getFingerprint(), pinned);
            }
            hboPinnedLoaded = true;
            LOG.info("loaded {} hbo pinned statistics entries from internal table",
                    pinnedPlanStatistics.asMap().size());
        } catch (Throwable t) {
            // table may not be ready yet (e.g. internal schema initialization in progress);
            // retry on the next access
            LOG.warn("failed to load hbo pinned statistics from internal table", t);
        }
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
