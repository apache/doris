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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global service for hbo plan stats. manager, including:
 * - HboPlanStatisticsProvider instance: hbo plan stats. cache (learned entries)
 * - HboPlanInfoProvider instance: plan info for runtime stats. identification
 * - pinned hbo statistics: manually injected statistics via the {@code HBO SET STATISTICS}
 *   statement, keyed by the hbo fingerprint; pinned entries take precedence over learned ones
 *   and are never overwritten or evicted by the automatic profile-based collection.
 */
public class HboPlanStatisticsManager {
    private HboPlanStatisticsProvider hboPlanStatisticsProvider;
    private HboPlanInfoProvider hboPlanInfoProvider;
    private final Map<String, PinnedHboStatistics> pinnedPlanStatistics = new ConcurrentHashMap<>();

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
        pinnedPlanStatistics.put(fingerprint,
                new PinnedHboStatistics(fingerprint, rows, nodeType, System.currentTimeMillis()));
    }

    public Optional<PinnedHboStatistics> getPinnedPlanStatistics(String fingerprint) {
        return Optional.ofNullable(pinnedPlanStatistics.get(fingerprint));
    }

    /**
     * Remove a pinned statistics entry. Learned entries of the same key (if any) are left
     * untouched here; callers may invalidate them explicitly.
     */
    public void removePinnedPlanStatistics(String fingerprint) {
        pinnedPlanStatistics.remove(fingerprint);
    }

    public Map<String, PinnedHboStatistics> getAllPinnedPlanStatistics() {
        return Collections.unmodifiableMap(pinnedPlanStatistics);
    }

    /**
     * A manually injected hbo statistics entry.
     */
    public static class PinnedHboStatistics {
        private final String fingerprint;
        private final long rows;
        private final String nodeType;
        private final long createTime;

        PinnedHboStatistics(String fingerprint, long rows, String nodeType, long createTime) {
            this.fingerprint = fingerprint;
            this.rows = rows;
            this.nodeType = nodeType == null ? "" : nodeType;
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
    }
}
