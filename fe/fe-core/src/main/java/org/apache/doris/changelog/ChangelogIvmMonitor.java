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

package org.apache.doris.changelog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Samples the changelog, the table streams and the IVM materialized views on the Master FE for monitoring.
 *
 * <p>Each round builds an immutable {@link ChangelogIvmMonitorSnapshot} and replaces the previous one at once, so
 * that /metrics renders the state from the snapshot instead of computing it while being scraped. A round that fails
 * keeps the previous snapshot, whose sample time then shows its age, and counts the failure.
 */
public class ChangelogIvmMonitor extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ChangelogIvmMonitor.class);

    // The monitor checks every second whether changelog_ivm_monitor_interval_seconds has passed since its last
    // round, so that a change of the interval applies at once: a daemon sleeping its own interval would only see
    // the change once the old interval is over.
    private static final long CHECK_INTERVAL_MS = 1000L;

    // only read and written by the daemon thread
    private long lastRoundMs = 0;

    // null before the first round completes, and while the monitor is disabled
    private volatile ChangelogIvmMonitorSnapshot snapshot;
    private final AtomicLong sampleFailures = new AtomicLong();

    public ChangelogIvmMonitor() {
        super("changelog-ivm-monitor", CHECK_INTERVAL_MS);
    }

    public static boolean isEnabled() {
        return Config.enable_feature_changelog && Config.enable_changelog_ivm_monitor;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!isEnabled()) {
            // Once enabled again, the monitor must not export what it sampled before it was disabled.
            snapshot = null;
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastRoundMs < Config.changelog_ivm_monitor_interval_seconds * 1000L) {
            return;
        }
        lastRoundMs = now;
        sample();
    }

    @VisibleForTesting
    void sample() {
        long startMs = System.currentTimeMillis();
        try {
            snapshot = buildSnapshot(startMs);
        } catch (Exception e) {
            sampleFailures.incrementAndGet();
            LOG.warn("failed to sample the changelog, the table streams and the IVM materialized views", e);
        }
    }

    @VisibleForTesting
    ChangelogIvmMonitorSnapshot buildSnapshot(long startMs) {
        // The statistics of a table are refreshed by TabletStatMgr or CloudTabletStatMgr, the same as SHOW DATA reads.
        long changelogSize = 0;
        long totalReplicaChangelogSize = 0;
        for (Database db : Env.getCurrentInternalCatalog().getDbs()) {
            for (Table table : db.getTables()) {
                if (table instanceof OlapTable) {
                    changelogSize += ((OlapTable) table).getChangelogSize(true);
                    totalReplicaChangelogSize += ((OlapTable) table).getChangelogSize(false);
                }
            }
        }
        long endMs = System.currentTimeMillis();
        return new ChangelogIvmMonitorSnapshot(endMs, endMs - startMs, changelogSize, totalReplicaChangelogSize);
    }

    public ChangelogIvmMonitorSnapshot getSnapshot() {
        return snapshot;
    }

    public long getSampleFailures() {
        return sampleFailures.get();
    }
}
