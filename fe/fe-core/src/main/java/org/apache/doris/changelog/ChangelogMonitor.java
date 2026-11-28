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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Samples IVM refresh summaries independently of the metrics scrape path. */
public class ChangelogMonitor extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ChangelogMonitor.class);
    private static final long CHECK_INTERVAL_MS = 1000;

    private final Env env;
    private volatile SamplingState state = new SamplingState();

    private static class SamplingState {
        private long lastRoundNanos;
        private boolean hasRun;
        private volatile ChangelogMonitorSnapshot snapshot;
    }

    public ChangelogMonitor(Env env) {
        super("changelog-monitor", CHECK_INTERVAL_MS);
        this.env = env;
    }

    @Override
    protected void runAfterCatalogReady() {
        sample(System.nanoTime());
    }

    void sample(long now) {
        SamplingState current = state;
        if (!isActive()) {
            current.snapshot = null;
            current.hasRun = false;
            return;
        }

        long interval = TimeUnit.SECONDS.toNanos(Math.max(1, Config.changelog_monitor_interval_seconds));
        if (current.hasRun && now - current.lastRoundNanos < interval) {
            return;
        }
        current.hasRun = true;
        current.lastRoundNanos = now;
        try {
            current.snapshot = collect();
        } catch (RuntimeException e) {
            LOG.warn("Failed to collect a changelog monitor snapshot", e);
        }
    }

    private boolean isActive() {
        return Config.enable_changelog_monitor && env.isMaster() && env.isReady();
    }

    ChangelogMonitorSnapshot collect() {
        List<ChangelogMonitorSnapshot.IvmRefresh> refreshes = new ArrayList<>();
        for (Database db : env.getInternalCatalog().getDbs()) {
            for (Table table : db.getTables()) {
                if (table instanceof MTMV mv && mv.isIvm()) {
                    refreshes.add(new ChangelogMonitorSnapshot.IvmRefresh(
                            db.getFullName(), mv.getName(), mv.getId(), mv.getIvmRefreshMetricValues()));
                }
            }
        }
        return new ChangelogMonitorSnapshot(System.currentTimeMillis(), refreshes);
    }

    public void reset() {
        // An in-flight round can only publish into the old state.
        state = new SamplingState();
    }

    public ChangelogMonitorSnapshot getSnapshot() {
        return isActive() ? state.snapshot : null;
    }
}
