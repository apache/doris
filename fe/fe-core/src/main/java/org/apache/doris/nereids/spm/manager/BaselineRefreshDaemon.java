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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.qe.VariableMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * BaselineRefreshDaemon - periodic baseline cache refresh on EVERY FE (M3, design doc 6.6).
 *
 * Baselines are persisted in the shared __internal_schema.spm_baselines internal table, but
 * every FE keeps its own in-memory index (BaselineManager), and creation can happen on any
 * FE: user DDL runs on the FE that receives it and auto capture runs on the Leader FE.
 * Without a refresh a baseline created on FE A would never become visible on FE B, so
 * whether a user query hits a baseline would depend on which FE serves the client.
 *
 * This daemon therefore runs on all FEs (no isMaster check) and periodically applies an
 * incremental diff of the internal table to the local cache. It is read-only: it never
 * writes the internal table. The interval is spm_baseline_refresh_interval_seconds
 * (default 60s); a `SET GLOBAL` change is picked up within one old-interval delay.
 */
public class BaselineRefreshDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(BaselineRefreshDaemon.class);

    private static final BaselineRefreshDaemon INSTANCE = new BaselineRefreshDaemon();

    private BaselineRefreshDaemon() {
        super("BaselineRefreshDaemon",
                VariableMgr.getDefaultSessionVariable().getSpmBaselineRefreshIntervalSeconds() * 1000L);
    }

    public static BaselineRefreshDaemon getInstance() {
        return INSTANCE;
    }

    @Override
    protected void runAfterCatalogReady() {
        // Re-read the interval every cycle so `SET GLOBAL` takes effect within one old
        // interval. Clamp to >= 1s so a misconfiguration cannot spin the thread.
        long intervalMs = Math.max(1L,
                VariableMgr.getDefaultSessionVariable().getSpmBaselineRefreshIntervalSeconds())
                * 1000L;
        setInterval(intervalMs);
        if (Env.isCheckpointThread()) {
            return;
        }
        try {
            BaselineManager.getInstance().refreshFromInternalTable();
        } catch (Throwable t) {
            LOG.warn("SPM baseline refresh cycle failed", t);
        }
    }
}
