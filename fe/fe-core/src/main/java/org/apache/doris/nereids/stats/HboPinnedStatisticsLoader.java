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

import org.apache.doris.common.util.Daemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Loads the pinned hbo statistics this FE has persisted in
 * {@code __internal_schema.hbo_statistics} into memory, once, in the background.
 *
 * <p>The load is a DDL plus a full table scan, so it must not run on a user query's planning
 * thread: it would block that query for the duration of the internal statement and, worse, park
 * every other planner on the manager's monitor while it runs. It also must not run inside a
 * SET/DELETE which holds that monitor: the internal statement the load issues is planned like any
 * other query, and if hbo is enabled for the internal session its plan consults the pinned cache
 * again - re-entering the load in the middle of the memory update and the DB write of that
 * SET/DELETE (which could resurrect the very entry a DELETE just removed).
 *
 * <p>The daemon therefore owns the only call to the load. It retries every
 * {@link HboPlanStatisticsManager#LOAD_RETRY_INTERVAL_MS} until the entries are in memory (the
 * internal schema may not be ready right after start-up), and stops as soon as the manager has
 * everything it needs - which also covers a FE whose internal schema database is disabled, and a
 * FE whose persistence is configured off.
 */
public class HboPinnedStatisticsLoader extends Daemon {
    private static final Logger LOG = LogManager.getLogger(HboPinnedStatisticsLoader.class);

    private final HboPlanStatisticsManager manager;

    public HboPinnedStatisticsLoader(HboPlanStatisticsManager manager) {
        super("hbo-pinned-statistics-loader", HboPlanStatisticsManager.LOAD_RETRY_INTERVAL_MS);
        this.manager = manager;
    }

    @Override
    protected void runOneCycle() {
        if (!manager.needsPinnedStatisticsLoad()) {
            // either the entries are in memory, or nothing can be loaded on this FE (the internal
            // schema database is disabled): this thread has nothing left to do
            LOG.info("hbo pinned statistics loader stops, loaded={}", manager.isPinnedStatisticsLoaded());
            exit();
            return;
        }
        manager.loadPinnedStatistics();
    }
}
