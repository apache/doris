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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Master-only retention sweeper of resolved Lance index job records. Resolved
 * records serve audit only; once one has been resolved for longer than
 * {@code Config.lance_index_job_keep_max_second} it is removed on all FEs
 * through one batch edit-log record per round, so every FE serves the same
 * SHOW LANCE INDEX JOBS view. Unresolved records are never removed, no matter
 * their age (fail-closed).
 *
 * <p>MasterDaemon itself performs no master check: the master-only semantics
 * come entirely from being started in {@code Env.startMasterOnlyDaemonThreads}.
 * Both retention configs are mutable and re-read every round.
 */
public class LanceIndexJobCleaner extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobCleaner.class);

    /** Upper bound of jobs removed per round, mirroring MAX_REMOVE_TXN_PER_ROUND. */
    private static final int MAX_REMOVE_PER_ROUND = 1024;

    public LanceIndexJobCleaner() {
        super("LanceIndexJobCleaner", Config.lance_index_job_clean_interval_second * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        // Both configs are mutable; re-read them every round.
        setInterval(Config.lance_index_job_clean_interval_second * 1000L);
        try {
            List<Long> removed = Env.getCurrentEnv().getLanceIndexJobManager()
                    .removeResolvedJobsOlderThan(Config.lance_index_job_keep_max_second * 1000L, MAX_REMOVE_PER_ROUND);
            if (!removed.isEmpty()) {
                LOG.info("lance index job retention GC removed {} resolved job(s): {}", removed.size(), removed);
            }
        } catch (Throwable t) {
            LOG.warn("lance index job retention GC round failed; retry next round", t);
        }
    }
}
