// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/LICENSE-2.0
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
 * through one batch edit-log record per batch, so every FE serves the same
 * SHOW LANCE INDEX JOBS view. Unresolved records are never removed, no matter
 * their age (fail-closed).
 *
 * <p>Each round drains expired records in repeated bounded batches rather than
 * a single capped one: the dispatcher can resolve far more jobs per hour than
 * one batch per default one-hour clean interval removes, and a capped single
 * batch would let stale records (and with them the audit map, the image, and
 * every dispatcher scan) grow without bound under sustained load. The batch
 * count per round is still bounded so one round's journal volume stays limited.
 *
 * <p>MasterDaemon itself performs no master check: the master-only semantics
 * come entirely from being started in {@code Env.startMasterOnlyDaemonThreads}.
 * Both retention configs are mutable and re-read every round. Values loaded
 * from fe.conf bypass the config validator (only ADMIN SET runs it), and the
 * validator itself accepts {@code Long.MAX_VALUE}, whose second-to-millisecond
 * conversion would overflow negative — so both conversions are clamped here:
 * the interval to at least one second (a non-positive period would kill the
 * thread inside {@code Thread.sleep} or spin it), and the keep window to a
 * saturated {@code Long.MAX_VALUE} milliseconds (retention effectively
 * forever, never an everything-expires-underflow).
 */
public class LanceIndexJobCleaner extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobCleaner.class);

    /** Upper bound of jobs removed per batch, mirroring MAX_REMOVE_TXN_PER_ROUND. */
    private static final int MAX_REMOVE_PER_BATCH = 1024;
    /**
     * Upper bound of batches per round (16,384 records): an order of magnitude
     * beyond the dispatcher's sustained resolution throughput, while one round's
     * journal stays at most this many batch records.
     */
    private static final int MAX_BATCHES_PER_ROUND = 16;

    public LanceIndexJobCleaner() {
        super("LanceIndexJobCleaner", cleanIntervalMs());
    }

    @Override
    protected void runAfterCatalogReady() {
        // Both configs are mutable; re-read them every round.
        setInterval(cleanIntervalMs());
        try {
            int removedTotal = 0;
            for (int batch = 0; batch < MAX_BATCHES_PER_ROUND; batch++) {
                List<Long> removed = Env.getCurrentEnv().getLanceIndexJobManager()
                        .removeResolvedJobsOlderThan(keepMs(), MAX_REMOVE_PER_BATCH);
                removedTotal += removed.size();
                if (removed.size() < MAX_REMOVE_PER_BATCH) {
                    // The expired backlog is drained; nothing above was skipped.
                    break;
                }
            }
            if (removedTotal > 0) {
                LOG.info("lance index job retention GC removed {} resolved job(s)", removedTotal);
            }
        } catch (Throwable t) {
            LOG.warn("lance index job retention GC round failed; retry next round", t);
        }
    }

    private static long cleanIntervalMs() {
        return Math.max(1L, Config.lance_index_job_clean_interval_second) * 1000L;
    }

    private static long keepMs() {
        long second = Math.max(0L, Config.lance_index_job_keep_max_second);
        return second > (Long.MAX_VALUE / 1000L) ? Long.MAX_VALUE : second * 1000L;
    }
}
