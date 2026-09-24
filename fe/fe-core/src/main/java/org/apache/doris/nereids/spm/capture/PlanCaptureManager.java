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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PlanCaptureManager - SPM auto capture scheduler (Phase 2, design doc 7.2.1 / 7.2.4).
 *
 * A Leader-FE daemon that periodically scans the audit_log internal table and
 * automatically creates baselines for high-value queries:
 *
 * - only queries executed by the Nereids planner are captured;
 * - the capture filter (PlanCaptureFilter) enforces the multi-table / table-exists /
 *   regex / performance-threshold rules;
 * - the baseline is built through the Phase 1 flow (SPMPlanner.buildBaselineFromSql:
 *   SPM-mode optimize + decompile + parameterize) with source = CAPTURE and the actual
 *   query_time filled for candidate ordering;
 * - duplicate (digest, planSql) baselines are skipped (BaselineManager dedup).
 *
 * The whole cycle is guarded by the global session variable enable_plan_capture
 * (default false, tunable via `SET GLOBAL enable_plan_capture = true`), and any
 * failure is logged and skipped so auto capture never breaks the cluster.
 */
public class PlanCaptureManager extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PlanCaptureManager.class);

    private static final PlanCaptureManager INSTANCE = new PlanCaptureManager();

    private final AuditLogScanner scanner = new AuditLogScanner();

    /** Capture filter, refreshed from the global session variables each cycle. */
    private PlanCaptureFilter filter;

    /** Last scan window start (epoch millis); 0 means "first run, scan one interval". */
    private long lastScanTimestamp = 0;

    // capture statistics (design doc 7.2.1 / 7.2.6)
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong skipDuplicateCount = new AtomicLong(0);
    private final AtomicLong skipSingleTableCount = new AtomicLong(0);
    private final AtomicLong skipFilterCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);

    private PlanCaptureManager() {
        super("PlanCaptureManager",
                VariableMgr.getDefaultSessionVariable().getPlanCaptureIntervalSeconds() * 1000L);
        this.filter = buildFilterFromGlobal();
    }

    public static PlanCaptureManager getInstance() {
        return INSTANCE;
    }

    /**
     * Builds a capture filter from the global session variables (so `SET GLOBAL`
     * changes to the thresholds / table regex take effect on the next cycle).
     *
     * @return a new filter
     */
    private static PlanCaptureFilter buildFilterFromGlobal() {
        SessionVariable global = VariableMgr.getDefaultSessionVariable();
        return new PlanCaptureFilter(global.getPlanCaptureIncludePattern(),
                global.getPlanCaptureExcludePattern(),
                global.getPlanCaptureMinQueryTimeMs(),
                global.getPlanCaptureMinScanRows());
    }

    @Override
    protected void runAfterCatalogReady() {
        SessionVariable global = VariableMgr.getDefaultSessionVariable();
        if (!global.isEnablePlanCapture()) {
            return;
        }
        if (!Env.getCurrentEnv().isMaster()) {
            // auto capture runs on the Leader FE only
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        try {
            // refresh the filter so SET GLOBAL changes take effect this cycle
            this.filter = buildFilterFromGlobal();

            long currentTime = System.currentTimeMillis();
            long scanStart = (lastScanTimestamp == 0)
                    ? currentTime - (long) global.getPlanCaptureIntervalSeconds() * 1000L
                    : lastScanTimestamp;
            if (scanStart >= currentTime) {
                return;
            }

            List<CapturedQuery> candidates =
                    scanner.scan(scanStart, currentTime, global.getPlanCaptureMaxBatchSize());
            for (CapturedQuery candidate : candidates) {
                processCandidate(candidate);
            }
            lastScanTimestamp = currentTime;

            LOG.info("PlanCapture cycle finished: captured={}, dup={}, singleTable={}, filtered={}, fail={}",
                    successCount.get(), skipDuplicateCount.get(), skipSingleTableCount.get(),
                    skipFilterCount.get(), failCount.get());
        } catch (Exception e) {
            LOG.warn("Plan capture cycle failed", e);
        }
    }

    /**
     * Filters and captures a single candidate query.
     *
     * @param candidate the audit candidate
     */
    private void processCandidate(CapturedQuery candidate) {
        try {
            // Level 3/5 filter: multi-table + table-name regex (pure logic)
            List<String> tables = PlanCaptureFilter.extractTableNames(candidate.getStmt());
            if (!filter.shouldCapture(candidate.toAuditEvent(), tables)) {
                if (tables.size() < 2) {
                    skipSingleTableCount.incrementAndGet();
                } else {
                    skipFilterCount.incrementAndGet();
                }
                return;
            }
            // Level 4 filter: tables must still exist
            if (!filter.allTablesExist(tables)) {
                skipFilterCount.incrementAndGet();
                return;
            }

            // Build the baseline through the Phase 1 flow (bindSql = planSql = stmt,
            // SPM-mode optimize + decompile + parameterize)
            BaselinePlan baseline;
            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                baseline = new SPMPlanner().buildBaselineFromSql(
                        ctx.connectContext, candidate.getStmt(), candidate.getStmt());
            }
            baseline.setSource(BaselineSource.CAPTURE);
            baseline.setQueryTimeMs(candidate.getQueryTimeMs());
            // audit correlation: the bindSql above is exactly the audit row's stmt text
            // (candidate.getStmt() read from audit_log verbatim), and the queryId lets the
            // audit record of the captured execution be located later through
            // `WHERE query_id = ...` (SHOW BASELINE PLANS exposes it as query_id)
            baseline.setQueryId(candidate.getQueryId());

            // Persist; createBaseline dedups identical (digest, planSql). The manager
            // stores the exact object reference, so we can tell "created" from
            // "duplicate" by reference identity.
            BaselineManager manager = BaselineManager.getInstance();
            long id = manager.createBaseline(baseline);
            if (manager.getBaseline(id) == baseline) {
                successCount.incrementAndGet();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Captured baseline {} for query: {}", id, candidate.getStmt());
                }
            } else {
                skipDuplicateCount.incrementAndGet();
            }
        } catch (Exception e) {
            failCount.incrementAndGet();
            LOG.warn("Failed to capture baseline for query: {}", candidate.getStmt(), e);
        }
    }

    // ==================== statistics (design doc 7.2.6) ====================

    /**
     * Snapshot of the capture counters.
     */
    public static class CaptureStats {
        public final long success;
        public final long duplicate;
        public final long singleTable;
        public final long filtered;
        public final long failed;

        CaptureStats(long success, long duplicate, long singleTable, long filtered, long failed) {
            this.success = success;
            this.duplicate = duplicate;
            this.singleTable = singleTable;
            this.filtered = filtered;
            this.failed = failed;
        }

        @Override
        public String toString() {
            return "CaptureStats{captured=" + success
                    + ", dup=" + duplicate
                    + ", singleTable=" + singleTable
                    + ", filtered=" + filtered
                    + ", fail=" + failed + "}";
        }
    }

    /**
     * Returns the current capture statistics.
     *
     * @return a snapshot of the capture counters
     */
    public CaptureStats getStats() {
        return new CaptureStats(successCount.get(), skipDuplicateCount.get(),
                skipSingleTableCount.get(), skipFilterCount.get(), failCount.get());
    }

    /**
     * For tests: resets the counters and the scan window.
     */
    public void resetForTest() {
        lastScanTimestamp = 0;
        successCount.set(0);
        skipDuplicateCount.set(0);
        skipSingleTableCount.set(0);
        skipFilterCount.set(0);
        failCount.set(0);
    }

    /**
     * For tests: processes a single candidate without touching the scanner.
     *
     * @param candidate the candidate query
     */
    public void processCandidateForTest(CapturedQuery candidate) {
        processCandidate(candidate);
    }

    /**
     * For tests: returns the internal filter.
     */
    public PlanCaptureFilter getFilter() {
        return filter;
    }
}
