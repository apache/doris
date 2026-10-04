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

package org.apache.doris.plugin.audit;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AuditEventProcessor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The cluster-wide AUDIT PUBLICATION HORIZON: the start time (epoch millis, the
 * {@code time} column of {@code audit_log}) of the oldest audit event that any FE has
 * accepted but not yet PUBLISHED. The SPM capture scans the shared audit table from the
 * leader, so it uses this value as a progress FENCE: its next scan window must still
 * start at or before it, otherwise a row an FE still owes falls behind the advanced
 * watermark and is never captured.
 *
 * <p>Three layers make the fence complete:
 * <ul>
 *   <li>{@link #localHorizon()} folds THIS FE's whole audit pipeline: completed queries
 *       still held by the {@link WorkloadRuntimeStatusMgr} (they enter the pipeline
 *       before any loader sees them), the {@link AuditEventProcessor} queue and its
 *       in-flight event (a plugin can stall while an event is dequeued), and the
 *       {@link AuditLoader} queue / assembled batch / not-yet-visible batch (a stream
 *       load can report Publish Timeout after commit). The stages are read
 *       UPSTREAM-FIRST (round-37 #3): every handoff enqueues the event downstream
 *       BEFORE the upstream stage stops covering it, so an event transferred between
 *       two reads can never fall in the gap - it is either still seen upstream or
 *       already seen downstream. Each stage keeps the event owned across its own
 *       handoff as well (the manager holds dequeued events until the processor call
 *       returns, the processor dequeues and publishes in-flight atomically - round-37
 *       #1/#2).</li>
 *   <li>each FE REPORTS its local horizon into the shared
 *       {@link InternalSchema#SPM_AUDIT_HORIZON_TBL_NAME} table, so a follower's
 *       backlog is visible to the leader that runs the capture. INTERNAL statements
 *       (including the reporter's own SQL) are not part of either side: the capture
 *       never scans {@code is_internal = true} rows, so including them would only let
 *       the reporter's writes fence (and thereby re-trigger) themselves forever on an
 *       idle FE (round-37 #7).</li>
 *   <li>{@link #clusterHorizon()} is the MINIMUM over the local value and the FRESH
 *       rows of that table; a row the reporter stopped refreshing (its FE died or
 *       stopped reporting - the events are gone with it) is ignored.</li>
 * </ul>
 */
public final class AuditPublicationHorizon {

    private static final Logger LOG = LogManager.getLogger(AuditPublicationHorizon.class);

    /**
     * A reported row older than this is IGNORED: its FE stopped refreshing the fence
     * (crashed / killed / its reporter thread is gone), so the events it still owed are
     * lost with it and fencing progress forever would freeze the capture instead of
     * protecting anything. Must be comfortably larger than the reporter's keepalive
     * interval ({@link AuditLoader#HORIZON_KEEPALIVE_MILLIS}).
     */
    public static final long ROW_STALE_MILLIS = 5 * 60 * 1000L;

    private static final String SELECT_ROWS_SQL =
            "SELECT `horizon_ms`, `update_time` FROM `" + FeConstants.INTERNAL_DB_NAME + "`."
                    + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`";
    private static final String SELECT_OWN_ROW_SQL = "SELECT `horizon_ms` FROM `"
            + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    // ONE atomic statement per report: the table is a merge-on-write UNIQUE KEY(`fe_name`)
    // table, so an INSERT of an existing fe_name IS the update of that FE's row - there is
    // no window (a crash, or a reader between two statements) in which the row is MISSING
    // while the follower still owes an old event (round-37 #9: the previous DELETE+INSERT
    // committed separately and the leader could read no row in between). A zero horizon
    // deletes the row instead (also one statement): a missing row and a zero row are the
    // same "nothing outstanding" to every reader.
    private static final String UPSERT_OWN_ROW_SQL = "INSERT INTO `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`"
            + " (`fe_name`, `horizon_ms`, `update_time`) VALUES ('${feName}', ${horizonMs}, '${updateTime}')";
    private static final String DELETE_OWN_ROW_SQL = "DELETE FROM `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    private static final int IO_TIMEOUT_SECONDS = 10;

    /**
     * update_time is rendered AND parsed in UTC (round-37 #4): the column is a zone-less
     * DATETIME crossing FEs that may render their local wall time in different zones, so
     * the previous both-sides-local rendering made a fresh row look hours old to a reader
     * in another zone (discarded as stale, dropping that follower's fence). A fixed zone
     * on both sides makes the freshness comparison independent of either FE's time zone.
     */
    private static final DateTimeFormatter UPDATE_TIME_PATTERN =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter UPDATE_TIME_UTC_FORMATTER =
            UPDATE_TIME_PATTERN.withZone(ZoneOffset.UTC);

    /**
     * Test seam: the shared-table read (one row per FE). Null in production.
     */
    @VisibleForTesting
    static volatile Supplier<List<Object[]>> horizonRowsReaderForTest;

    /**
     * Test seam: the shared-table write of this FE's row. Returns whether the write is
     * CONFIRMED (the production implementation re-reads its own row). Null in production.
     */
    @VisibleForTesting
    static volatile Function<Long, Boolean> localHorizonWriterForTest;

    private AuditPublicationHorizon() {
    }

    /**
     * The oldest audit event THIS FE has accepted but not published, 0 when nothing is
     * outstanding: the MINIMUM over every stage of the local pipeline (see the class
     * javadoc). Cheap - no I/O - so callers may poll it.
     *
     * <p>The stages are read UPSTREAM-FIRST (round-37 #3): the pre-loader stages before
     * the loader. A downstream stage enqueues an event BEFORE the upstream stage
     * releases it, so reading upstream first means an event transferred between the two
     * reads is either still seen upstream (it has not transferred yet) or already seen
     * downstream (it transferred before the upstream read) - the previous
     * downstream-first order could read both stages around the transfer and miss it.
     */
    public static long localHorizon() {
        long oldest = preLoaderHorizon();
        oldest = minPositive(oldest, AuditLoader.oldestUnpublishedEventTime());
        return oldest;
    }

    /**
     * The stages BEFORE the audit loader, read UPSTREAM-FIRST (round-37 #3): the runtime
     * status manager (a completed query enters its list before the processor sees it, and
     * its dequeued events stay fenced until the processor call returns) and then the
     * processor (whose dequeued-in-flight event is published atomically with the
     * dequeue).
     */
    private static long preLoaderHorizon() {
        long oldest = 0;
        try {
            Env env = Env.getCurrentEnv();
            WorkloadRuntimeStatusMgr mgr = env == null ? null : env.getWorkloadRuntimeStatusMgr();
            if (mgr != null) {
                oldest = minPositive(oldest, mgr.oldestHeldAuditEventTime());
            }
        } catch (Throwable t) {
            LOG.debug("audit publication horizon: the workload runtime status manager is"
                    + " unavailable: {}", t.getMessage());
        }
        try {
            AuditEventProcessor processor = Env.getCurrentAuditEventProcessor();
            if (processor != null) {
                oldest = minPositive(oldest, processor.oldestQueuedOrInFlightEventTime());
            }
        } catch (Throwable t) {
            // an FE without this component (tests, partial startup) has nothing to fence
            LOG.debug("audit publication horizon: the audit event processor is unavailable: {}",
                    t.getMessage());
        }
        return oldest;
    }

    /**
     * The fence the CAPTURE uses: the minimum over this FE's own pipeline and the fresh
     * rows every other FE reported. Throws {@link IllegalStateException} when the shared
     * table cannot be read - the caller must NOT advance without a complete fence
     * (round-36 #1: an unreadable follower row is exactly the hole this guards).
     */
    public static long clusterHorizon() {
        long oldest = localHorizon();
        return minPositive(oldest, remoteHorizon());
    }

    /**
     * The minimum horizon over the FRESH rows of the shared table (0 when none / all
     * stale). A read failure propagates as a retryable {@link IllegalStateException}.
     */
    private static long remoteHorizon() {
        List<Object[]> rows;
        Supplier<List<Object[]>> reader = horizonRowsReaderForTest;
        if (reader != null) {
            rows = reader.get();
        } else if (!sharedTableAvailable()) {
            return 0L; // no live FE environment (unit tests / not ready): nothing reported
        } else {
            try {
                List<ResultRow> result = StatisticsUtil.executeQuery(
                        SELECT_ROWS_SQL, Collections.emptyMap(), IO_TIMEOUT_SECONDS);
                rows = new ArrayList<>();
                if (result != null) {
                    for (ResultRow row : result) {
                        List<String> values = row.getValues();
                        if (values == null || values.size() < 2) {
                            continue;
                        }
                        rows.add(new Object[] {Long.parseLong(values.get(0).trim()),
                                parseUpdateTime(values.get(1).trim())});
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("SPM capture cannot read the cluster audit"
                        + " publication horizon: " + e.getMessage(), e);
            }
        }
        long oldest = 0;
        long now = System.currentTimeMillis();
        for (Object[] row : rows) {
            if (row == null || row.length < 2 || row[0] == null || row[1] == null) {
                continue;
            }
            long horizon = (Long) row[0];
            long updatedAt = (Long) row[1];
            if (horizon <= 0) {
                continue;
            }
            if (updatedAt <= 0 || now - updatedAt > ROW_STALE_MILLIS) {
                continue; // the reporter stopped: its outstanding events are gone with it
            }
            oldest = minPositive(oldest, horizon);
        }
        return oldest;
    }

    /**
     * Publishes THIS FE's current horizon into the shared table (one row per FE) and
     * returns whether the written state is CONFIRMED readable from it. Called by the
     * audit loader's reporter thread on change and on its keepalive cadence; the caller
     * may remember the value as reported only when this returns true, so a failed (or
     * not yet visible) write is retried on the next tick instead of being treated as
     * done until the 60s keepalive (round-37 #5: SQL OK can still leave a COMMITTED
     * INSERT unpublished, and the previous void return silently swallowed failures the
     * reporter had already recorded as reported).
     *
     * <p>Each report is ONE atomic statement (round-37 #9): a merge-on-write upsert of
     * the FE's row for a positive horizon, a single DELETE for zero. A reader can never
     * observe the row missing while it is being refreshed by the old value.
     *
     * @param horizon the local horizon (0 = nothing outstanding: the row is deleted)
     * @return whether the written state is confirmed visible in the shared table
     */
    public static boolean reportLocalHorizon(long horizon) {
        Function<Long, Boolean> writer = localHorizonWriterForTest;
        if (writer != null) {
            return Boolean.TRUE.equals(writer.apply(horizon));
        }
        if (!sharedTableAvailable()) {
            // no live FE environment (unit tests / not ready): no shared table, and also
            // nothing that could be confirmed - the reporter keeps retrying
            return false;
        }
        String feName = AuditLoader.selfFeName();
        try {
            Map<String, String> params = new HashMap<>();
            params.put("feName", StatisticsUtil.escapeSQL(feName));
            if (horizon > 0) {
                params.put("horizonMs", String.valueOf(horizon));
                params.put("updateTime", renderUpdateTime(System.currentTimeMillis()));
                StatisticsUtil.execUpdate(UPSERT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
            } else {
                StatisticsUtil.execUpdate(DELETE_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
            }
            return ownRowConfirms(feName, horizon);
        } catch (Exception e) {
            LOG.warn("audit publication horizon: cannot report the local fence {}: {}",
                    horizon, e.getMessage());
            return false;
        }
    }

    /** Removes this FE's row (graceful shutdown: nothing more will be published). */
    public static void clearLocalReport() {
        reportLocalHorizon(0L);
    }

    /**
     * Re-reads THIS FE's row and checks it matches what was just written: a positive
     * horizon is confirmed when its row carries exactly that value, a zero horizon when
     * no (or a non-positive) row remains. A read failure is an UNCONFIRMED write - the
     * caller retries (round-37 #5).
     */
    private static boolean ownRowConfirms(String feName, long horizon) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("feName", StatisticsUtil.escapeSQL(feName));
        List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
        long readBack = 0;
        if (rows != null && !rows.isEmpty()) {
            List<String> values = rows.get(0).getValues();
            if (values != null && !values.isEmpty()) {
                readBack = Long.parseLong(values.get(0).trim());
            }
        }
        return horizon > 0 ? readBack == horizon : readBack <= 0;
    }

    /**
     * Renders the shared row's update_time in the FIXED UTC zone (round-37 #4): the
     * column is zone-less, so both the write and the read pin the same explicit zone
     * instead of each FE's local one.
     */
    @VisibleForTesting
    static String renderUpdateTime(long epochMillis) {
        return UPDATE_TIME_UTC_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
    }

    /** Parses the shared row's update_time rendering (see {@link #renderUpdateTime}). */
    @VisibleForTesting
    static long parseUpdateTime(String text) {
        return LocalDateTime.parse(text, UPDATE_TIME_PATTERN).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * Whether a live FE environment is up: the shared table only exists (and is only
     * meaningful) then. Unit-test JVMs and a starting FE answer false - the horizon then
     * degrades to the local pipeline instead of attempting real internal-table I/O.
     */
    private static boolean sharedTableAvailable() {
        try {
            Env env = Env.getCurrentEnv();
            return env != null && env.isReady();
        } catch (Throwable t) {
            return false;
        }
    }

    private static long minPositive(long current, long candidate) {
        if (candidate <= 0) {
            return current;
        }
        return current == 0 || candidate < current ? candidate : current;
    }
}
