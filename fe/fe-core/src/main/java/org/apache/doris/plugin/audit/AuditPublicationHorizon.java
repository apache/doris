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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.AuditEventProcessor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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
 *       load can report Publish Timeout after commit).</li>
 *   <li>each FE REPORTS its local horizon into the shared
 *       {@link InternalSchema#SPM_AUDIT_HORIZON_TBL_NAME} table, so a follower's
 *       backlog is visible to the leader that runs the capture.</li>
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
    private static final String DELETE_OWN_ROW_SQL = "DELETE FROM `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    private static final String INSERT_OWN_ROW_SQL = "INSERT INTO `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`"
            + " (`fe_name`, `horizon_ms`, `update_time`) VALUES ('${feName}', ${horizonMs}, '${updateTime}')";
    private static final int IO_TIMEOUT_SECONDS = 10;

    /**
     * Test seam: the shared-table read (one row per FE). Null in production.
     */
    @VisibleForTesting
    static volatile Supplier<List<Object[]>> horizonRowsReaderForTest;

    /**
     * Test seam: the shared-table write of this FE's row (delete + optional insert).
     * Null in production.
     */
    @VisibleForTesting
    static volatile Consumer<Long> localHorizonWriterForTest;

    private AuditPublicationHorizon() {
    }

    /**
     * The oldest audit event THIS FE has accepted but not published, 0 when nothing is
     * outstanding: the MINIMUM over every stage of the local pipeline (see the class
     * javadoc). Cheap - no I/O - so callers may poll it.
     */
    public static long localHorizon() {
        long oldest = 0;
        oldest = minPositive(oldest, AuditLoader.oldestUnpublishedEventTime());
        oldest = minPositive(oldest, preLoaderHorizon());
        return oldest;
    }

    /** The stages BEFORE the audit loader: held completed queries and the processor. */
    private static long preLoaderHorizon() {
        long oldest = 0;
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
                                TimeUtils.timeStringToLong(values.get(1).trim())});
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
     * Publishes THIS FE's current horizon into the shared table (one row per FE). Called
     * by the audit loader's reporter thread on change and on its keepalive cadence; a
     * write failure only logs - the next tick retries, and the row simply goes stale if
     * the FE dies.
     *
     * @param horizon the local horizon (0 = nothing outstanding)
     */
    public static void reportLocalHorizon(long horizon) {
        Consumer<Long> writer = localHorizonWriterForTest;
        if (writer != null) {
            writer.accept(horizon);
            return;
        }
        if (!sharedTableAvailable()) {
            return; // no live FE environment (unit tests / not ready): no shared table
        }
        String feName = AuditLoader.selfFeName();
        try {
            Map<String, String> deleteParams = new HashMap<>();
            deleteParams.put("feName", StatisticsUtil.escapeSQL(feName));
            StatisticsUtil.execUpdate(DELETE_OWN_ROW_SQL, deleteParams, IO_TIMEOUT_SECONDS);
            if (horizon > 0) {
                Map<String, String> insertParams = new HashMap<>();
                insertParams.put("feName", StatisticsUtil.escapeSQL(feName));
                insertParams.put("horizonMs", String.valueOf(horizon));
                insertParams.put("updateTime", TimeUtils.longToTimeString(System.currentTimeMillis()));
                StatisticsUtil.execUpdate(INSERT_OWN_ROW_SQL, insertParams, IO_TIMEOUT_SECONDS);
            }
        } catch (Exception e) {
            LOG.warn("audit publication horizon: cannot report the local fence {}: {}",
                    horizon, e.getMessage());
        }
    }

    /** Removes this FE's row (graceful shutdown: nothing more will be published). */
    public static void clearLocalReport() {
        reportLocalHorizon(0L);
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
