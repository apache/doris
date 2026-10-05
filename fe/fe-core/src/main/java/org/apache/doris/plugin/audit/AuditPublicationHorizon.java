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
import org.apache.doris.system.Frontend;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *       rows of that table; a row the reporter stopped refreshing is only IGNORED
 *       when its FE is provably GONE (the events died with it). A live FE whose
 *       keepalive writes fail - or one whose liveness cannot be decided - makes the
 *       read FAIL CLOSED instead: its pipeline may still owe events (and may even
 *       have gained events with OLDER start times), so neither the stale value may
 *       be trusted nor the fence released (round-38 #2).</li>
 *   <li>a COMMITTED batch whose rows are only not readable yet (Publish Timeout) keeps
 *       fencing even after its FE died: its {@code committed_fence_ms} marker survives
 *       the death for the same bound the loader itself applies (round-40 #10) - the
 *       FEs' writer-zone history in that row is kept for exactly as long as durable
 *       capture progress has not passed it, so no uncompleted window loses its zone
 *       (round-40 #2).</li>
 * </ul>
 */
public final class AuditPublicationHorizon {

    private static final Logger LOG = LogManager.getLogger(AuditPublicationHorizon.class);

    /**
     * A reported row older than this is OVERDUE: its FE's reporter failed to refresh it
     * (keepalive failures, a stuck thread, or a crash). An overdue row is only DISCARDED
     * when the FE is provably gone - its events died with it, and fencing forever would
     * freeze the capture instead of protecting anything. For a FE that is still alive (or
     * whose liveness cannot be decided) the read FAILS CLOSED instead: the reporter's
     * writes can fail for minutes while the FE still owes its events, and its pipeline
     * may even hold NEW events with older start times than the last reported value
     * (round-38 #2). Must be comfortably larger than the reporter's keepalive interval
     * ({@link AuditLoader#HORIZON_KEEPALIVE_MILLIS}).
     */
    public static final long ROW_STALE_MILLIS = 5 * 60 * 1000L;

    /**
     * How long the committed-publication fence of a PROVABLY GONE FE keeps fencing
     * (round-40 #10): a batch whose stream load reported Publish Timeout is COMMITTED,
     * and its rows can become readable AFTER the FE died - dropping the fence at death
     * would let the capture checkpoint past them. Deliberately the SAME bound the loader
     * itself applies ({@link AuditLoader#PUBLISH_FENCE_MAX_MILLIS}): after it the batch
     * is "conclusively lost" for both sides, so fencing longer would freeze the capture
     * without protecting anything.
     */
    public static final long COMMITTED_FENCE_SURVIVAL_MILLIS =
            AuditLoader.PUBLISH_FENCE_MAX_MILLIS;

    private static final String SELECT_ROWS_SQL =
            "SELECT `fe_name`, `horizon_ms`, `update_time`, `writer_zones`,"
                    + " `committed_fence_ms` FROM `"
                    + FeConstants.INTERNAL_DB_NAME + "`."
                    + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`";
    private static final String SELECT_OWN_ROW_SQL = "SELECT `horizon_ms`, `writer_zones`,"
            + " `committed_fence_ms` FROM `"
            + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    // ONE atomic statement per report: the table is a merge-on-write UNIQUE KEY(`fe_name`)
    // table, so an INSERT of an existing fe_name IS the update of that FE's row - there is
    // no window (a crash, or a reader between two statements) in which the row is MISSING
    // while the follower still owes an old event (round-37 #9: the previous DELETE+INSERT
    // committed separately and the leader could read no row in between). A zero horizon
    // with an EMPTY writer-zone registry deletes the row instead (also one statement): a
    // missing row and a zero row are the same "nothing outstanding" to every reader. A
    // zero horizon WITH recorded zones keeps the row (round-39 #3): the capture still
    // needs this FE's zone history for windows it has not completed, and deleting the row
    // would drop exactly that knowledge.
    private static final String UPSERT_OWN_ROW_SQL = "INSERT INTO `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`"
            + " (`fe_name`, `horizon_ms`, `update_time`, `writer_zones`, `committed_fence_ms`)"
            + " VALUES ('${feName}', ${horizonMs}, '${updateTime}', '${writerZones}',"
            + " ${committedFenceMs})";
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

    /**
     * Test seam: the liveness of the FE behind a reported fence row ({@code null} =
     * undecidable; see {@link #reportingFeAlive}). Null in production.
     */
    @VisibleForTesting
    static volatile Function<String, Boolean> feAliveProbeForTest;

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
     * table cannot be read or when the fence is INCOMPLETE (a live reporter's overdue row)
     * - the caller must NOT advance without a complete fence (round-36 #1: an unreadable
     * follower row is exactly the hole this guards; round-38 #2 added the overdue-live
     * reporter, whose last confirmed value may already be stale).
     */
    public static long clusterHorizon() {
        long oldest = localHorizon();
        return minPositive(oldest, remoteHorizon());
    }

    /**
     * The zones the CLUSTER's audit writers have RENDERED rows in - this FE's own live
     * history plus every fresh reporter row's registered zones (round-39 #3). The SPM
     * capture must render a window pass in every one of them before completing the
     * window: rows stored under a zone that is no longer current are invisible to bounds
     * rendered in the current zone, and a zone change BETWEEN two capture cycles is
     * invisible to the capture's own start/end comparisons. A read failure fails closed
     * exactly like {@link #clusterHorizon()}.
     *
     * @return the zone IDs that may own audit rows
     */
    public static Set<String> clusterWriterZones() {
        Set<String> zones = new LinkedHashSet<>(AuditWriterZones.zones());
        zones.addAll(remoteWriterZones());
        return zones;
    }

    /**
     * The zones registered in the rows of the shared table. A row's zones stay REQUIRED
     * until the DURABLE CAPTURE PROGRESS has passed the row's last refresh (round-40 #2):
     * a follower can publish a row under a zone and then stop reporting (crash, stalled
     * keepalive) while an uncompleted capture window still contains that row - dropping
     * its zone here let a UTC leader exhaust the window scanning only its own zones and
     * checkpoint past the row stored under e.g. -05:00. Fresh rows are always included;
     * an unreadable capture watermark keeps every zone (fail closed).
     */
    private static Set<String> remoteWriterZones() {
        List<Object[]> rows;
        Supplier<List<Object[]>> reader = horizonRowsReaderForTest;
        if (reader != null) {
            rows = reader.get();
        } else if (!sharedTableAvailable()) {
            return Collections.emptySet(); // no live FE environment: nothing reported
        } else {
            try {
                List<ResultRow> result = StatisticsUtil.executeQuery(
                        SELECT_ROWS_SQL, Collections.emptyMap(), IO_TIMEOUT_SECONDS);
                rows = new ArrayList<>();
                if (result != null) {
                    for (ResultRow row : result) {
                        List<String> values = row.getValues();
                        if (values == null || values.size() < 3) {
                            continue;
                        }
                        rows.add(new Object[] {values.get(0).trim(),
                                Long.parseLong(values.get(1).trim()),
                                parseUpdateTime(values.get(2).trim()),
                                values.size() > 3 && values.get(3) != null
                                        ? values.get(3) : "",
                                values.size() > 4 && values.get(4) != null
                                        && !values.get(4).trim().isEmpty()
                                        ? Long.parseLong(values.get(4).trim()) : 0L});
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("SPM capture cannot read the cluster audit"
                        + " writer zones: " + e.getMessage(), e);
            }
        }
        Set<String> zones = new LinkedHashSet<>();
        long now = System.currentTimeMillis();
        long coveredThrough = AuditWriterZones.captureCoveredThrough();
        for (Object[] row : rows) {
            if (row == null || row.length < 4 || row[2] == null) {
                continue;
            }
            long updatedAt = (Long) row[2];
            if (updatedAt <= 0) {
                continue;
            }
            if (now - updatedAt > ROW_STALE_MILLIS && updatedAt < coveredThrough) {
                // an expired row whose last render is COVERED by durable capture
                // progress: no uncompleted window can still contain its rows
                continue;
            }
            zones.addAll(AuditWriterZones.decode((String) row[3]));
        }
        return zones;
    }

    /**
     * The minimum horizon over the FRESH rows of the shared table (0 when none / when
     * every overdue row belongs to a FE that is provably gone). A read failure - or an
     * overdue row of a live / undecidable reporter - propagates as a retryable
     * {@link IllegalStateException} (see {@link #reportingFeAlive}).
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
                        if (values == null || values.size() < 3) {
                            continue;
                        }
                        rows.add(new Object[] {values.get(0).trim(),
                                Long.parseLong(values.get(1).trim()),
                                parseUpdateTime(values.get(2).trim()),
                                values.size() > 3 && values.get(3) != null
                                        ? values.get(3) : "",
                                values.size() > 4 && values.get(4) != null
                                        && !values.get(4).trim().isEmpty()
                                        ? Long.parseLong(values.get(4).trim()) : 0L});
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
            if (row == null || row.length < 3 || row[0] == null
                    || row[1] == null || row[2] == null) {
                continue;
            }
            String feName = (String) row[0];
            long horizon = (Long) row[1];
            long updatedAt = (Long) row[2];
            long committedFence = row.length > 4 && row[4] != null ? (Long) row[4] : 0L;
            // the COMMITTED fence is the stronger statement of the two (round-40 #10):
            // the row's horizon already covers it while the reporter is alive, but after
            // a crash only the marker says that rows may still publish
            long fence = Math.max(horizon, committedFence);
            if (fence <= 0) {
                continue; // nothing outstanding: a zero row needs no fence
            }
            if (updatedAt <= 0 || now - updatedAt > ROW_STALE_MILLIS) {
                // Round-38 #2: an OVERDUE row does NOT mean its FE is gone - its keepalive
                // upserts can fail for minutes while the FE still holds the events, and
                // its pipeline may even have GAINED events with older start times - so the
                // stale VALUE cannot be trusted either. Only a KNOWN-GONE FE releases its
                // fence (the events died with it); a live - or an undecidable - reporter
                // fails this read closed, and the capture skips the cycle and retries
                // promptly instead of checkpointing past the unread fence.
                Boolean alive = reportingFeAlive(feName);
                if (Boolean.FALSE.equals(alive)) {
                    if (committedFence > 0
                            && now - updatedAt <= COMMITTED_FENCE_SURVIVAL_MILLIS) {
                        // Round-40 #10: the batch is COMMITTED, so its rows can become
                        // readable even though the FE is dead; the fence survives its
                        // death until they are observed or the same bound the loader
                        // itself applies has elapsed
                        LOG.warn("audit publication horizon: keeping the COMMITTED fence"
                                        + " {} of FE {} (not refreshed for {} ms) although the"
                                        + " FE is gone: the committed batch may still publish",
                                committedFence, feName, now - updatedAt);
                        oldest = minPositive(oldest, committedFence);
                        continue;
                    }
                    LOG.warn("audit publication horizon: dropping the overdue fence of FE"
                            + " {} (oldest event {}, not refreshed for {} ms): the FE is"
                            + " gone, its events died with it", feName, horizon, now - updatedAt);
                    continue;
                }
                throw new IllegalStateException("SPM capture cannot trust the cluster audit"
                        + " publication horizon: the fence of FE " + feName + " (oldest event"
                        + " " + horizon + ") has not been refreshed for " + (now - updatedAt)
                        + " ms while the FE is " + (alive == null ? "not confirmably gone"
                        : "still alive") + "; the capture must retry on a later cycle");
            }
            oldest = minPositive(oldest, fence);
        }
        return oldest;
    }

    /**
     * Whether the FE that reported a fence row is still ALIVE: {@code null} when that
     * cannot be decided (no live environment / no membership view / a failed lookup), and
     * {@code false} ONLY when the FE is provably gone. Called for OVERDUE rows only: the
     * capture runs on the leader, whose frontend list tracks every member's heartbeat, so
     * a row whose FE is absent from the membership is a leftover whose events died with
     * that FE (see {@link #remoteHorizon}, round-38 #2).
     */
    private static Boolean reportingFeAlive(String feName) {
        Function<String, Boolean> probe = feAliveProbeForTest;
        if (probe != null) {
            return probe.apply(feName);
        }
        try {
            Env env = Env.getCurrentEnv();
            if (env == null || feName == null || feName.isEmpty()) {
                return null;
            }
            List<Frontend> frontends = env.getFrontends(null);
            if (frontends == null || frontends.isEmpty()) {
                return null; // no membership view (not the leader / not ready): undecidable
            }
            for (Frontend frontend : frontends) {
                if (feName.equals(frontend.getNodeName())) {
                    return frontend.isAlive();
                }
            }
            return Boolean.FALSE; // not a member any more: dropped / decommissioned
        } catch (Throwable t) {
            return null;
        }
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
     * @param horizon the local horizon (0 = nothing outstanding: the row is deleted
     *                unless the WRITER-ZONE set or a committed publish fence keeps it)
     * @return whether the written state is confirmed visible in the shared table
     */
    public static boolean reportLocalHorizon(long horizon) {
        // A COMMITTED-but-unreadable batch must survive this FE's death (round-40 #10):
        // the fence is folded in HERE, at write time, so even a report computed before the
        // batch timed out (a stale zero, or the close path's clear) cannot DELETE or
        // understate it - this is what keeps the crash/close gap closed.
        long committedFence = AuditLoader.oldestCommittedPublishFenceEventTime();
        long effectiveHorizon = Math.max(Math.max(0L, horizon), committedFence);
        Function<Long, Boolean> writer = localHorizonWriterForTest;
        if (writer != null) {
            return Boolean.TRUE.equals(writer.apply(effectiveHorizon));
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
            // the writer-zone snapshot travels with every report (round-39 #3), and a
            // ZERO-horizon report KEEPS the row while zones are registered: the capture
            // still needs them for windows it has not completed, so deleting the row
            // would drop exactly that knowledge
            String writerZones = AuditWriterZones.encode();
            if (effectiveHorizon > 0 || !writerZones.isEmpty()) {
                params.put("horizonMs", String.valueOf(effectiveHorizon));
                params.put("updateTime", renderUpdateTime(System.currentTimeMillis()));
                params.put("writerZones", StatisticsUtil.escapeSQL(writerZones));
                params.put("committedFenceMs", String.valueOf(committedFence));
                StatisticsUtil.execUpdate(UPSERT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
            } else {
                StatisticsUtil.execUpdate(DELETE_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
            }
            boolean confirmed = ownRowConfirms(feName, effectiveHorizon, writerZones, committedFence);
            if (confirmed) {
                // Only a CONFIRMED report makes the zones known to the capture (round-41
                // #6): the registry keeps a fresh zone outside the covered-through filter
                // until this point, so it cannot be dropped before it was ever shared.
                AuditWriterZones.markReported(AuditWriterZones.decode(writerZones));
            }
            return confirmed;
        } catch (Exception e) {
            LOG.warn("audit publication horizon: cannot report the local fence {}: {}",
                    horizon, e.getMessage());
            return false;
        }
    }

    /**
     * Reports "this FE is done" (graceful shutdown). The row is DELETED unless a
     * writer-zone set or a COMMITTED-but-unreadable batch keeps it (round-40 #10): the
     * latter's rows can still publish after this FE stops, so its fence must survive it.
     */
    public static void clearLocalReport() {
        reportLocalHorizon(0L);
    }

    /**
     * Re-reads THIS FE's row and checks it matches what was just written: the horizon
     * value, the WRITER-ZONE SET (round-40 #3: a zero-horizon report carrying a NEW zone
     * set is indistinguishable from the old row by the horizon alone - if the UPSERT
     * committed without being readable, the reporter would record the new zones as
     * reported and the capture could checkpoint in the gap without scanning the new
     * zone) and the COMMITTED fence (round-40 #10: an unreadable marker would let a crash
     * drop the fence). A read failure is an UNCONFIRMED write - the caller retries.
     */
    private static boolean ownRowConfirms(String feName, long horizon, String writerZones,
            long committedFenceMs) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("feName", StatisticsUtil.escapeSQL(feName));
        List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
        long readBack = 0;
        String readBackZones = "";
        long readBackCommitted = 0;
        if (rows != null && !rows.isEmpty()) {
            List<String> values = rows.get(0).getValues();
            if (values != null && !values.isEmpty()) {
                readBack = Long.parseLong(values.get(0).trim());
                readBackZones = values.size() > 1 && values.get(1) != null ? values.get(1) : "";
                readBackCommitted = values.size() > 2 && values.get(2) != null
                        && !values.get(2).trim().isEmpty()
                        ? Long.parseLong(values.get(2).trim()) : 0L;
            }
        }
        if (horizon > 0) {
            return readBack == horizon && readBackCommitted == committedFenceMs
                    && sameZones(readBackZones, writerZones);
        }
        return readBack <= 0 && readBackCommitted <= 0 && sameZones(readBackZones, writerZones);
    }

    /** Zone-set comparison of a read-back against the attempted report (null = empty). */
    private static boolean sameZones(String readBack, String attempted) {
        return (readBack == null ? "" : readBack.trim())
                .equals(attempted == null ? "" : attempted.trim());
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
