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
 * time column of audit_log) of the oldest audit event that any FE has
 * accepted but not yet PUBLISHED. The SPM capture scans the shared audit table from the
 * leader, so it uses this value as a progress FENCE: its next scan window must still
 * start at or before it, otherwise a row an FE still owes falls behind the advanced
 * watermark and is never captured.
 *
 * Three layers make the fence complete:
 *   localHorizon folds THIS FE's whole audit pipeline: completed queries
 *       still held by the WorkloadRuntimeStatusMgr (they enter the pipeline
 *       before any loader sees them), the AuditEventProcessor queue and its
 *       in-flight event (a plugin can stall while an event is dequeued), and the
 *       AuditLoader queue / assembled batch / not-yet-visible batch (a stream
 *       load can report Publish Timeout after commit). The stages are read
 *       UPSTREAM-FIRST: every handoff enqueues the event downstream
 *       BEFORE the upstream stage stops covering it, so an event transferred between
 *       two reads can never fall in the gap - it is either still seen upstream or
 *       already seen downstream. Each stage keeps the event owned across its own
 *       handoff as well (the manager holds dequeued events until the processor call
 *       returns, the processor dequeues and publishes in-flight atomically).
 *   each FE REPORTS its local horizon into the shared
 *       InternalSchema#SPM_AUDIT_HORIZON_TBL_NAME table, so a follower's
 *       backlog is visible to the leader that runs the capture. INTERNAL statements
 *       (including the reporter's own SQL) are not part of either side: the capture
 *       never scans is_internal = true rows, so including them would only let
 *       the reporter's writes fence (and thereby re-trigger) themselves forever on an
 *       idle FE.
 *   clusterHorizon is the MINIMUM over the local value and the FRESH
 *       rows of that table; a row the reporter stopped refreshing is only IGNORED
 *       when its FE is provably GONE (the events died with it). A live FE whose
 *       keepalive writes fail - or one whose liveness cannot be decided - makes the
 *       read FAIL CLOSED instead: its pipeline may still owe events (and may even
 *       have gained events with OLDER start times), so neither the stale value may
 *       be trusted nor the fence released - and liveness means
 *       MEMBERSHIP, not the heartbeat flag, whose transient false must not release a
 *       running FE's fence. An IDLE row (zero fence) contributes its
 *       own report INSTANT rather than nothing: the row vouches for its
 *       FE's pipeline only up to the moment it was written, and an event captured
 *       right after it may still be unpublished.
 *   a COMMITTED batch whose rows are only not readable yet (Publish Timeout) keeps
 *       fencing even after its FE died: its committed_fence_ms marker survives
 *       the death for the same bound the loader itself applies, and its
 *       batches' load labels ride along so the reader resolves each
 *       transaction and keeps the marker until the LAST one is terminal (VISIBLE /
 *       ABORTED) - a dead FE cannot re-report, and expiring the marker on the age bound
 *       alone lost a batch that published just after it. The FEs' writer-zone history in
 *       that row is kept for exactly as long as durable capture progress has not passed
 *       it, so no uncompleted window loses its zone.
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
     * . Must be comfortably larger than the reporter's keepalive interval
     * (AuditLoader#HORIZON_KEEPALIVE_MILLIS).
     */
    public static final long ROW_STALE_MILLIS = 5 * 60 * 1000L;

    /**
     * How long the committed-publication fence of a PROVABLY GONE FE keeps fencing when
     * its transaction CANNOT be resolved by label (made this
     * the LAST RESORT instead of the rule): a batch whose stream load reported Publish
     * Timeout is COMMITTED, and its rows can become readable AFTER the FE died -
     * dropping the fence at death would let the capture checkpoint past them. A row that
     * carries its batches' labels keeps fencing on the TRANSACTION outcome (only
     * VISIBLE / ABORTED releases it, whatever the age); the bound applies to the labels
     * the transaction manager cannot resolve at all, where it deliberately mirrors the
     * loader's own fallback (AuditLoader#PUBLISH_FENCE_MAX_MILLIS) so a
     * genuinely lost batch cannot freeze the capture forever.
     */
    public static final long COMMITTED_FENCE_SURVIVAL_MILLIS =
            AuditLoader.PUBLISH_FENCE_MAX_MILLIS;

    private static final String SELECT_ROWS_SQL =
            "SELECT `fe_name`, `horizon_ms`, `update_time`, `writer_zones`,"
                    + " `committed_fence_ms`, `committed_fence_labels` FROM `"
                    + FeConstants.INTERNAL_DB_NAME + "`."
                    + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`";
    private static final String SELECT_OWN_ROW_SQL = "SELECT `horizon_ms`, `writer_zones`,"
            + " `committed_fence_ms`, `committed_fence_labels`, `update_time` FROM `"
            + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    // ONE atomic statement per report: the table is a merge-on-write UNIQUE KEY(`fe_name`)
    // table, so an INSERT of an existing fe_name IS the update of that FE's row - there is
    // no window (a crash, or a reader between two statements) in which the row is MISSING
    // while the follower still owes an old event (the previous DELETE+INSERT
    // committed separately and the leader could read no row in between). A zero horizon
    // with an EMPTY writer-zone registry deletes the row instead (also one statement): a
    // missing row and a zero row are the same "nothing outstanding" to every reader. A
    // zero horizon WITH recorded zones keeps the row: the capture still
    // needs this FE's zone history for windows it has not completed, and deleting the row
    // would drop exactly that knowledge.
    private static final String UPSERT_OWN_ROW_SQL = "INSERT INTO `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "`"
            + " (`fe_name`, `horizon_ms`, `update_time`, `writer_zones`, `committed_fence_ms`,"
            + " `committed_fence_labels`)"
            + " VALUES ('${feName}', ${horizonMs}, '${updateTime}', '${writerZones}',"
            + " ${committedFenceMs}, '${committedFenceLabels}')";
    private static final String DELETE_OWN_ROW_SQL = "DELETE FROM `" + FeConstants.INTERNAL_DB_NAME + "`."
            + "`" + InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME + "` WHERE `fe_name` = '${feName}'";
    private static final int IO_TIMEOUT_SECONDS = 10;

    /**
     * update_time is rendered AND parsed in UTC: the column is a zone-less
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
     * Test seam enumerating the OTHER alive FEs that must have registered a row (see
     * verifyEveryLiveReporterRegistered); null falls back to the live membership
     * view, which unit tests do not have.
     */
    @VisibleForTesting
    static volatile Supplier<Set<String>> reporterNamesForTest;

    /**
     * Test seam: the liveness of the FE behind a reported fence row (null =
     * undecidable; see reportingFeAlive). Null in production.
     */
    @VisibleForTesting
    static volatile Function<String, Boolean> feAliveProbeForTest;

    /**
     * Test seam: THIS FE's own shared row as read at startup (see
     * restoreCarriedPublicationState). Each row is the tuple [horizon (Long),
     * update_time (Long, epoch millis), writer_zones (String), committed_fence_ms (Long),
     * committed_fence_labels (String)]. Null in production (the real read queries the
     * shared table).
     */
    @VisibleForTesting
    static volatile Supplier<List<Object[]>> ownRowRestoreReaderForTest;

    /**
     * The committed fence THIS FE's PREVIOUS incarnation reported and that is not
     * resolved yet (see restoreCarriedPublicationState): the restarted process starts
     * with an empty pending list and empty writer-zone registry, while the shared row -
     * keyed by the stable fe_name - is the only copy of a batch that was COMMITTED and
     * unreadable when the process went down. The first zero / empty UPSERT would
     * otherwise replace that fence (a pending window ending before the idle report could
     * then complete while the old load is still unreadable) and the old rendering zones
     * (a window scanned only in UTC while an older -05:00 row remains).
     */
    private static volatile long carriedCommittedFence = 0;
    private static volatile String carriedCommittedFenceLabels = "";
    private static volatile long carriedCommittedFenceUpdatedAt = 0;

    /** Whether the previous incarnation's row was already read (once per process). */
    private static volatile boolean carriedStateRestored = false;

    private AuditPublicationHorizon() {
    }

    /**
     * The oldest audit event THIS FE has accepted but not published, 0 when nothing is
     * outstanding: the MINIMUM over every stage of the local pipeline (see the class
     * javadoc). Cheap - no I/O - so callers may poll it.
     *
     * The stages are read UPSTREAM-FIRST: the pre-loader stages before
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
     * The stages BEFORE the audit loader, read UPSTREAM-FIRST: the runtime
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
     * rows every other FE reported. Throws IllegalStateException when the shared
     * table cannot be read or when the fence is INCOMPLETE (a live reporter's overdue row)
     * - the caller must NOT advance without a complete fence (an unreadable
     * follower row is exactly the hole this guards; added the overdue-live
     * reporter, whose last confirmed value may already be stale).
     */
    public static long clusterHorizon() {
        long oldest = localHorizon();
        return minPositive(oldest, remoteHorizon());
    }

    /**
     * The zones the CLUSTER's audit writers have RENDERED rows in - this FE's own live
     * history plus every fresh reporter row's registered zones. The SPM
     * capture must render a window pass in every one of them before completing the
     * window: rows stored under a zone that is no longer current are invisible to bounds
     * rendered in the current zone, and a zone change BETWEEN two capture cycles is
     * invisible to the capture's own start/end comparisons. A read failure fails closed
     * exactly like clusterHorizon().
     *
     * @return the zone IDs that may own audit rows
     */
    public static Set<String> clusterWriterZones() {
        Set<String> zones = new LinkedHashSet<>(AuditWriterZones.zones());
        zones.addAll(remoteWriterZones(false));
        return zones;
    }

    /**
     * As clusterWriterZones(), for a window that may REWIND to an earlier pending
     * checkpoint: every zone the reporting FEs ever registered, INCLUDING rows the
     * forward filter (see remoteWriterZones) already considers covered / expired.
     * A zone retired from the forward set can still own a row of a pending window
     * that surfaces late (an unreadable checkpoint row is invisible to the rewind-floor
     * read), and that window must still be scanned in the zone before it completes.
     *
     * @return the zone IDs a rewindable window may have rendered rows in
     */
    public static Set<String> clusterWriterZonesForRewind() {
        Set<String> zones = new LinkedHashSet<>(AuditWriterZones.zonesForRewind());
        zones.addAll(remoteWriterZones(true));
        return zones;
    }

    /**
     * The zones registered in the rows of the shared table. A row's zones stay REQUIRED
     * until the DURABLE CAPTURE PROGRESS has passed the row's last refresh:
     * a follower can publish a row under a zone and then stop reporting (crash, stalled
     * keepalive) while an uncompleted capture window still contains that row - dropping
     * its zone here let a UTC leader exhaust the window scanning only its own zones and
     * checkpoint past the row stored under e.g. -05:00. Fresh rows are always included;
     * an unreadable capture watermark keeps every zone (fail closed).
     */
    private static Set<String> remoteWriterZones(boolean forRewind) {
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
                                        ? Long.parseLong(values.get(4).trim()) : 0L,
                                values.size() > 5 && values.get(5) != null
                                        ? values.get(5).trim() : ""});
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
            if (!forRewind && now - updatedAt > ROW_STALE_MILLIS
                    && updatedAt < coveredThrough) {
                // an expired row whose last render is COVERED by durable capture
                // progress: no uncompleted FORWARD window can still contain its rows.
                // A rewindable window is different: an earlier pending checkpoint that
                // surfaces later may reach back BELOW that floor (the floor read cannot
                // see an unreadable pending row), so the rewind set keeps every row's
                // zones until the row itself disappears.
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
     * IllegalStateException (see reportingFeAlive).
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
                                        ? Long.parseLong(values.get(4).trim()) : 0L,
                                values.size() > 5 && values.get(5) != null
                                        ? values.get(5).trim() : ""});
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
            String fenceLabels = row.length > 5 && row[5] != null ? (String) row[5] : "";
            // The ROW's earliest obligation is the MINIMUM of its horizon and its
            // committed fence: both are lower bounds on "events that may
            // still be missing", so the earlier one fences. (The reporter's write already
            // folds them; a row written by an older build can still carry a horizon that
            // OVERSTATES the committed fence, and taking the max kept that overstatement.)
            long fence = minPositive(horizon, committedFence);
            boolean overdue = updatedAt <= 0 || now - updatedAt > ROW_STALE_MILLIS;
            if (overdue) {
                // An OVERDUE row does NOT mean its FE is gone - its keepalive
                // upserts can fail for minutes while the FE still holds the events, and
                // its pipeline may even have GAINED events with older start times - so the
                // stale VALUE cannot be trusted either. Only a KNOWN-GONE FE releases its
                // fence (the events died with it); a live - or an undecidable - reporter
                // fails this read closed, and the capture skips the cycle and retries
                // promptly instead of checkpointing past the unread fence. This check runs
                // BEFORE the zero-fence shortcut: an overdue ZERO row of a
                // live FE is just as untrustworthy as a positive one - the FE may have
                // captured events since its last report, and trusting the stale zero let
                // the capture advance past them.
                Boolean alive = reportingFeAlive(feName);
                if (Boolean.FALSE.equals(alive)) {
                    if (committedFence > 0
                            && !committedFenceSettled(fenceLabels, updatedAt, now)) {
                        // The batch is COMMITTED, so its rows can become
                        // readable even though the FE is dead.: the fence
                        // survives on the TRANSACTION's outcome, not on the age bound -
                        // the labels the row carries resolve each batch's state, and
                        // only terminal (VISIBLE / ABORTED) transactions release it. A
                        // dead FE cannot re-report, so the age bound alone released the
                        // only marker that protected a publication arriving just after
                        // it; it remains the last resort for an UNRESOLVABLE label only.
                        LOG.warn("audit publication horizon: keeping the COMMITTED fence"
                                        + " {} of FE {} (not refreshed for {} ms) although the"
                                        + " FE is gone: the committed batch may still publish",
                                committedFence, feName, now - updatedAt);
                        oldest = minPositive(oldest, committedFence);
                        continue;
                    }
                    LOG.warn("audit publication horizon: dropping the overdue fence of FE"
                            + " {} (oldest event {}, not refreshed for {} ms): the FE is"
                            + " gone and its committed fence is settled or beyond every"
                            + " resolution bound", feName, horizon, now - updatedAt);
                    continue;
                }
                throw new IllegalStateException("SPM capture cannot trust the cluster audit"
                        + " publication horizon: the fence of FE " + feName + " (oldest event"
                        + " " + horizon + ") has not been refreshed for " + (now - updatedAt)
                        + " ms while the FE is " + (alive == null ? "not confirmably gone"
                        : "still alive") + "; the capture must retry on a later cycle");
            }
            if (fence <= 0) {
                // An IDLE row is NOT "no fence". It proves only that its FE
                // had nothing outstanding at `updatedAt`; whatever was captured AFTER that
                // instant may still be unpublished (the next report tick is up to a few
                // seconds away, a stuck load far longer). Contributing the row's report
                // INSTANT keeps every window that reaches past it under the re-scan
                // overlap until that later event is itself reported - skipping the row
                // outright let the capture checkpoint past an event the idle report never
                // vouched for.
                oldest = minPositive(oldest, updatedAt);
                continue;
            }
            oldest = minPositive(oldest, fence);
        }
        // The row set must cover every LIVE audit-producing FE. A live
        // follower whose first report failed holds no row even though it can carry a
        // committed, unreadable batch; interpreting that gap as a zero horizon let a
        // capture window checkpoint past the row that publishes later. Idle FEs register
        // a zero row (see reportLocalHorizon), so ABSENCE means "never reported / not
        // visible" - fail closed and retry promptly instead.
        verifyEveryLiveReporterRegistered(rows);
        return oldest;
    }

    /**
     * Verifies that every ALIVE frontend (other than this FE, whose own pipeline is
     * covered by localHorizon) has a row in the shared table.
     * Without a live membership view / a seam the requirement cannot be verified and is
     * skipped (the shared table is not authoritative in that state either).
     *
     * @param rows the rows read from the shared table
     * @throws IllegalStateException when a live FE has not registered yet (retryable)
     */
    private static void verifyEveryLiveReporterRegistered(List<Object[]> rows) {
        Set<String> expected = liveReporterNames();
        if (expected == null || expected.isEmpty()) {
            return;
        }
        Set<String> registered = new LinkedHashSet<>();
        for (Object[] row : rows) {
            if (row != null && row.length >= 1 && row[0] != null) {
                registered.add(((String) row[0]).trim());
            }
        }
        for (String feName : expected) {
            if (!registered.contains(feName)) {
                throw new IllegalStateException("SPM capture cannot trust the cluster audit"
                        + " publication horizon: FE " + feName + " is alive but has not"
                        + " registered its fence yet (a committed batch may still be"
                        + " unreadable); the capture must retry on a later cycle");
            }
        }
    }

    /**
     * The names of the alive FEs that run an audit loader, EXCLUDING this FE (whose
     * obligations fold in through localHorizon()), or null when the membership
     * cannot be enumerated.
     */
    private static Set<String> liveReporterNames() {
        Supplier<Set<String>> seam = reporterNamesForTest;
        if (seam != null) {
            return seam.get();
        }
        try {
            Env env = Env.getCurrentEnv();
            if (env == null) {
                return null;
            }
            List<Frontend> frontends = env.getFrontends(null);
            if (frontends == null || frontends.isEmpty()) {
                return null; // no membership view (not the leader / not ready)
            }
            String self = AuditLoader.selfFeName();
            Set<String> names = new LinkedHashSet<>();
            for (Frontend frontend : frontends) {
                // MEMBERSHIP decides who must have registered - NOT the
                // heartbeat flag. `isAlive()` is false whenever the last heartbeat or an
                // RPC failed, which is exactly the state in which a still-running FE's
                // row (and its committed batch) is missing from the table: excluding it
                // here read the not-yet-registered gap as "no obligation", the very
                // failure exists to catch. A member with a failed heartbeat
                // is simply a reporter whose row is (still) overdue - handled by the
                // per-row liveness logic, not by dropping it from the obligation set.
                if (frontend.getNodeName() != null
                        && !frontend.getNodeName().equals(self)) {
                    names.add(frontend.getNodeName());
                }
            }
            return names;
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Whether the FE that reported a fence row can still publish something: null
     * when that cannot be decided (no live environment / no membership view / a failed
     * lookup), and false ONLY when the FE is provably gone - it is no longer a
     * member of the cluster. Called for OVERDUE rows only: the capture runs on the
     * leader, whose frontend list tracks every member, so a row whose FE is absent from
     * the membership is a leftover whose events died with that FE (see
     * remoteHorizon).
     *
     * A MEMBER whose isAlive is false is still alive enough to
     * hold events - the flag drops on a single failed heartbeat / RPC while the process
     * keeps running - so it must NOT release its fence. Only absence from the membership
     * (dropped / decommissioned / replaced) proves the events died with the FE.
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
                    return Boolean.TRUE; // still a member: do NOT trust its stale value
                }
            }
            return Boolean.FALSE; // not a member any more: dropped / decommissioned
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Whether the committed fence of a PROVABLY GONE FE is SETTLED: every
     * batch the row lists is either TERMINAL (VISIBLE - its rows are readable; ABORTED -
     * it can never publish) or unresolvable with the retention bound elapsed (the same
     * last resort the live loader applies, a label the transaction
     * manager does not know - the request never got as far as creating a transaction -
     * cannot be proven lost, so it keeps fencing until the bound). A batch still
     * COMMITTED / PRECOMMITTED keeps its fence REGARDLESS OF AGE: the publish daemon can
     * make its rows readable at any moment, and a dead FE can no longer re-report, so
     * releasing the marker on the age bound alone lost exactly the
     * publication that arrived just after it.
     *
     * While ANY listed batch keeps fencing, the row contributes its full
     * committed_fence_ms (the MINIMUM over all its batches, settled ones
     * included): the resolution has no per-batch event times, and over-fencing merely
     * delays the capture while under-fencing would skip an event.
     *
     * @param labelsCsv the row's committed-fence labels (oldest first, "-" = unknown
     *                  identity), "" for a row written before the column existed
     * @param updatedAt the row's last refresh (epoch millis)
     * @param now       the read instant (epoch millis)
     * @return true when no listed batch can still publish
     */
    private static boolean committedFenceSettled(String labelsCsv, long updatedAt, long now) {
        if (labelsCsv == null || labelsCsv.trim().isEmpty()) {
            // No resolvable label (a row written before the column, or by a loader that
            // never learned its labels): the retention bound stays the last resort
            return now - updatedAt > COMMITTED_FENCE_SURVIVAL_MILLIS;
        }
        for (String label : labelsCsv.split(";", -1)) {
            String trimmed = label.trim();
            if (AuditLoader.OVERFLOWED_FENCE_LABEL.equals(trimmed)) {
                // The marker stands for at least one dropped-publish-timeout batch with
                // NO resolvable identity: nothing can ever PROVE its publication, so the
                // fence is never settled by age (the previous bounded release assumed
                // those batches lost, but a COMMITTED one may still publish - the
                // reviewer's C8 finding). The loader no longer emits the sentinel
                // (identities are never dropped from the aggregate), so this branch only
                // keeps rows written by an intermediate build fenced.
                return false;
            }
            if (trimmed.isEmpty() || "-".equals(trimmed)) {
                // no identity was recorded: a label lookup can NEVER settle it, and the
                // batch may be COMMITTED with its publication pending - keep fencing
                // instead of releasing on age alone (the reviewer's C8 finding)
                return false;
            }
            String status = AuditLoader.transactionStatusForLabel(trimmed);
            if (AuditLoader.isTerminalTransactionStatus(status)) {
                continue; // VISIBLE / ABORTED: this batch is settled
            }
            if ("COMMITTED".equals(status) || "PRECOMMITTED".equals(status)) {
                return false; // the publish daemon may still make its rows readable
            }
            // unresolvable (a lookup failure / a not-yet-known label): keep fencing -
            // only a PROVABLE terminal state may settle the row's share
            return false;
        }
        return true;
    }

    /**
     * Reads THIS FE's own shared row ONCE per process, before the first report, and
     * merges what the restarted process cannot know any more: the previous
     * incarnation's unresolved committed fence (with its labels) and its writer-zone
     * registry. The row is keyed by the stable fe_name, so it IS this FE's state, and a
     * restart does not retire it - empty process memory is not a resolution. A read
     * failure leaves the restore pending (retried on the next tick); an absent row
     * means there is nothing to carry.
     */
    private static void restoreCarriedPublicationState() {
        if (carriedStateRestored) {
            return;
        }
        // An UNAVAILABLE read (the environment is still starting, a failed SELECT) is
        // PENDING, not "nothing to carry": the previous incarnation's row may hold an
        // unresolved COMMITTED fence / writer zones, and both the first report and the
        // graceful close must wait for the restore to complete before they replace or
        // delete that row (see reportLocalHorizonLocked / clearLocalReportLocked).
        List<Object[]> ownRows = readOwnRowsForRestore();
        if (ownRows == null) {
            return; // unreadable: retry on the next report tick
        }
        carriedStateRestored = true;
        if (ownRows.isEmpty()) {
            return; // no row: nothing to carry
        }
        Object[] row = ownRows.get(0);
        if (row == null || row.length < 5 || row[1] == null) {
            return;
        }
        long updatedAt = ((Number) row[1]).longValue();
        // The zones traveled in a CONFIRMED report of the previous incarnation and are
        // re-registered (see AuditWriterZones#restore): without them the first zero /
        // empty report of this process scans future windows in the CURRENT zone only,
        // while an older row rendered under another zone remains invisible.
        AuditWriterZones.restore(AuditWriterZones.decodePairs((String) row[2]));
        long fence = ((Number) row[3]).longValue();
        if (fence <= 0) {
            return;
        }
        carriedCommittedFence = fence;
        carriedCommittedFenceLabels = row[4] == null ? "" : (String) row[4];
        // An unparsable update_time must not make the fence look ancient: aged from the
        // restart it keeps its full survival window (fail closed).
        carriedCommittedFenceUpdatedAt = updatedAt > 0 ? updatedAt : System.currentTimeMillis();
        LOG.info("audit publication horizon: carrying the committed fence {} of the previous"
                        + " incarnation (labels '{}') until its transactions are resolved",
                fence, carriedCommittedFenceLabels);
    }

    /**
     * The carried fence while it is still UNRESOLVED: 0 and cleared once every listed
     * transaction is terminal / beyond the bound. It is aged by the PREVIOUS row's last
     * refresh, exactly like the reader ages it.
     */
    private static long liveCarriedCommittedFence(long now) {
        if (carriedCommittedFence <= 0) {
            return 0;
        }
        if (committedFenceSettled(carriedCommittedFenceLabels, carriedCommittedFenceUpdatedAt,
                now)) {
            LOG.info("audit publication horizon: the committed fence {} carried from the"
                            + " previous incarnation is settled (labels '{}'): releasing it",
                    carriedCommittedFence, carriedCommittedFenceLabels);
            carriedCommittedFence = 0;
            carriedCommittedFenceLabels = "";
            carriedCommittedFenceUpdatedAt = 0;
            return 0;
        }
        return carriedCommittedFence;
    }

    /** Concatenates two ';'-joined fence-label lists, skipping blank members. */
    private static String mergeFenceLabels(String first, String second) {
        if (first == null || first.trim().isEmpty()) {
            return second == null ? "" : second.trim();
        }
        if (second == null || second.trim().isEmpty()) {
            return first.trim();
        }
        return first.trim() + ";" + second.trim();
    }

    /**
     * THIS FE's own shared row for the startup merge (see
     * restoreCarriedPublicationState): null when the read FAILED (retry), otherwise the
     * parsed row(s) in the seam shape - horizon, update_time (millis), writer_zones,
     * committed_fence_ms, committed_fence_labels.
     */
    private static List<Object[]> readOwnRowsForRestore() {
        Supplier<List<Object[]>> seam = ownRowRestoreReaderForTest;
        if (seam != null) {
            return seam.get(); // null = unavailable (pending), like a failed SELECT
        }
        if (!sharedTableAvailable()) {
            // The environment is not ready yet (the reporter starts during
            // Env.initialize, before the internal table can be read): the read is
            // UNAVAILABLE, so the restore stays pending. Treating it as "no row" marked
            // the restore complete and the first post-readiness idle report replaced the
            // previous incarnation's confirmed COMMITTED fence / writer zones with zeros.
            return null;
        }
        try {
            Map<String, String> params = new HashMap<>();
            params.put("feName", StatisticsUtil.escapeSQL(AuditLoader.selfFeName()));
            List<ResultRow> result = StatisticsUtil.executeQuery(SELECT_OWN_ROW_SQL, params,
                    IO_TIMEOUT_SECONDS);
            List<Object[]> rows = new ArrayList<>();
            if (result != null) {
                for (ResultRow resultRow : result) {
                    List<String> values = resultRow.getValues();
                    if (values == null || values.size() < 5) {
                        continue;
                    }
                    rows.add(new Object[] {
                            parseLongOrZero(values.get(0)),
                            parseUpdateTimeOrZero(values.get(4)),
                            values.get(1) == null ? "" : values.get(1),
                            parseLongOrZero(values.get(2)),
                            values.get(3) == null ? "" : values.get(3).trim()});
                }
            }
            return rows;
        } catch (Exception e) {
            LOG.warn("audit publication horizon: cannot read this FE's own row for the"
                    + " restart merge: {}", e.getMessage());
            return null; // retry on the next tick
        }
    }

    /** One numeric cell of the own-row restore read (blank / NULL = 0). */
    private static long parseLongOrZero(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /** The own row's update_time in epoch millis (an unparsable rendering = 0). */
    private static long parseUpdateTimeOrZero(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0L;
        }
        try {
            return parseUpdateTime(text.trim());
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    /**
     * Publishes THIS FE's current horizon into the shared table (one row per FE) and
     * returns whether the written state is CONFIRMED readable from it. Called by the
     * audit loader's reporter thread on change and on its keepalive cadence; the caller
     * may remember the value as reported only when this returns true, so a failed (or
     * not yet visible) write is retried on the next tick instead of being treated as
     * done until the 60s keepalive (SQL OK can still leave a COMMITTED
     * INSERT unpublished, and the previous void return silently swallowed failures the
     * reporter had already recorded as reported).
     *
     * Each report is ONE atomic statement: a merge-on-write upsert of
     * the FE's row for a positive horizon, a single DELETE for zero. A reader can never
     * observe the row missing while it is being refreshed by the old value.
     *
     * The whole snapshot-to-write sequence runs under the loader's fence monitor (see
     * AuditLoader#withPublicationFenceLock): a batch retained between the fence snapshot
     * and the UPSERT (the load thread can durably report the batch's own label before
     * sending it) would otherwise be written over by this older report.
     *
     * @param horizon the local horizon (0 = nothing outstanding: the row is deleted
     *                unless the WRITER-ZONE set or a committed publish fence keeps it)
     * @return whether the written state is confirmed visible in the shared table
     */
    public static boolean reportLocalHorizon(long horizon) {
        return AuditLoader.withPublicationFenceLock(() -> reportLocalHorizonLocked(horizon));
    }

    /** The body of reportLocalHorizon, run with the publication-fence monitor held. */
    private static boolean reportLocalHorizonLocked(long horizon) {
        // A COMMITTED-but-unreadable batch must survive this FE's death:
        // the fence is folded in HERE, at write time, so even a report computed before the
        // batch timed out (a stale zero, or the close path's clear) cannot DELETE or
        // understate it - this is what keeps the crash/close gap closed.: the
        // batches' load LABELS travel with the fence, so a reader that finds the FE gone
        // can resolve each transaction's outcome instead of expiring the marker on the
        // age bound alone.
        long committedFence = AuditLoader.oldestCommittedPublishFenceEventTime();
        String committedLabels = AuditLoader.oldestCommittedPublishFenceLabels();
        // The PREVIOUS incarnation's unresolved fence and zones are merged in before
        // the first write of this process (see restoreCarriedPublicationState): the
        // restarted process has empty in-memory fences and zones, and its first idle
        // report must not retire obligations the shared row is the only copy of.
        long now = System.currentTimeMillis();
        restoreCarriedPublicationState();
        if (!carriedStateRestored) {
            // The previous incarnation's row could not be read YET (the environment is
            // still starting, or the SELECT failed transiently): replacing it now would
            // overwrite obligations this process cannot see - a previous COMMITTED fence
            // with zeros, or its writer-zone record with an empty set. Report nothing;
            // the reporter retries on its next tick.
            LOG.info("audit publication horizon: the previous incarnation's row is not"
                    + " readable yet; the report waits so it cannot overwrite it");
            return false;
        }
        long carriedFence = liveCarriedCommittedFence(now);
        if (carriedFence > 0) {
            if (committedFence <= 0) {
                committedFence = carriedFence;
                committedLabels = carriedCommittedFenceLabels;
            } else {
                // both fence - keep the EARLIER value and BOTH label sets: the reader
                // releases the fence only once every listed transaction is terminal
                committedFence = minPositive(committedFence, carriedFence);
                committedLabels = mergeFenceLabels(carriedCommittedFenceLabels,
                        committedLabels);
            }
        }
        // EVERY locally known obligation is folded with minPositive: the
        // caller's value can be a PARTIAL report (AuditLoader.reportCommittedFence passes
        // only the batch fence), and taking the MAX of (value, committed fence) OVERSTATED
        // the shared horizon whenever an OLDER event was still upstream - a queued 09:55
        // event next to a 10:00 committed batch reported 10:00, a capture window then
        // opened at 10:00, and the 09:55 row could be checkpointed past once it published.
        // The minimum of the full local pipeline, the caller's value and the committed
        // fence is the EARLIEST event this FE can still owe; 0 means "none" and is
        // ignored, so an idle report still reduces to the committed fence / zero.
        long effectiveHorizon = minPositive(
                minPositive(horizon, localHorizon()), committedFence);
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
            // the writer-zone snapshot travels with every report, and a
            // ZERO-horizon report KEEPS the row while zones are registered: the capture
            // still needs them for windows it has not completed, so deleting the row
            // would drop exactly that knowledge
            String writerZones = AuditWriterZones.encode();
            // ALWAYS upsert the row - a ZERO-horizon report is an EXPLICIT IDLE
            // REGISTRATION: the cluster check treats a live FE without a
            // row as an INCOMPLETE horizon (its first report may have failed while it
            // holds a committed, unreadable batch), so absence must mean "never
            // reported / not visible", never "nothing owed". The clean-close path
            // de-registers explicitly via clearLocalReport().
            params.put("horizonMs", String.valueOf(effectiveHorizon));
            params.put("updateTime", renderUpdateTime(System.currentTimeMillis()));
            params.put("writerZones", StatisticsUtil.escapeSQL(writerZones));
            params.put("committedFenceMs", String.valueOf(committedFence));
            params.put("committedFenceLabels", StatisticsUtil.escapeSQL(committedLabels));
            StatisticsUtil.execUpdate(UPSERT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
            boolean confirmed = ownRowConfirms(feName, effectiveHorizon, writerZones,
                    committedFence, committedLabels);
            if (confirmed) {
                // Only a CONFIRMED report makes the zones known to the capture
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
     * De-registers this FE's shared row (graceful shutdown) UNLESS the row still carries
     * state that only it can hold. A pending COMMITTED-but-unreadable batch keeps the row
     * (its rows can publish after this FE stops, so its fence must survive it - the fold
     * at write time keeps the row whenever the committed fence is positive), and the
     * WRITER-ZONE record stays until resolved capture progress covers its last use: a
     * follower can publish a row rendered in -05:00, drain and close with no pending
     * committed batch, and deleting the row would leave the leader's process-local
     * registry with UTC only - capture checkpoints past the visible -05:00 row without a
     * pass that can find it.
     *
     * The check and the re-report run under the fence monitor (see reportLocalHorizon),
     * so a batch retained concurrently cannot slip between the check and the write.
     */
    public static void clearLocalReport() {
        AuditLoader.withPublicationFenceLock(() -> {
            clearLocalReportLocked();
            return Boolean.TRUE;
        });
    }

    /** The body of clearLocalReport, run with the publication-fence monitor held. */
    private static void clearLocalReportLocked() {
        long now = System.currentTimeMillis();
        restoreCarriedPublicationState();
        if (!carriedStateRestored) {
            // Deleting the row before the restore completed would destroy the previous
            // incarnation's committed fence / writer zones, which this process has not
            // read yet. Keep the row; the close waits one more tick (the reporter and the
            // close share the fence monitor, so a later clear sees the completed state).
            LOG.info("audit publication horizon: the previous incarnation's row is not"
                    + " readable yet; the close keeps it instead of de-registering");
            return;
        }
        long committedFence = AuditLoader.oldestCommittedPublishFenceEventTime();
        long carriedFence = liveCarriedCommittedFence(now);
        if (committedFence > 0 || carriedFence > 0
                || AuditWriterZones.anyZoneNeedingCoverage()) {
            // The row is the only durable copy of these obligations: an explicit zero
            // re-report refreshes it (and re-carries the zones) instead of deleting it.
            reportLocalHorizonLocked(0L);
            return;
        }
        if (!sharedTableAvailable()) {
            return;
        }
        try {
            Map<String, String> params = new HashMap<>();
            params.put("feName", StatisticsUtil.escapeSQL(AuditLoader.selfFeName()));
            StatisticsUtil.execUpdate(DELETE_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
        } catch (Exception e) {
            LOG.warn("audit publication horizon: cannot de-register this FE's row: {}",
                    e.getMessage());
        }
    }

    /**
     * Re-reads THIS FE's row and checks it matches what was just written: the horizon
     * value, the WRITER-ZONE SET (a zero-horizon report carrying a NEW zone
     * set is indistinguishable from the old row by the horizon alone - if the UPSERT
     * committed without being readable, the reporter would record the new zones as
     * reported and the capture could checkpoint in the gap without scanning the new
     * zone), the COMMITTED fence (an unreadable marker would let a crash
     * drop the fence) and its LABELS (an unreadable labels write would leave
     * a dead FE's fence unresolvable, which is exactly the age-bound release this column
     * exists to replace). A read failure is an UNCONFIRMED write - the caller retries.
     */
    private static boolean ownRowConfirms(String feName, long horizon, String writerZones,
            long committedFenceMs, String committedLabels) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("feName", StatisticsUtil.escapeSQL(feName));
        List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_OWN_ROW_SQL, params, IO_TIMEOUT_SECONDS);
        long readBack = 0;
        String readBackZones = "";
        long readBackCommitted = 0;
        String readBackLabels = "";
        boolean sawRow = rows != null && !rows.isEmpty();
        if (sawRow) {
            List<String> values = rows.get(0).getValues();
            if (values != null && !values.isEmpty()) {
                readBack = Long.parseLong(values.get(0).trim());
                readBackZones = values.size() > 1 && values.get(1) != null ? values.get(1) : "";
                readBackCommitted = values.size() > 2 && values.get(2) != null
                        && !values.get(2).trim().isEmpty()
                        ? Long.parseLong(values.get(2).trim()) : 0L;
                readBackLabels = values.size() > 3 && values.get(3) != null
                        ? values.get(3).trim() : "";
            }
        }
        if (horizon > 0) {
            return readBack == horizon && readBackCommitted == committedFenceMs
                    && sameText(readBackZones, writerZones)
                    && sameText(readBackLabels, committedLabels);
        }
        // An IDLE REGISTRATION must be VISIBLE: the cluster check reads
        // the row's ABSENCE as "the FE never registered", so a zero report is confirmed
        // only once the row itself can be read back - an unconfirmed zero would stop the
        // reporter's retries while the leader keeps failing the cycle closed.
        return sawRow && readBack <= 0 && readBackCommitted <= 0
                && sameText(readBackZones, writerZones)
                && sameText(readBackLabels, committedLabels);
    }

    /** Zone-set / label comparison of a read-back against the attempted report. */
    private static boolean sameText(String readBack, String attempted) {
        return (readBack == null ? "" : readBack.trim())
                .equals(attempted == null ? "" : attempted.trim());
    }

    /**
     * Renders the shared row's update_time in the FIXED UTC zone: the
     * column is zone-less, so both the write and the read pin the same explicit zone
     * instead of each FE's local one.
     */
    @VisibleForTesting
    static String renderUpdateTime(long epochMillis) {
        return UPDATE_TIME_UTC_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
    }

    /** Parses the shared row's update_time rendering (see renderUpdateTime). */
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

    /**
     * For tests: clears every seam AND the carried previous-incarnation state, so one
     * test's fake row cannot leak into the next (restore runs only once per process).
     */
    @VisibleForTesting
    static void resetForTest() {
        horizonRowsReaderForTest = null;
        localHorizonWriterForTest = null;
        reporterNamesForTest = null;
        feAliveProbeForTest = null;
        ownRowRestoreReaderForTest = null;
        carriedCommittedFence = 0;
        carriedCommittedFenceLabels = "";
        carriedCommittedFenceUpdatedAt = 0;
        carriedStateRestored = false;
    }
}
