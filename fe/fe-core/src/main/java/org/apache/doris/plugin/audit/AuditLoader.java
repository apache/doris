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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.transaction.TransactionState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoader extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoader.class);

    public static final String AUDIT_LOG_TABLE = "audit_log";

    /**
     * How long the load worker sleeps between queue polls (millis). A queued event can
     * therefore stay visible to the loader this long after it was released by the audit
     * event pipeline - together with WorkloadRuntimeStatusMgr's hold
     * (query_audit_log_timeout_ms / be_report_query_statistics_timeout_ms)
     * and the batch interval this is part of the upstream publication delay the SPM
     * capture overlap must cover (see PlanCaptureManager#scanWindowOverlapMs).
     */
    public static final long QUEUE_POLL_INTERVAL_MILLIS = 5_000L;

    // the "0x1F" and "0x1E" are used to separate columns and lines in audit log data
    public static final char AUDIT_TABLE_COL_SEPARATOR = 0x1F;
    public static final char AUDIT_TABLE_LINE_DELIMITER = 0x1E;
    // the "\\x1F" and "\\x1E" are used to specified column and line delimiter in stream load request
    // which is corresponding to the "\\u001F" and "\\u001E" in audit log data.
    public static final String AUDIT_TABLE_COL_SEPARATOR_STR = "\\x1F";
    public static final String AUDIT_TABLE_LINE_DELIMITER_STR = "\\x1E";

    /**
     * The FE's running builtin loader, published for readers that must know how far this
     * FE's audit events have actually been PUBLISHED: the SPM capture advances its scan
     * watermark from audit_log, and a row that is still inside this loader (queued,
     * or assembled into the not yet flushed batch) is not readable there yet - advancing
     * past it would drop the query permanently (see
     * PlanCaptureManager#scanWindowOverlapMs).
     */
    private static volatile AuditLoader runningLoader;

    private StringBuilder auditLogBuffer = new StringBuilder();
    private int auditLogNum = 0;
    private long lastLoadTimeAuditLog = 0;
    // start time of the oldest event the current, NOT YET LOADED batch holds (0 = empty).
    // written by the assembling thread (AuditEventProcessor) under the loader monitor,
    // read by the SPM capture thread - volatile, and only ever narrowed while a batch is
    // being assembled, so a stale read can only under-, never over-state the horizon.
    private volatile long batchOldestEventTime = 0;
    // query id of the event that set batchOldestEventTime: the sample row a pending
    // publish is probed with (see pendingPublishFences).
    private String batchOldestQueryId = "";
    // The zone the sample row's time column was RENDERED in (see fillLogBuffer):
    // the zone-usage registry keeps only the LAST use per zone, so re-deriving the
    // zone from the event time later can pick a zone that was in effect at that
    // instant but is NOT the one the row was written with (a `SET GLOBAL time_zone`
    // switches the registry forward, and the derivation then matches the NEW zone).
    // The retained publish fence would probe the row under the wrong zone and never
    // see it publish, fencing the capture progress until the bound expired.
    private String batchOldestZoneId = "";
    /**
     * Fences of the batches whose stream loads reported Publish Timeout (or failed
     * ambiguously): every such transaction is COMMITTED but its rows are not readable
     * yet, so the batch must KEEP fencing progress until publication is confirmed
     * . EVERY pending batch is retained as its OWN entry:
     * keeping only the oldest batch's sample released the whole fence the moment that
     * sample became visible, although a NEWER batch B could still be committed and
     * unreadable - the capture then checkpointed past B, and once the watermark moved,
     * later windows could never reach B's rows. Each entry carries its own sample row
     * (queryId) and its own since bound (see
     * PUBLISH_FENCE_MAX_MILLIS). Guarded by the loader monitor.
     */
    private final List<PublishFence> pendingPublishFences = new ArrayList<>();

    /**
     * The AGGREGATE fence of the batches dropped by the MAX_PENDING_PUBLISH_FENCES
     * bound: the earliest event time among them plus the deadline of the
     * batch dropped LATEST. Removing the oldest entry outright released its
     * fence without ANY visibility probe - a sustained Publish-Timeout rate then let the
     * capture advance past an event that was still committed-but-unreadable, and the
     * pinned window can never widen back to include it. The aggregate must outlive EVERY
     * member's OWN retention window: expiring with the FIRST dropped batch's deadline
     * released a batch that overflowed at minute 29 already at minute 30, before its own
     * 30-minute bound and without any visibility proof. Guarded by the loader monitor.
     */
    private long droppedPublishFenceTime = 0;
    private long droppedPublishFenceUntil = 0;

    /**
     * The load labels of the aggregated fences, OLDEST first ("-" = no identity was ever
     * allocated). The aggregate's event time alone does not settle it: the dead-FE
     * settlement resolves every contributing transaction by label, so a COMMITTED
     * overflowed batch whose label was dropped from here could be read as settled once
     * the SURVIVING labels turn terminal - the capture would then checkpoint past a row
     * that still publishes. Bounded by MAX_AGGREGATED_FENCE_LABELS; at capacity the
     * OLDEST identities are resolved first so a provably settled one frees its slot, and
     * only an identity that still cannot be resolved keeps fencing through
     * OVERFLOWED_FENCE_LABEL (never expired by age).
     * Guarded by the loader monitor.
     */
    private final List<String> droppedPublishFenceLabels = new ArrayList<>();

    /**
     * The marker of an aggregated batch whose identity did NOT fit the label budget: the
     * reader cannot resolve it, so it keeps the shared fence fenced instead of expiring
     * it on the row's age alone (see AuditPublicationHorizon#committedFenceSettled).
     */
    static final String OVERFLOWED_FENCE_LABEL = "*";

    /** Whether an aggregated label was lost to the bound (see droppedPublishFenceLabels). */
    private boolean droppedFenceLabelsOverflowed = false;

    /** One committed-but-unreadable batch (see pendingPublishFences). */
    private static final class PublishFence {
        final long oldestEventTime;
        final String queryId;
        final long since;
        /**
         * The load label of the batch's transaction: the terminal state
         * (ABORTED / VISIBLE) is resolved by label once the retention bound elapsed, so
         * the fence is released on an OUTCOME, never on elapsed time alone. Empty when
         * unknown (the fence then keeps fencing until the sample row is readable).
         */
        final String label;
        /**
         * The zone the sample row's time column was RENDERED in:
         * the visibility probe must ask for that audit time, not for a rendering in the
         * current global zone - after a `SET GLOBAL time_zone` the probe used to look for
         * a wall clock hours away from the stored one and could never confirm a row that
         * was perfectly visible. Null / empty = fall back to the default rendering.
         */
        final String writerZoneId;

        PublishFence(long oldestEventTime, String queryId, long since, String label,
                String writerZoneId) {
            this.oldestEventTime = oldestEventTime;
            this.queryId = queryId == null ? "" : queryId;
            this.since = since;
            this.label = label == null ? "" : label;
            this.writerZoneId = writerZoneId == null ? "" : writerZoneId;
        }
    }

    /**
     * How long a Publish-Timeout fence may hold progress without its sample row ever
     * becoming readable before it is released with a warning (see
     * pendingPublishFences).
     */
    public static final long PUBLISH_FENCE_MAX_MILLIS = 30 * 60 * 1000L;

    /**
     * Hard bound of pendingPublishFences: a sustained Publish-Timeout rate over
     * the whole fencing window would otherwise grow the list without limit. Beyond the
     * bound the OLDEST entry is released early with a warning (it is the one closest to
     * its own time bound anyway).
     */
    static final int MAX_PENDING_PUBLISH_FENCES = 256;

    /**
     * Hard bound of the labels carried for the aggregated fences (the shared row's
     * committed_fence_labels column). At capacity the OLDEST retained identities are
     * resolved and the terminal ones dropped first (see aggregateDroppedFence), so the
     * bound is only reached when this many transactions are SIMULTANEOUSLY unresolved;
     * the row itself already carries up to MAX_PENDING_PUBLISH_FENCES pending labels, so
     * the same order of magnitude fits the column. Beyond it the settlement sees the
     * overflow marker, which never settles by age.
     */
    static final int MAX_AGGREGATED_FENCE_LABELS = 256;

    /** How often the horizon reporter wakes up (it only writes on change / keepalive). */
    static final long HORIZON_REPORT_TICK_MILLIS = 5_000L;

    /**
     * How often an UNCHANGED, non-zero horizon is re-reported: the shared row must stay
     * fresh while a long hold persists, otherwise AuditPublicationHorizon treats
     * the FE as gone and ignores its fence.
     */
    public static final long HORIZON_KEEPALIVE_MILLIS = 60_000L;

    private static final String PUBLISH_PROBE_SQL = "SELECT `query_id` FROM `"
            + FeConstants.INTERNAL_DB_NAME + "`.`" + AUDIT_LOG_TABLE
            + "` WHERE `query_id` = '${queryId}' AND `time` >= '${eventTime}' LIMIT 1";
    private static final int PUBLISH_PROBE_TIMEOUT_SECONDS = 10;

    /**
     * Test seam: whether a Publish-Timeout batch's rows are readable yet. One call is ONE
     * probe attempt. Null in production (the real probe reads the audit table).
     */
    @VisibleForTesting
    interface PublishVisibilityProbe {
        boolean isVisible(long eventTime, String queryId);
    }

    @VisibleForTesting
    static volatile PublishVisibilityProbe publishVisibilityProbeForTest;

    /** Test seam: the clock of the publish-fence bookkeeping (null = the real clock). */
    @VisibleForTesting
    static volatile java.util.function.LongSupplier publishFenceClockForTest;

    /**
     * Test seam: the TERMINAL transaction status of a load label; null =
     * undecidable. One call resolves one fence's outcome; null in production (the real
     * resolver reads the transaction manager).
     */
    @VisibleForTesting
    static volatile java.util.function.Function<String, String> transactionStatusForTest;

    /**
     * Test seam: invoked at the START of every reportCommittedFence call, so a test can
     * count how many reports one path pays (null in production). The load worker must
     * report each batch's obligation EXACTLY ONCE - retaining the fence keeps it local,
     * and the confirmed report is the send gate.
     */
    @VisibleForTesting
    static volatile Runnable reportCommittedFenceHookForTest;

    /** The clock of the publish-fence bookkeeping (see publishFenceClockForTest). */
    private static long publishFenceNow() {
        java.util.function.LongSupplier clock = publishFenceClockForTest;
        return clock == null ? System.currentTimeMillis() : clock.getAsLong();
    }

    // sometimes the audit log may fail to load to doris, count it to observe.
    private long discardLogNum = 0;

    private BlockingQueue<AuditEvent> auditEventQueue;
    private AuditStreamLoader streamLoader;
    private Thread loadThread;
    private Thread horizonReporterThread;

    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    private final PluginInfo pluginInfo;

    public AuditLoader() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLoader", PluginType.AUDIT,
                "builtin audit loader, to load audit log to internal table", DigitalVersion.fromString("2.1.0"),
                DigitalVersion.fromString("1.8.31"), AuditLoader.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            this.lastLoadTimeAuditLog = System.currentTimeMillis();
            // make capacity large enough to avoid blocking.
            // and it will not be too large because the audit log will flush if num in queue is larger than
            // GlobalVariable.audit_plugin_max_batch_bytes.
            this.auditEventQueue = Queues.newLinkedBlockingDeque(100000);
            this.streamLoader = new AuditStreamLoader();
            this.loadThread = new Thread(new LoadWorker(), "audit loader thread");
            this.loadThread.start();
            // the cluster-wide publication fence: this FE's own horizon must be visible to
            // the leader that runs the SPM capture, or a follower's backlog stays
            // invisible to it (see AuditPublicationHorizon)
            this.horizonReporterThread = new Thread(new HorizonReporter(), "audit horizon reporter");
            this.horizonReporterThread.setDaemon(true);
            this.horizonReporterThread.start();

            isInit = true;
            runningLoader = this;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when closing the audit loader", e);
                }
            }
        }
        if (horizonReporterThread != null) {
            try {
                horizonReporterThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // A batch whose load reported Publish Timeout is COMMITTED but may become
        // readable only AFTER this FE stops: clearing the row
        // unconditionally would let the capture advance past its rows. reportLocalHorizon
        // reads the pending fence at WRITE time and KEEPS the row while one exists; with
        // no such batch the row is deleted so the capture does not wait for a gone FE
        // (never-sent events die with the FE and need no fence). runningLoader stays set
        // until after this final report - it is how the write path sees the fence.
        AuditPublicationHorizon.clearLocalReport();
        if (runningLoader == this) {
            runningLoader = null;
        }
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    public void exec(AuditEvent event) {
        if (!GlobalVariable.enableAuditLoader) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("builtin audit loader is disabled, discard current audit event");
            }
            return;
        }
        try {
            auditEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            ++discardLogNum;
            if (LOG.isDebugEnabled()) {
                LOG.debug("encounter exception when putting current audit batch, discard current audit event."
                        + " total discard num: {}", discardLogNum, e);
            }
        }
    }

    private synchronized void assembleAudit(AuditEvent event) {
        String writerZoneId = fillLogBuffer(event, auditLogBuffer);
        ++auditLogNum;
        long eventTime = event.timestamp;
        // INTERNAL events never fence progress: the capture only scans
        // is_internal = false rows, so tracking e.g. the horizon reporter's own INSERT
        // here would let it keep its FE's fence (and thereby its own writes) alive
        // forever on an idle FE.
        if (event.isInternal) {
            return;
        }
        if (eventTime > 0 && (batchOldestEventTime == 0 || eventTime < batchOldestEventTime)) {
            batchOldestEventTime = eventTime;
            // the sample row of a later publish probe (see publishFenceOldestEventTime)
            batchOldestQueryId = event.queryId == null ? "" : event.queryId;
            // ... and the zone THAT row was rendered in (see the field): the probe must
            // read it back under the same zone
            batchOldestZoneId = writerZoneId == null ? "" : writerZoneId;
        }
    }

    /**
     * Start time (epoch millis, the time column of audit_log) of the OLDEST
     * event this FE's builtin loader has accepted but not published yet - queued events plus
     * the assembled, not yet flushed batch. Returns 0 when the loader is not running (or has
     * nothing outstanding), i.e. when there is no known publication delay to retain.
     *
     * The SPM capture uses this as a progress fence: its next scan window must still start
     * at or before this instant, otherwise a row the local loader still owes (e.g. a query the
     * query_audit_log_timeout_ms hold released late, or one sitting behind a slow
     * stream load in the auditEventQueue) would fall behind the advanced watermark
     * and never be captured. INTERNAL events are excluded - the capture never scans them,
     * and the reporter's own SQL would otherwise fence itself.
     */
    public static long oldestUnpublishedEventTime() {
        AuditLoader loader = runningLoader;
        return loader == null ? 0L : loader.oldestOutstandingEventTime();
    }

    private long oldestOutstandingEventTime() {
        BlockingQueue<AuditEvent> queue = auditEventQueue;
        if (queue == null) {
            return 0L;
        }
        // The batch value and the queue are ONE state (an event moves from the queue into
        // the batch, see transferNextEvent): the read takes the same monitor as the
        // transfer and the batch reset, so it can never observe an event in NEITHER
        // structure. The previous unsynchronized read could: the worker polled an event
        // (gone from the queue) and the reader - between that poll and assembleAudit -
        // read the STALE batch value and an empty queue, reporting "nothing outstanding"
        // while an accepted event was still unpublished; the capture then advanced its
        // watermark past the row the loader eventually wrote.
        synchronized (this) {
            long oldest = batchOldestEventTime;
            // a batch whose load reported Publish Timeout is COMMITTED but still
            // unreadable: it keeps fencing until its rows are observed;
            // EVERY pending batch fences, so the oldest of them is the
            // value
            for (PublishFence fence : pendingPublishFences) {
                if (oldest == 0 || fence.oldestEventTime < oldest) {
                    oldest = fence.oldestEventTime;
                }
            }
            // the queue is drained FIFO, but the ENQUEUE order is not the event-time order
            // (the upstream hold releases events by completion, not by start), so every
            // queued event is examined. The queue is a weak-consistency view and the scan
            // is cheap next to a capture cycle; a concurrently dequeued event is simply no
            // longer outstanding.
            for (AuditEvent event : queue) {
                if (event == null || event.isInternal) {
                    // INTERNAL events can never be captured: fencing for
                    // them would only keep the reporter's own writes alive forever
                    continue;
                }
                long eventTime = event.timestamp;
                if (eventTime > 0 && (oldest == 0 || eventTime < oldest)) {
                    oldest = eventTime;
                }
            }
            // The bounded-out batches are accepted-but-unpublished too, so
            // they belong to the LOCAL horizon exactly like the batch / queue entries:
            // the shared-row report already folds the aggregate in as the committed fence
            // ( keeps it until its newest member's own deadline), but a
            // transient report failure must not let the local path understate the fence.
            return minPositive(oldest, liveDroppedPublishFence());
        }
    }

    /**
     * Appends one event's row to the batch buffer.
     *
     * @return the zone id this row's time column was rendered in (the caller records it
     *         for the batch's oldest row, see batchOldestZoneId)
     */
    private String fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        // should be same order as InternalSchema.AUDIT_SCHEMA

        // The zone this row's time column is RENDERED in, read ONCE and
        // used for BOTH the registration and the rendering: two
        // independent reads of the global time_zone could observe a
        // `SET GLOBAL time_zone` in between (and back before the next report), leaving
        // the row stored under a zone nobody had registered - a capture scanning only
        // the registered (UTC) rendering then checkpointed past it.
        String writerZoneId = AuditWriterZones.currentWriterZoneId();
        AuditWriterZones.note(writerZoneId, System.currentTimeMillis());

        // uuid and time
        appendField(logBuffer, event.queryId);
        logBuffer.append(TimeUtils.longToTimeStringWithms(event.timestamp, writerZoneId))
                .append(AUDIT_TABLE_COL_SEPARATOR);

        // cs info
        appendField(logBuffer, event.clientIp);
        appendField(logBuffer, event.user);
        appendField(logBuffer, event.feIp);

        // default ctl and db
        appendField(logBuffer, event.ctl);
        appendField(logBuffer, event.db);

        // query state
        appendField(logBuffer, event.state);
        logBuffer.append(event.errorCode).append(AUDIT_TABLE_COL_SEPARATOR);
        appendField(logBuffer, event.errorMessage);

        // execution info
        logBuffer.append(event.queryTime).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.queueTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.cpuTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.peakMemoryBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.returnRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.shuffleSendRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.shuffleSendBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.spillWriteBytesToLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.spillReadBytesFromLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytesFromLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytesFromRemoteStorage).append(AUDIT_TABLE_COL_SEPARATOR);

        // plan info
        logBuffer.append(event.parseTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        // planTimesMs / getMetaTimesMs / scheduleTimesMs are String columns (formatted timing
        // breakdowns), not numbers, so they must be sanitized too.
        appendField(logBuffer, event.planTimesMs);
        appendField(logBuffer, event.getMetaTimesMs);
        appendField(logBuffer, event.scheduleTimesMs);
        logBuffer.append(event.hitSqlCache ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isHandledInFe ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);

        // queried tables, views and m-views
        appendField(logBuffer, event.queriedTablesAndViews);
        appendField(logBuffer, event.chosenMViews);

        // variable and configs
        appendField(logBuffer, event.changedVariables);
        appendField(logBuffer, event.sqlMode);


        // type and digest
        appendField(logBuffer, event.stmtType);
        logBuffer.append(event.stmtId).append(AUDIT_TABLE_COL_SEPARATOR);
        appendField(logBuffer, event.sqlHash);
        appendField(logBuffer, event.sqlDigest);
        logBuffer.append(event.isQuery ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isNereids ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isInternal ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);

        // resource
        appendField(logBuffer, event.workloadGroup);
        appendField(logBuffer, event.cloudClusterName);

        // protocol
        appendField(logBuffer, event.protocol);

        // already trim the query in org.apache.doris.qe.AuditLogHelper#logAuditLog
        String stmt = event.stmt;
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive audit event with stmt: {}", stmt);
        }
        // stmt is the last (and only free-text) column; sanitize it too so a statement carrying
        // raw 0x1F/0x1E cannot truncate its own row and forge a following one.
        appendLastField(logBuffer, stmt);
        // the zone this row's time column was rendered in (see the caller): it identifies HOW
        // the row is read back by a later publish probe
        return writerZoneId;
    }

    /**
     * Append one string column to the delimiter-framed audit stream-load payload, followed by the
     * column separator. The value is sanitized first so that user-controlled text (SQL statement,
     * identifiers, session-variable values, error messages, ...) cannot embed the column separator
     * (0x1F) or row delimiter (0x1E) and thereby forge, truncate, or misattribute audit rows in the
     * internal audit_log table (O07 / CWE-117 log injection). Numeric and boolean columns
     * are appended directly since they can never contain these bytes.
     */
    private static void appendField(StringBuilder logBuffer, String value) {
        logBuffer.append(sanitizeField(value)).append(AUDIT_TABLE_COL_SEPARATOR);
    }

    /**
     * Append the final string column of a row: sanitize the value (same reason as
     * appendField) and terminate the row with the line delimiter. Every string column is written
     * through appendField/appendLastField so none can bypass the sanitizer.
     */
    private static void appendLastField(StringBuilder logBuffer, String value) {
        logBuffer.append(sanitizeField(value)).append(AUDIT_TABLE_LINE_DELIMITER);
    }

    /**
     * Replace the audit framing bytes (column separator 0x1F and row delimiter 0x1E) with a space so
     * field content cannot alter row/column framing. Only these two bytes are structural, so other
     * characters (including newlines and tabs already present in SQL text) are preserved as-is.
     */
    private static String sanitizeField(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if (value.indexOf(AUDIT_TABLE_COL_SEPARATOR) < 0 && value.indexOf(AUDIT_TABLE_LINE_DELIMITER) < 0) {
            return value;
        }
        return value.replace(AUDIT_TABLE_COL_SEPARATOR, ' ').replace(AUDIT_TABLE_LINE_DELIMITER, ' ');
    }

    // public for external call.
    // synchronized to avoid concurrent load.
    public synchronized void loadIfNecessary(boolean force) {
        long currentTime = System.currentTimeMillis();

        if (auditLogBuffer.length() != 0 && (force || auditLogBuffer.length() >= GlobalVariable.auditPluginMaxBatchBytes
                || currentTime - lastLoadTimeAuditLog >= GlobalVariable.auditPluginMaxBatchInternalSec * 1000)) {
            // begin to load
            long batchOldest = batchOldestEventTime;
            String batchQueryId = batchOldestQueryId;
            // the zone the sample row was actually RENDERED in (see batchOldestZoneId):
            // passed to the fence so its probe reads the row back under the same zone
            String batchZoneId = batchOldestZoneId;
            // whether the load's outcome was CONFIRMED published; null = the batch was
            // never sent (an earlier failure), so there is nothing to fence
            Boolean published = null;
            // the load label + whether the request reached a BE: the fence
            // resolution needs the label, and a request that never left the FE cannot
            // have committed anything.: the label is allocated - and its
            // obligation RETAINED - BEFORE the request can leave the FE, so the batch
            // stays fenced even when this FE dies while the load is in flight (the BE
            // may commit and publish the batch without this FE ever seeing a response).
            String fenceLabel = "";
            boolean fenceRetained = false;
            boolean fenceOutcomeKnown = false;
            boolean fenceSent = false;
            // set when the send was HELD BACK because the durable fence could not be
            // confirmed: the buffer is kept for the next tick instead of being reset
            boolean holdBatch = false;
            try {
                String token = "";
                try {
                    // Acquire token from master
                    token = Env.getCurrentEnv().getTokenManager().acquireToken();
                } catch (Exception e) {
                    LOG.warn("Failed to get auth token: {}", e);
                    discardLogNum += auditLogNum;
                    return;
                }
                // The obligation is durable BEFORE the body can be written;
                // the pre-allocated label identifies the transaction the request may
                // create, so its outcome stays resolvable by label even after this FE
                // died mid-load.
                fenceLabel = streamLoader.allocateLabel();
                retainPublishFence(batchOldest, batchQueryId, fenceLabel, batchZoneId);
                fenceRetained = batchOldest > 0;
                // The single report of this batch's durable obligation (the retain
                // above is LOCAL only): its confirmed outcome is the send gate.
                if (fenceRetained && !reportCommittedFence()) {
                    // The batch's durable obligation did not land in the shared row: if this
                    // FE dies after the BE commits the load with publication pending, only
                    // the stale row remains, and the capture can checkpoint past the batch's
                    // event time once the row is ignored. HOLD the send (the buffer and the
                    // local fence stay) until the report is confirmed.
                    releasePublishAttempt(fenceLabel,
                            "the durable fence report did not land before the send");
                    fenceRetained = false;
                    holdBatch = true;
                    LOG.warn("audit loader: holding the batch of {} event(s) back until its"
                            + " durable publish fence is confirmed in the shared horizon row",
                            auditLogNum);
                    return;
                }
                AuditStreamLoader.LoadResponse response =
                        streamLoader.loadBatch(auditLogBuffer, token, fenceLabel);
                fenceOutcomeKnown = true;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("audit loader response: {}", response);
                }
                published = batchPublicationConfirmed(response);
                fenceSent = response.sent;
                if (!published) {
                    LOG.warn("audit loader: the stream load of {} event(s) is not confirmed"
                            + " published ({}); its rows keep fencing the capture progress"
                            + " until they become readable", auditLogNum, response);
                }
            } catch (Exception e) {
                // a reported error (typically a timeout) may hide a COMMITTED load whose
                // rows are only not readable yet: fence it like a Publish Timeout. With
                // the obligation already retained an exception cannot open
                // the crash gap either: the request may have been delivered.
                published = Boolean.FALSE;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
                }
                discardLogNum += auditLogNum;
            } finally {
                // Resolve the pre-send obligation on a DECIDED outcome:
                // confirmed-published rows are readable, and a request that never
                // reached a BE cannot have created a transaction. Every other outcome
                // (Publish Timeout, an error, an unknowable delivery state) KEEPS the
                // obligation - it is exactly the crash gap this fence exists for.
                if (fenceRetained) {
                    if (Boolean.TRUE.equals(published)) {
                        releasePublishAttempt(fenceLabel,
                                "the stream load CONFIRMED its publication");
                    } else if (fenceOutcomeKnown && !fenceSent) {
                        releasePublishAttempt(fenceLabel,
                                "the request never reached a BE (no transaction can exist)");
                    } else {
                        // re-report: the pre-send report may have been lost, and this
                        // batch may now be COMMITTED and unreadable
                        reportCommittedFence();
                    }
                }
                // make a new string builder to receive following events.
                if (holdBatch) {
                    // the events were NOT sent: keep them buffered for the next tick, only
                    // push the retry out so the loop does not spin on a failing report
                    lastLoadTimeAuditLog = currentTime;
                } else {
                    resetBatch(currentTime);
                }
                if (discardLogNum > 0) {
                    LOG.info("num of total discarded audit logs: {}", discardLogNum);
                }
            }
        }
    }

    /**
     * Whether a stream-load response PROVES the batch is published (visible in the shared
     * audit table). Only a HTTP-OK response with a COMPLETE, parseable body whose
     * Status is Success does: a body that could not be read
     * (or was only partially read, see AuditStreamLoader) says NOTHING about the
     * transaction, and the previous "does not contain 'publish timeout'" test treated it
     * as published - a committed Publish Timeout whose body read failed then reset its
     * batch WITHOUT a fence and the capture could checkpoint past the still-unreadable
     * rows. Every other outcome (Publish Timeout, any failure status, an unreadable
     * body) keeps fencing until the batch's sample row is observed or the bound expires.
     */
    @VisibleForTesting
    static boolean batchPublicationConfirmed(AuditStreamLoader.LoadResponse response) {
        if (response == null || response.status != 200 || !response.contentComplete) {
            return false;
        }
        String content = response.respContent;
        if (content == null || content.trim().isEmpty()) {
            return false;
        }
        try {
            JsonElement parsed = JsonParser.parseString(content);
            if (!parsed.isJsonObject()) {
                return false;
            }
            JsonElement status = parsed.getAsJsonObject().get("Status");
            return status != null && !status.isJsonNull()
                    && "success".equalsIgnoreCase(status.getAsString().trim());
        } catch (RuntimeException e) {
            return false; // not a parseable stream-load response: ambiguous, keep fencing
        }
    }

    /**
     * Retains the fence of a batch whose publication is not confirmed. EVERY pending
     * batch gets its OWN entry: the previous single slot kept only the
     * OLDEST batch's sample, so the moment A's sample became visible the WHOLE fence was
     * cleared although a newer batch B could still be committed and unreadable - the
     * capture then checkpointed past B and later windows could never reach it. Each entry
     * carries its own sample row (probed individually) and its own time bound, so the
     * total wait of every batch stays bounded by PUBLISH_FENCE_MAX_MILLIS.
     */
    private void retainPublishFence(long batchOldest, String batchQueryId) {
        retainPublishFence(batchOldest, batchQueryId, "");
    }

    private void retainPublishFence(long batchOldest, String batchQueryId, String label) {
        retainPublishFence(batchOldest, batchQueryId, label, "");
    }

    /**
     * Retains the obligation of one batch whose publication outcome is not yet decided
     * ( as the per-batch fence; called BEFORE the load is sent,
     * so the possible commitment is durable before the request can leave the FE - a
     * crash while the load is in flight no longer leaves the batch unfenced for the
     * cluster). The entry carries everything a later resolution needs: the batch's
     * oldest event time, its sample query id, its load label (the transaction manager
     * resolves the transaction's TERMINAL state by label) and the zone the sample row
     * was RENDERED in. The obligation is RELEASED again when the outcome proves it
     * unnecessary (see releasePublishAttempt); on an ambiguous outcome it
     * stays and is resolved by confirmPublishFence locally, or - after this
     * FE's death - by the shared-row reader through the label column.
     *
     * @param batchOldest the batch's oldest event time (0 = nothing to fence)
     * @param batchQueryId the sample row's query id ("" = not probeable)
     * @param label        the batch's load label, "" when the request never reached a BE
     * @param renderedZone the zone the sample row's time column was rendered in ("" =
     *                     unknown, e.g. a legacy caller: fall back to the usage-based
     *                     derivation)
     */
    private void retainPublishFence(long batchOldest, String batchQueryId, String label,
            String renderedZone) {
        if (batchOldest <= 0) {
            return;
        }
        // The zone the sample row's time column was rendered in. Callers that watched the
        // row being built pass it directly (see batchOldestZoneId) - re-deriving it from
        // the zone-usage registry can pick a DIFFERENT zone: the registry keeps only the
        // LAST use per zone, so after a `SET GLOBAL time_zone` the derivation matches the
        // new zone for instants the old zone actually rendered, and the fence's probe
        // then never finds its sample row. The derivation remains only as the fallback
        // for callers that did not.
        String writerZoneId = renderedZone == null || renderedZone.isEmpty()
                ? AuditWriterZones.zoneOfRenderTime(batchOldest)
                : renderedZone;
        synchronized (this) {
            pendingPublishFences.add(new PublishFence(batchOldest,
                    batchQueryId == null ? "" : batchQueryId, publishFenceNow(), label,
                    writerZoneId));
            while (pendingPublishFences.size() > MAX_PENDING_PUBLISH_FENCES) {
                PublishFence dropped = pendingPublishFences.remove(0);
                // the dropped batch keeps fencing through the AGGREGATE:
                // releasing it with the removal let the capture advance past a batch whose
                // rows may still publish
                aggregateDroppedFence(dropped);
                LOG.warn("audit loader: aggregating the publish fence of event time {}"
                                + " into the dropped-fence aggregate (more than {} batches"
                                + " await publication; the aggregate expires with the same"
                                + " {} ms bound)",
                        dropped.oldestEventTime, MAX_PENDING_PUBLISH_FENCES,
                        PUBLISH_FENCE_MAX_MILLIS);
            }
        }
        // The obligation stays LOCAL here: republishing it is the CALLER's step - the
        // load worker reports the just-retained fence ONCE and uses the CONFIRMED report
        // as its send gate (see loadIfNecessary). Reporting here as well paid a second
        // synchronous shared-row write + read-back for every batch, with its result
        // discarded anyway.
    }

    /**
     * The OLDEST event time of a batch whose stream load reported Publish Timeout (or
     * failed ambiguously): that transaction is COMMITTED, so its rows may publish even
     * AFTER this FE stops. The horizon row carries this value so the cluster fence keeps
     * holding past the FE's death (see AuditPublicationHorizon). 0 = nothing pending.
     */
    long oldestPendingPublishFenceEventTime() {
        synchronized (this) {
            long oldest = 0;
            for (PublishFence fence : pendingPublishFences) {
                if (oldest == 0 || fence.oldestEventTime < oldest) {
                    oldest = fence.oldestEventTime;
                }
            }
            return minPositive(oldest, liveDroppedPublishFence());
        }
    }

    /**
     * Merges one bounded-out batch into the dropped-fence aggregate
     * the aggregate carries the EARLIEST dropped event time and the LATEST
     * member's own retention deadline, so NO member loses its fence before its own
     * PUBLISH_FENCE_MAX_MILLIS window elapsed.
     */
    private void aggregateDroppedFence(PublishFence dropped) {
        if (droppedPublishFenceTime == 0 || dropped.oldestEventTime < droppedPublishFenceTime) {
            droppedPublishFenceTime = dropped.oldestEventTime;
        }
        droppedPublishFenceUntil = Math.max(droppedPublishFenceUntil,
                dropped.since + PUBLISH_FENCE_MAX_MILLIS);
        // the batch's IDENTITY must survive here too: the event time alone cannot be
        // resolved against the transaction manager (see droppedPublishFenceLabels).
        // At capacity the OLDEST retained identities are resolved first and the ones
        // that can no longer publish (VISIBLE rows are readable, ABORTED can never
        // publish) give up their slots - an identity LOST here cannot be resolved after
        // this FE dies, so only a genuinely unresolved set may overflow.
        if (droppedPublishFenceLabels.size() >= MAX_AGGREGATED_FENCE_LABELS) {
            drainResolvedAggregatedLabels();
        }
        if (droppedPublishFenceLabels.size() < MAX_AGGREGATED_FENCE_LABELS) {
            droppedPublishFenceLabels.add(dropped.label.isEmpty() ? "-" : dropped.label);
        } else {
            droppedFenceLabelsOverflowed = true;
        }
    }

    /**
     * Drops every retained aggregated identity whose transaction already resolved to a
     * TERMINAL state (see AuditPublicationHorizon#committedFenceSettled): VISIBLE rows
     * are readable and ABORTED batches can never publish, so neither needs its slot.
     * Called under the loader monitor, on the (already pathological) eviction path only.
     */
    private void drainResolvedAggregatedLabels() {
        java.util.Iterator<String> iterator = droppedPublishFenceLabels.iterator();
        while (iterator.hasNext()) {
            String label = iterator.next();
            if (label.isEmpty() || "-".equals(label)
                    || OVERFLOWED_FENCE_LABEL.equals(label)) {
                // no identity was recorded, or the slot stands for a batch whose identity
                // is UNKNOWN: a label lookup can never settle either, so they keep their
                // slots (fail closed)
                continue;
            }
            if (isTerminalTransactionStatus(transactionStatusForLabel(label))) {
                iterator.remove();
            }
        }
    }

    /**
     * The still-valid dropped-fence aggregate, or 0 once EVERY member's own retention
     * bound elapsed (the batch overflowed at minute 29 keeps its share of
     * the aggregate until minute 59; the sample rows of the aggregated batches cannot be
     * probed individually without re-growing the list, so the deadlines - not a
     * read-back - release it, exactly like the per-batch fallback in
     * confirmPublishFence). Clears itself lazily under the monitor.
     */
    private long liveDroppedPublishFence() {
        if (droppedPublishFenceTime == 0) {
            return 0;
        }
        if (publishFenceNow() > droppedPublishFenceUntil) {
            // The age bound alone does NOT release the aggregate: a bounded-out batch's
            // transaction can be COMMITTED with its publication still pending, and a zero
            // report then let the capture checkpoint past an unreadable batch. Resolve
            // every retained label by its transaction state first (see
            // confirmPublishFence): terminal (VISIBLE / ABORTED) releases, COMMITTED /
            // PRECOMMITTED keeps WITHOUT an age bound, and an unresolvable label keeps the
            // aggregate for one survival window before the age fallback applies.
            List<String> retained = new ArrayList<>();
            boolean committedFound = false;
            for (String label : droppedPublishFenceLabels) {
                String status = label.isEmpty() || "-".equals(label)
                        ? null : transactionStatusForLabel(label);
                if (isTerminalTransactionStatus(status)) {
                    continue;
                }
                retained.add(label);
                if (isCommittedTransactionStatus(status)) {
                    committedFound = true;
                }
            }
            long now = publishFenceNow();
            boolean survivalElapsed = now > droppedPublishFenceUntil
                    + AuditPublicationHorizon.COMMITTED_FENCE_SURVIVAL_MILLIS;
            if (committedFound || (!retained.isEmpty() && !survivalElapsed)
                    || (droppedFenceLabelsOverflowed && !survivalElapsed)) {
                droppedPublishFenceLabels.clear();
                droppedPublishFenceLabels.addAll(retained);
                droppedFenceLabelsOverflowed = droppedFenceLabelsOverflowed && !committedFound;
                // re-arm the re-check window; a COMMITTED member keeps the aggregate until
                // its label resolves instead of being released by age alone
                droppedPublishFenceUntil = now + PUBLISH_FENCE_MAX_MILLIS;
                LOG.warn("audit loader: the aggregated dropped-publish fence of event time"
                                + " {} stays live: {} member(s) are still unresolved{}",
                        droppedPublishFenceTime, retained.size(),
                        committedFound ? " (a COMMITTED transaction is still pending)" : "");
                return droppedPublishFenceTime;
            }
            LOG.warn("audit loader: the aggregated dropped-publish fence of event time {}"
                            + " is released after every member's own {} ms bound elapsed; the"
                            + " aggregated batches are assumed lost",
                    droppedPublishFenceTime, PUBLISH_FENCE_MAX_MILLIS);
            droppedPublishFenceTime = 0;
            droppedPublishFenceUntil = 0;
            droppedPublishFenceLabels.clear();
            droppedFenceLabelsOverflowed = false;
            return 0;
        }
        return droppedPublishFenceTime;
    }

    /** Whether a resolved transaction state can still PUBLISH (see liveDroppedPublishFence). */
    private static boolean isCommittedTransactionStatus(String status) {
        return "COMMITTED".equals(status) || "PRECOMMITTED".equals(status);
    }

    /** The smaller of two positive values (0 means "none" and is ignored). */
    private static long minPositive(long first, long second) {
        if (first <= 0) {
            return Math.max(0L, second);
        }
        if (second <= 0) {
            return first;
        }
        return Math.min(first, second);
    }

    /**
     * The committed-but-unreadable fence this FE owes the cluster, or 0 when no such
     * batch is pending (or no loader runs). Read by the horizon reporter and by the
     * shared-table writer itself so a report can never clear the durable fence.
     */
    public static long oldestCommittedPublishFenceEventTime() {
        AuditLoader loader = runningLoader;
        return loader == null ? 0L : loader.oldestPendingPublishFenceEventTime();
    }

    /**
     * The load labels of the batches still fencing this FE's progress, oldest first,
     * "-" for a batch without an identity and "*" for an identity that did not fit the
     * aggregated-label budget, joined with ';' ("" when nothing fences). Travels in the
     * shared horizon row so the reader can resolve each transaction after this FE died
     * - a dead FE cannot re-report, so the labels are the only way to learn whether its
     * committed fences are terminal instead of expiring them on a bare age bound (and
     * the overflow marker is never expired that way at all).
     */
    static String oldestCommittedPublishFenceLabels() {
        AuditLoader loader = runningLoader;
        return loader == null ? "" : loader.pendingPublishFenceLabels();
    }

    /**
     * The encoded labels of the pending fences (see above); guarded by the monitor. The
     * AGGREGATED fences come first - their batches are older than every survivor - and
     * only while their aggregate is still live: the labels are what the dead-FE
     * settlement resolves, and dropping them from a live aggregate let a still-COMMITTED
     * overflowed batch read as settled.
     */
    private String pendingPublishFenceLabels() {
        synchronized (this) {
            long droppedFence = liveDroppedPublishFence();
            if (pendingPublishFences.isEmpty() && droppedFence <= 0) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            if (droppedFence > 0) {
                for (String label : droppedPublishFenceLabels) {
                    appendFenceLabel(sb, label);
                }
                if (droppedFenceLabelsOverflowed) {
                    // a DISTINCT marker (not "-"): the reader must keep this slot fenced
                    // without an age bound - the omitted batch is a real
                    // dropped-publish-timeout whose transaction may still publish
                    appendFenceLabel(sb, OVERFLOWED_FENCE_LABEL);
                }
            }
            for (PublishFence fence : pendingPublishFences) {
                appendFenceLabel(sb, fence.label.isEmpty() ? "-" : fence.label);
            }
            return sb.toString();
        }
    }

    /** Appends one ';'-separated fence label (see pendingPublishFenceLabels). */
    private static void appendFenceLabel(StringBuilder sb, String label) {
        if (sb.length() > 0) {
            sb.append(';');
        }
        sb.append(label);
    }

    /**
     * Runs the given action with this loader's PUBLICATION-FENCE monitor held (see
     * AuditPublicationHorizon#reportLocalHorizon): the report's snapshot of the fence
     * value / labels and its UPSERT must be atomic against the fence mutations, all of
     * which already run under this monitor. Without the serialization, a batch retained
     * BETWEEN the snapshot and the write (the load thread can durably report the batch's
     * own label before sending it) is written over by the OLDER report - if the report's
     * UPSERT then commits LAST and this FE dies, the reader sees committed_fence_ms = 0
     * and drops the row, letting the capture checkpoint past a still-unreadable audit
     * event.
     *
     * With no running loader there is no fence state to serialize either (shutdown, unit
     * tests): the action runs directly.
     */
    static <T> T withPublicationFenceLock(java.util.function.Supplier<T> action) {
        AuditLoader loader = runningLoader;
        if (loader == null) {
            return action.get();
        }
        synchronized (loader) {
            return action.get();
        }
    }

    /** Best-effort immediate report of this FE's committed-but-unreadable batches. */
    private boolean reportCommittedFence() {
        Runnable hook = reportCommittedFenceHookForTest;
        if (hook != null) {
            hook.run();
        }
        try {
            long pending = oldestPendingPublishFenceEventTime();
            if (pending <= 0) {
                // no committed-but-unreadable batch is owed: there is nothing a send
                // could leave behind unattended
                return true;
            }
            Env env = Env.getCurrentEnv();
            if (env == null || !env.isReady()) {
                // a not-yet-ready (or absent) Env cannot confirm anything, and the
                // loader's own tick reports again once it is: the durable-fence gate
                // would otherwise stall every load during startup with nothing gained
                return true;
            }
            return AuditPublicationHorizon.reportLocalHorizon(pending);
        } catch (Throwable t) {
            LOG.warn("audit loader: cannot report the committed publish fence: {}",
                    t.getMessage());
            return false;
        }
    }

    /**
     * Releases the fence of EVERY pending batch whose sample row is READABLE (each batch
     * is confirmed INDIVIDUALLY), and the fences whose transaction reached a
     * TERMINAL state that cannot publish any more. Elapsed time ALONE is
     * not such a state: a Publish-Timeout transaction is COMMITTED and the BE's publish
     * daemon keeps retrying, so releasing the only fence on the age bound could miss an
     * event that publishes just after - the outcome (VISIBLE / ABORTED) is resolved by
     * the batch's load label instead. Unconfirmable probes keep their fence. Called by
     * the load worker on every tick.
     */
    private void confirmPublishFence() {
        List<PublishFence> pending;
        synchronized (this) {
            if (pendingPublishFences.isEmpty()) {
                return;
            }
            pending = new ArrayList<>(pendingPublishFences);
        }
        long now = publishFenceNow();
        for (PublishFence fence : pending) {
            boolean visible = publishVisibilityProbeForTest != null
                    ? publishVisibilityProbeForTest.isVisible(fence.oldestEventTime, fence.queryId)
                    : publishFenceRowVisible(fence);
            if (visible) {
                releasePublishFence(fence, "its rows are readable now");
                continue;
            }
            if (now - fence.since > PUBLISH_FENCE_MAX_MILLIS) {
                String status = transactionStatusForLabel(fence.label);
                if (isTerminalTransactionStatus(status)) {
                    releasePublishFence(fence, "its transaction is terminal (" + status
                            + "): the rows can no longer appear");
                } else if ("COMMITTED".equals(status) || "PRECOMMITTED".equals(status)) {
                    // Elapsed time is NOT proof of loss. A Publish-Timeout
                    // batch stays COMMITTED while the publish daemon keeps retrying, so
                    // releasing the only fence at the bound could miss an event that
                    // publishes a minute later. The fence is released by the OUTCOME
                    // (VISIBLE / ABORTED), which the transaction manager reports once the
                    // retry stops.
                    LOG.warn("audit loader: the publish fence of event time {} is older"
                                    + " than {} ms, but its transaction is still {}: keeping"
                                    + " the fence - the publish daemon may make its rows"
                                    + " readable at any moment",
                            fence.oldestEventTime, PUBLISH_FENCE_MAX_MILLIS, status);
                } else {
                    // The outcome cannot be resolved at all (no transaction manager, no
                    // record of the label - e.g. the load never got as far as creating a
                    // transaction - or a state that can no longer commit). Exactly like
                    // the overflow aggregate above, the retention bound stays the LAST
                    // resort so a genuinely lost batch cannot fence the capture forever.
                    releasePublishFence(fence, "its transaction state is unknowable ("
                            + (status == null ? "unresolved" : status) + ") and the "
                            + PUBLISH_FENCE_MAX_MILLIS + " ms retention elapsed; assuming"
                            + " the batch was lost");
                }
            }
        }
    }

    /**
     * Whether a transaction status is TERMINAL for the publish fence: a
     * VISIBLE transaction's rows are readable (the probe may merely have failed to
     * confirm them), an ABORTED one can never publish. Everything else - including an
     * undecidable state - keeps fencing. Package-visible: the shared-row reader applies
     * the same rule to the committed fence of a GONE FE.
     */
    static boolean isTerminalTransactionStatus(String status) {
        return "VISIBLE".equals(status) || "ABORTED".equals(status);
    }

    /**
     * The transaction status of one audit batch, resolved by its load label
     * #6), or null when it cannot be resolved. The audit stream load is sent to THIS
     * FE's own endpoint, so the transaction (if the request was delivered) is registered
     * in the internal schema's transaction manager and its state is decidable here.
     *
     * @param label the load label ("" = nothing to resolve)
     * @return VISIBLE / ABORTED / COMMITTED / ... , or null
     */
    @VisibleForTesting
    static String transactionStatusForLabel(String label) {
        java.util.function.Function<String, String> seam = transactionStatusForTest;
        if (seam != null) {
            return seam.apply(label);
        }
        if (label == null || label.isEmpty()) {
            return null;
        }
        try {
            Env env = Env.getCurrentEnv();
            if (env == null || env.getInternalCatalog() == null) {
                return null;
            }
            Database db = env.getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);
            if (db == null) {
                return null;
            }
            Long txnId = env.getGlobalTransactionMgr().getTransactionId(db.getId(), label);
            if (txnId == null) {
                return null;
            }
            TransactionState state = env.getGlobalTransactionMgr()
                    .getTransactionState(db.getId(), txnId);
            return state == null ? null : state.getTransactionStatus().name();
        } catch (Throwable t) {
            LOG.warn("audit loader: cannot resolve the transaction state of label {}: {}",
                    label, t.getMessage());
            return null;
        }
    }

    /**
     * Releases the PRE-SEND obligation of one batch whose outcome makes it
     * unnecessary: the response CONFIRMED publication (the rows are readable), or the
     * request never reached a BE (no transaction can exist). Matching is by the batch's
     * own label, so the obligation of every OTHER batch stays in place.
     *
     * @param label  the batch's pre-allocated load label
     * @param reason the resolved outcome (logged)
     */
    private void releasePublishAttempt(String label, String reason) {
        if (label == null || label.isEmpty()) {
            return;
        }
        PublishFence match = null;
        synchronized (this) {
            for (PublishFence fence : pendingPublishFences) {
                if (label.equals(fence.label)) {
                    match = fence;
                    break;
                }
            }
        }
        if (match != null) {
            releasePublishFence(match, reason);
        }
    }

    /**
     * Releases ONE pending batch's fence (see confirmPublishFence); the other
     * pending batches keep fencing.
     */
    private void releasePublishFence(PublishFence fence, String reason) {
        synchronized (this) {
            if (!pendingPublishFences.remove(fence)) {
                return; // already released by an earlier tick
            }
            LOG.info("audit loader: released the Publish-Timeout fence of event time {}"
                            + " ({}; {} pending batches remain)", fence.oldestEventTime, reason,
                    pendingPublishFences.size());
        }
    }

    /** For tests: the event time of the OLDEST pending publish fence (0 = none). */
    @VisibleForTesting
    long oldestPublishFenceForTest() {
        return oldestPendingPublishFenceEventTime();
    }

    /** For tests: the writer zone carried by the OLDEST pending fence ("" = unknown). */
    @VisibleForTesting
    String oldestPublishFenceWriterZoneForTest() {
        synchronized (this) {
            for (PublishFence fence : pendingPublishFences) {
                return fence.writerZoneId;
            }
        }
        return "";
    }

    /** For tests: how many batches currently fence progress (see #pendingPublishFences). */
    @VisibleForTesting
    int pendingPublishFenceCountForTest() {
        synchronized (this) {
            return pendingPublishFences.size();
        }
    }

    /**
     * Real visibility probe of a fenced batch: is its sample audit row readable yet? The
     * bound is rendered in the zone the row was WRITTEN with: the audit
     * table stores the writer's local wall clock, so after a `SET GLOBAL time_zone` the
     * same instant rendered in the current zone is hours away from the stored one and the
     * probe could never confirm the (perfectly visible) row.
     */
    private static boolean publishFenceRowVisible(PublishFence fence) {
        if (fence.queryId.isEmpty()) {
            return false; // nothing to probe: only the terminal resolution releases the fence
        }
        try {
            Map<String, String> params = new HashMap<>();
            params.put("queryId", StatisticsUtil.escapeSQL(fence.queryId));
            params.put("eventTime", renderProbeEventTime(fence.oldestEventTime,
                    fence.writerZoneId));
            List<ResultRow> rows = StatisticsUtil.executeQuery(PUBLISH_PROBE_SQL, params,
                    PUBLISH_PROBE_TIMEOUT_SECONDS);
            return rows != null && !rows.isEmpty();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("audit loader: publish fence probe failed: {}", e.getMessage());
            }
            return false; // unconfirmable: keep fencing
        }
    }

    /**
     * The probe's time bound for one fence: the instant rendered in the zone the
     * row was written with, falling back to the default rendering when the zone is
     * unknown. Package-visible for tests.
     *
     * @param eventTime    the fence's oldest event time
     * @param writerZoneId the zone the row was rendered in ("" = unknown)
     * @return the rendered bound
     */
    @VisibleForTesting
    static String renderProbeEventTime(long eventTime, String writerZoneId) {
        if (writerZoneId == null || writerZoneId.isEmpty()) {
            return TimeUtils.longToTimeStringWithms(eventTime);
        }
        return TimeUtils.longToTimeStringWithms(eventTime, writerZoneId);
    }

    /**
     * The FE identity reported to the shared horizon table. Uses the FE's configured node
     * name (unique per FE); a missing Env (partial startup) falls back to a constant that
     * only matters while nothing is published anyway.
     */
    static String selfFeName() {
        try {
            Env env = Env.getCurrentEnv();
            String name = env == null ? null : env.getNodeName();
            return name == null || name.isEmpty() ? "unknown-fe" : name;
        } catch (Throwable t) {
            return "unknown-fe";
        }
    }

    private void resetBatch(long currentTime) {
        synchronized (this) {
            this.auditLogBuffer = new StringBuilder();
            this.lastLoadTimeAuditLog = currentTime;
            this.auditLogNum = 0;
            // the batch is published now (its load has returned), so it no longer fences
            // progress
            this.batchOldestEventTime = 0;
            this.batchOldestZoneId = "";
        }
    }

    /**
     * Whether the reporter must (re-)send this FE's row on this tick: a CHANGED horizon /
     * committed fence, a changed writer-zone set, or the KEEPALIVE cadence. An IDLE row
     * (zero horizon, no zones, no fence) is refreshed like any other: the reader treats an
     * OVERDUE row of a LIVE FE as unreadable and fails EVERY capture cycle closed, and the
     * default capture interval (three hours) is far beyond the staleness bound - an idle,
     * healthy cluster that stopped refreshing its zero registration could therefore never
     * advance capture.
     */
    @VisibleForTesting
    static boolean shouldReportHorizon(boolean changed, boolean zonesChanged, long lastReportAt,
            long now) {
        return changed || zonesChanged || now - lastReportAt >= HORIZON_KEEPALIVE_MILLIS;
    }

    private class LoadWorker implements Runnable {

        public LoadWorker() {
        }

        public void run() {
            while (!isClosed) {
                try {
                    // the poll and the assembly are ONE atomic step (see
                    // transferNextEvent): a reader of the publication horizon must never
                    // see the event in neither the queue nor the batch
                    AuditEvent event = transferNextEvent();
                    if (event == null) {
                        // idle: wait OUTSIDE the monitor so the horizon read / another
                        // transfer is never blocked by an empty queue
                        Thread.sleep(QUEUE_POLL_INTERVAL_MILLIS);
                    }
                    // process all audit logs
                    loadIfNecessary(false);
                    // a batch whose load reported Publish Timeout keeps fencing until its
                    // rows are readable
                    confirmPublishFence();
                } catch (InterruptedException ie) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when loading current audit batch", ie);
                    }
                } catch (Exception e) {
                    LOG.error("run audit logger error:", e);
                }
            }
        }
    }

    /**
     * Reports THIS FE's audit publication horizon into the shared table so the leader
     * that runs the capture sees a follower's backlog. It writes when the
     * value CHANGED and re-reports an unchanged value on the keepalive cadence
     * (the reader ignores rows whose reporter went silent), INCLUDING an idle zero row:
     * an overdue row of a LIVE FE fails the reader closed, so an idle cluster that
     * stopped refreshing its registration could never advance capture.
     */
    private class HorizonReporter implements Runnable {

        @Override
        public void run() {
            long lastReported = -1;
            long lastReportAt = 0;
            long lastReportedCommitted = -1;
            String lastReportedZones = "";
            while (!isClosed) {
                try {
                    Thread.sleep(HORIZON_REPORT_TICK_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (isClosed) {
                    return;
                }
                long horizon;
                try {
                    horizon = AuditPublicationHorizon.localHorizon();
                } catch (Throwable t) {
                    LOG.warn("audit horizon reporter: cannot compute the local horizon: {}",
                            t.getMessage());
                    continue;
                }
                long now = System.currentTimeMillis();
                // keep the durable capture watermark fresh for the zone eviction gate
                // - the read is throttled inside
                AuditWriterZones.refreshCaptureCoveredThrough();
                // the audit WRITER's zone history travels with every report:
                // a zone change must reach the capture even while nothing is outstanding,
                // so a changed registry reports immediately and keeps the (zero-horizon)
                // row fresh on the keepalive cadence
                String zones = AuditWriterZones.encode();
                // a committed-but-unreadable batch is part of the durable state even when
                // the horizon VALUE is unchanged: its marker must reach
                // the shared row or a crash would drop the fence
                long committed = oldestCommittedPublishFenceEventTime();
                boolean changed = horizon != lastReported || committed != lastReportedCommitted;
                boolean zonesChanged = !zones.equals(lastReportedZones);
                if (shouldReportHorizon(changed, zonesChanged, lastReportAt, now)) {
                    // Remember the value only when the shared row CONFIRMS it
                    // #5): a failed / not-yet-visible write must be retried on the next
                    // tick, otherwise an old unpublished event would have no
                    // master-visible fence until the 60s keepalive.
                    if (AuditPublicationHorizon.reportLocalHorizon(horizon)) {
                        lastReported = horizon;
                        lastReportedCommitted = committed;
                        lastReportAt = now;
                        lastReportedZones = zones;
                    }
                }
            }
        }
    }

    /**
     * Moves ONE queued event into the current batch, ATOMICALLY with the horizon read
     * (see oldestOutstandingEventTime): the poll and the assembly happen under
     * the loader monitor, so an accepted event is always visible - in the queue before
     * the poll, in the batch after the assembly, never in neither. The previous
     * poll() outside the monitor left exactly that window open, and a capture
     * cycle reading the horizon during it concluded that nothing was outstanding.
     *
     * @return the transferred event, or null when the queue is empty
     */
    private AuditEvent transferNextEvent() throws InterruptedException {
        synchronized (this) {
            AuditEvent event = auditEventQueue.poll();
            if (event != null) {
                assembleAudit(event);
            }
            return event;
        }
    }
}
