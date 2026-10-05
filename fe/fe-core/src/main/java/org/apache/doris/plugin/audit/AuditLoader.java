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
     * ({@code query_audit_log_timeout_ms} / {@code be_report_query_statistics_timeout_ms})
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
     * watermark from {@code audit_log}, and a row that is still inside this loader (queued,
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
    /**
     * Fences of the batches whose stream loads reported Publish Timeout (or failed
     * ambiguously): every such transaction is COMMITTED but its rows are not readable
     * yet, so the batch must KEEP fencing progress until publication is confirmed
     * (round-36 #2). EVERY pending batch is retained as its OWN entry (round-39 #2):
     * keeping only the oldest batch's sample released the whole fence the moment that
     * sample became visible, although a NEWER batch B could still be committed and
     * unreadable - the capture then checkpointed past B, and once the watermark moved,
     * later windows could never reach B's rows. Each entry carries its own sample row
     * ({@code queryId}) and its own {@code since} bound (see
     * {@link #PUBLISH_FENCE_MAX_MILLIS}). Guarded by the loader monitor.
     */
    private final List<PublishFence> pendingPublishFences = new ArrayList<>();

    /** One committed-but-unreadable batch (see {@link #pendingPublishFences}). */
    private static final class PublishFence {
        final long oldestEventTime;
        final String queryId;
        final long since;

        PublishFence(long oldestEventTime, String queryId, long since) {
            this.oldestEventTime = oldestEventTime;
            this.queryId = queryId == null ? "" : queryId;
            this.since = since;
        }
    }

    /**
     * How long a Publish-Timeout fence may hold progress without its sample row ever
     * becoming readable before it is released with a warning (see
     * {@link #pendingPublishFences}).
     */
    public static final long PUBLISH_FENCE_MAX_MILLIS = 30 * 60 * 1000L;

    /**
     * Hard bound of {@link #pendingPublishFences}: a sustained Publish-Timeout rate over
     * the whole fencing window would otherwise grow the list without limit. Beyond the
     * bound the OLDEST entry is released early with a warning (it is the one closest to
     * its own time bound anyway).
     */
    static final int MAX_PENDING_PUBLISH_FENCES = 256;

    /** How often the horizon reporter wakes up (it only writes on change / keepalive). */
    static final long HORIZON_REPORT_TICK_MILLIS = 5_000L;

    /**
     * How often an UNCHANGED, non-zero horizon is re-reported: the shared row must stay
     * fresh while a long hold persists, otherwise {@code AuditPublicationHorizon} treats
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

    /** The clock of the publish-fence bookkeeping (see {@link #publishFenceClockForTest}). */
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
        // readable only AFTER this FE stops (round-40 #10): clearing the row
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
        fillLogBuffer(event, auditLogBuffer);
        ++auditLogNum;
        long eventTime = event.timestamp;
        // INTERNAL events never fence progress (round-37 #7): the capture only scans
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
        }
    }

    /**
     * Start time (epoch millis, the {@code time} column of {@code audit_log}) of the OLDEST
     * event this FE's builtin loader has accepted but not published yet - queued events plus
     * the assembled, not yet flushed batch. Returns 0 when the loader is not running (or has
     * nothing outstanding), i.e. when there is no known publication delay to retain.
     *
     * <p>The SPM capture uses this as a progress fence: its next scan window must still start
     * at or before this instant, otherwise a row the local loader still owes (e.g. a query the
     * {@code query_audit_log_timeout_ms} hold released late, or one sitting behind a slow
     * stream load in the {@link #auditEventQueue}) would fall behind the advanced watermark
     * and never be captured. INTERNAL events are excluded - the capture never scans them,
     * and the reporter's own SQL would otherwise fence itself (round-37 #7).
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
            // unreadable: it keeps fencing until its rows are observed (round-36 #2);
            // EVERY pending batch fences (round-39 #2), so the oldest of them is the
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
                    // INTERNAL events can never be captured (round-37 #7): fencing for
                    // them would only keep the reporter's own writes alive forever
                    continue;
                }
                long eventTime = event.timestamp;
                if (eventTime > 0 && (oldest == 0 || eventTime < oldest)) {
                    oldest = eventTime;
                }
            }
            return oldest;
        }
    }

    private void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        // should be same order as InternalSchema.AUDIT_SCHEMA

        // The zone this row's time column is RENDERED in (round-39 #3), read ONCE and
        // used for BOTH the registration and the rendering (round-40 #8): two
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
    }

    /**
     * Append one string column to the delimiter-framed audit stream-load payload, followed by the
     * column separator. The value is sanitized first so that user-controlled text (SQL statement,
     * identifiers, session-variable values, error messages, ...) cannot embed the column separator
     * (0x1F) or row delimiter (0x1E) and thereby forge, truncate, or misattribute audit rows in the
     * internal {@code audit_log} table (O07 / CWE-117 log injection). Numeric and boolean columns
     * are appended directly since they can never contain these bytes.
     */
    private static void appendField(StringBuilder logBuffer, String value) {
        logBuffer.append(sanitizeField(value)).append(AUDIT_TABLE_COL_SEPARATOR);
    }

    /**
     * Append the final string column of a row: sanitize the value (same reason as {@link
     * #appendField}) and terminate the row with the line delimiter. Every string column is written
     * through {@code appendField}/{@code appendLastField} so none can bypass the sanitizer.
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
            // whether the load's outcome was CONFIRMED published; null = the batch was
            // never sent (an earlier failure), so there is nothing to fence
            Boolean published = null;
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
                AuditStreamLoader.LoadResponse response = streamLoader.loadBatch(auditLogBuffer, token);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("audit loader response: {}", response);
                }
                published = batchPublicationConfirmed(response);
                if (!published) {
                    LOG.warn("audit loader: the stream load of {} event(s) is not confirmed"
                            + " published ({}); its rows keep fencing the capture progress"
                            + " until they become readable", auditLogNum, response);
                }
            } catch (Exception e) {
                // a reported error (typically a timeout) may hide a COMMITTED load whose
                // rows are only not readable yet: fence it like a Publish Timeout
                published = Boolean.FALSE;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
                }
                discardLogNum += auditLogNum;
            } finally {
                if (published != null && !published) {
                    retainPublishFence(batchOldest, batchQueryId);
                }
                // make a new string builder to receive following events.
                resetBatch(currentTime);
                if (discardLogNum > 0) {
                    LOG.info("num of total discarded audit logs: {}", discardLogNum);
                }
            }
        }
    }

    /**
     * Whether a stream-load response PROVES the batch is published (visible in the shared
     * audit table). Only a HTTP-OK response with a COMPLETE, parseable body whose
     * {@code Status} is {@code Success} does (round-40 #9): a body that could not be read
     * (or was only partially read, see {@link AuditStreamLoader}) says NOTHING about the
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
     * batch gets its OWN entry (round-39 #2): the previous single slot kept only the
     * OLDEST batch's sample, so the moment A's sample became visible the WHOLE fence was
     * cleared although a newer batch B could still be committed and unreadable - the
     * capture then checkpointed past B and later windows could never reach it. Each entry
     * carries its own sample row (probed individually) and its own time bound, so the
     * total wait of every batch stays bounded by {@link #PUBLISH_FENCE_MAX_MILLIS}.
     */
    private void retainPublishFence(long batchOldest, String batchQueryId) {
        if (batchOldest <= 0) {
            return;
        }
        synchronized (this) {
            pendingPublishFences.add(new PublishFence(batchOldest,
                    batchQueryId == null ? "" : batchQueryId, publishFenceNow()));
            while (pendingPublishFences.size() > MAX_PENDING_PUBLISH_FENCES) {
                PublishFence dropped = pendingPublishFences.remove(0);
                LOG.warn("audit loader: releasing the publish fence of event time {} early"
                                + " (more than {} batches await publication; each batch also"
                                + " releases its own fence after {} ms)",
                        dropped.oldestEventTime, MAX_PENDING_PUBLISH_FENCES,
                        PUBLISH_FENCE_MAX_MILLIS);
            }
        }
        // Publish the fence RIGHT AWAY instead of waiting for the reporter's tick
        // (round-40 #10): the batch is COMMITTED, and a crash before the next tick would
        // otherwise leave no durable trace of it - the reader then drops this FE's row
        // at death and the capture checkpoints past rows that may still publish. Best
        // effort: the reporter re-reports on its own cadence anyway, and
        // reportLocalHorizon can never DELETE the row while the fence is pending (it
        // reads the fence value at WRITE time).
        reportCommittedFence();
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
            return oldest;
        }
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

    /** Best-effort immediate report of this FE's committed-but-unreadable batches. */
    private void reportCommittedFence() {
        try {
            long pending = oldestPendingPublishFenceEventTime();
            if (pending > 0) {
                AuditPublicationHorizon.reportLocalHorizon(pending);
            }
        } catch (Throwable t) {
            LOG.warn("audit loader: cannot report the committed publish fence: {}",
                    t.getMessage());
        }
    }

    /**
     * Releases the fence of EVERY pending batch whose sample row is READABLE (each batch
     * is confirmed INDIVIDUALLY, round-39 #2), and the fences whose
     * {@link #PUBLISH_FENCE_MAX_MILLIS} bound elapsed with the row never appearing (that
     * batch was lost - fencing forever would freeze the capture instead of protecting
     * anything). Called by the load worker on every tick; unconfirmable probes keep their
     * fence.
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
                    : publishFenceRowVisible(fence.oldestEventTime, fence.queryId);
            if (visible) {
                releasePublishFence(fence, "its rows are readable now");
                continue;
            }
            if (now - fence.since > PUBLISH_FENCE_MAX_MILLIS) {
                releasePublishFence(fence, "its rows are still unreadable after "
                        + PUBLISH_FENCE_MAX_MILLIS + " ms; assuming the batch was lost");
            }
        }
    }

    /**
     * Releases ONE pending batch's fence (see {@link #confirmPublishFence}); the other
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
        synchronized (this) {
            long oldest = 0;
            for (PublishFence fence : pendingPublishFences) {
                if (oldest == 0 || fence.oldestEventTime < oldest) {
                    oldest = fence.oldestEventTime;
                }
            }
            return oldest;
        }
    }

    /** For tests: how many batches currently fence progress (see #pendingPublishFences). */
    @VisibleForTesting
    int pendingPublishFenceCountForTest() {
        synchronized (this) {
            return pendingPublishFences.size();
        }
    }

    /** Real visibility probe of a fenced batch: is its sample audit row readable yet? */
    private static boolean publishFenceRowVisible(long eventTime, String queryId) {
        if (queryId == null || queryId.isEmpty()) {
            return false; // nothing to probe: only the bounded retention releases the fence
        }
        try {
            Map<String, String> params = new HashMap<>();
            params.put("queryId", StatisticsUtil.escapeSQL(queryId));
            params.put("eventTime", TimeUtils.longToTimeStringWithms(eventTime));
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
        }
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
                    // rows are readable (round-36 #2)
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
     * that runs the capture sees a follower's backlog (round-36 #1). It writes when the
     * value CHANGED and re-reports an unchanged non-zero value on the keepalive cadence
     * (the reader ignores rows whose reporter went silent). A zero horizon is reported
     * once (which removes the row).
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
                // (round-40 #4) - the read is throttled inside
                AuditWriterZones.refreshCaptureCoveredThrough();
                // the audit WRITER's zone history travels with every report (round-39 #3):
                // a zone change must reach the capture even while nothing is outstanding,
                // so a changed registry reports immediately and keeps the (zero-horizon)
                // row fresh on the keepalive cadence
                String zones = AuditWriterZones.encode();
                // a committed-but-unreadable batch is part of the durable state even when
                // the horizon VALUE is unchanged (round-40 #10): its marker must reach
                // the shared row or a crash would drop the fence
                long committed = oldestCommittedPublishFenceEventTime();
                boolean changed = horizon != lastReported || committed != lastReportedCommitted;
                boolean zonesChanged = !zones.equals(lastReportedZones);
                boolean keepAlive = (horizon > 0 || !zones.isEmpty() || committed > 0)
                        && now - lastReportAt >= HORIZON_KEEPALIVE_MILLIS;
                if (changed || zonesChanged || keepAlive) {
                    // Remember the value only when the shared row CONFIRMS it (round-37
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
     * (see {@link #oldestOutstandingEventTime}): the poll and the assembly happen under
     * the loader monitor, so an accepted event is always visible - in the queue before
     * the poll, in the batch after the assembly, never in neither. The previous
     * {@code poll()} outside the monitor left exactly that window open, and a capture
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
