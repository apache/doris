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

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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
    // sometimes the audit log may fail to load to doris, count it to observe.
    private long discardLogNum = 0;

    private BlockingQueue<AuditEvent> auditEventQueue;
    private AuditStreamLoader streamLoader;
    private Thread loadThread;

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

            isInit = true;
            runningLoader = this;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (runningLoader == this) {
            runningLoader = null;
        }
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when closing the audit loader", e);
                }
            }
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
        if (eventTime > 0 && (batchOldestEventTime == 0 || eventTime < batchOldestEventTime)) {
            batchOldestEventTime = eventTime;
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
     * and never be captured.
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
            // the queue is drained FIFO, but the ENQUEUE order is not the event-time order
            // (the upstream hold releases events by completion, not by start), so every
            // queued event is examined. The queue is a weak-consistency view and the scan
            // is cheap next to a capture cycle; a concurrently dequeued event is simply no
            // longer outstanding.
            for (AuditEvent event : queue) {
                if (event == null) {
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

        // uuid and time
        appendField(logBuffer, event.queryId);
        logBuffer.append(TimeUtils.longToTimeStringWithms(event.timestamp)).append(AUDIT_TABLE_COL_SEPARATOR);

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
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
                }
                discardLogNum += auditLogNum;
            } finally {
                // make a new string builder to receive following events.
                resetBatch(currentTime);
                if (discardLogNum > 0) {
                    LOG.info("num of total discarded audit logs: {}", discardLogNum);
                }
            }
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
