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
     * The longest an IDLE load worker waits for the next queued event before it looks at the
     * batch again (millis). The wait is woken by an enqueue (see exec), so this is an upper
     * bound, and it is further bounded by the batch's own flush deadline
     * (see idleWaitMillisLocked).
     */
    public static final long QUEUE_POLL_INTERVAL_MILLIS = 5_000L;

    // the "0x1F" and "0x1E" are used to separate columns and lines in audit log data
    public static final char AUDIT_TABLE_COL_SEPARATOR = 0x1F;
    public static final char AUDIT_TABLE_LINE_DELIMITER = 0x1E;
    // the "\\x1F" and "\\x1E" are used to specified column and line delimiter in stream load request
    // which is corresponding to the "\\u001F" and "\\u001E" in audit log data.
    public static final String AUDIT_TABLE_COL_SEPARATOR_STR = "\\x1F";
    public static final String AUDIT_TABLE_LINE_DELIMITER_STR = "\\x1E";


    private StringBuilder auditLogBuffer = new StringBuilder();
    private int auditLogNum = 0;
    private long lastLoadTimeAuditLog = 0;

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
            // Wake an IDLE worker (see transferNextEvent): it waits on the loader monitor
            // between empty polls, so without the notification the event would sit in the
            // queue for a whole poll interval before it entered the batch. The enqueue
            // above happens FIRST, so a worker that polls before this monitor is taken
            // already finds the event and never waits for the notification.
            synchronized (this) {
                notifyAll();
            }
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
    }

    /**
     * Appends one event's row to the batch buffer.
     */
    private void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        // should be same order as InternalSchema.AUDIT_SCHEMA

        // uuid and time
        appendField(logBuffer, event.queryId);
        logBuffer.append(TimeUtils.longToTimeStringWithms(event.timestamp))
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
        logBuffer.append(event.spillWriteBytesToRemoteStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.spillReadBytesFromRemoteStorage).append(AUDIT_TABLE_COL_SEPARATOR);
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
        }
    }

    private class LoadWorker implements Runnable {

        public LoadWorker() {
        }

        public void run() {
            while (!isClosed) {
                try {
                    // the poll, the wait for the next event and the assembly are ONE
                    // atomic step (see transferNextEvent): a reader of the audit batch
                    // state must never see the event in neither the queue nor the batch.
                    // An empty queue blocks here instead of sleeping after the poll, so an
                    // event arriving while idle is picked up immediately.
                    transferNextEvent();
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
     * Moves ONE queued event into the current batch, ATOMICALLY with the batch state
     * read: the poll and the assembly happen under the loader monitor, so an accepted
     * event is always visible - in the queue before the poll, in the batch after the
     * assembly, never in neither.
     *
     * <p>While the queue is EMPTY it waits on the loader monitor (which the wait releases)
     * instead of sleeping after the poll, so an event enqueued right after an empty poll
     * wakes the worker through the notification in {@link #exec} instead of waiting out a
     * whole poll interval; the batch flush then runs immediately as well. The wait is
     * bounded by the BATCH's own flush deadline, so an interval configured below the poll
     * interval still flushes on time (see idleWaitMillisLocked). The notification cannot
     * be lost: {@link #exec} enqueues BEFORE it takes the monitor, so a worker that polls
     * in between finds the event and never waits at all.
     *
     * @return the transferred event, or null when the wait ended without one
     */
    private AuditEvent transferNextEvent() throws InterruptedException {
        synchronized (this) {
            AuditEvent event = auditEventQueue.poll();
            if (event == null) {
                wait(idleWaitMillisLocked());
                event = auditEventQueue.poll();
            }
            if (event != null) {
                assembleAudit(event);
            }
            return event;
        }
    }

    /**
     * How long an idle worker may wait for the next event: the poll interval, bounded by the
     * remaining time until the current NON-EMPTY batch must be flushed
     * (audit_plugin_max_batch_interval_sec), so a short interval is not stretched to the
     * poll interval. Caller holds the loader monitor.
     *
     * @return the wait in milliseconds, always positive
     */
    private long idleWaitMillisLocked() {
        if (auditLogNum == 0) {
            return QUEUE_POLL_INTERVAL_MILLIS;
        }
        long flushAt = lastLoadTimeAuditLog + GlobalVariable.auditPluginMaxBatchInternalSec * 1000L;
        long remaining = flushAt - System.currentTimeMillis();
        return Math.max(1L, Math.min(QUEUE_POLL_INTERVAL_MILLIS, remaining));
    }
}
