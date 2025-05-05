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
import org.apache.doris.metric.MetricRepo;
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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoader extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoader.class);

    public static final String AUDIT_LOG_TABLE = "audit_log";

    private StringBuilder auditLogBuffer = new StringBuilder();
    private int auditLogNum = 0;
    private long lastLoadTimeAuditLog = 0;
    // sometimes the audit log may fail to load to doris, count it to observe.
    private long discardLogNum = 0;

    private BlockingQueue<AuditEvent> auditEventQueue;
    private AuditStreamLoader streamLoader;
    private Thread loadThread;
    private AssistantOfLoadWorker assistantOfLoadWorker;

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
            this.assistantOfLoadWorker = new AssistantOfLoadWorker("audit assistant thread");
            this.assistantOfLoadWorker.start();

            isInit = true;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        Thread[] threads = {loadThread, assistantOfLoadWorker};
        for (Thread thread : threads) {
            if (thread != null) {
                try {
                    loadThread.join();
                } catch (InterruptedException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when closing the audit loader", e);
                    }
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

    private void assembleAudit(AuditEvent event) {
        fillLogBuffer(event, auditLogBuffer);
        ++auditLogNum;
    }

    private void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        // should be same order as InternalSchema.AUDIT_SCHEMA
        logBuffer.append(event.queryId).append("\t");
        logBuffer.append(TimeUtils.longToTimeStringWithms(event.timestamp)).append("\t");
        logBuffer.append(event.clientIp).append("\t");
        logBuffer.append(event.user).append("\t");
        logBuffer.append(event.ctl).append("\t");
        logBuffer.append(event.db).append("\t");
        logBuffer.append(event.state).append("\t");
        logBuffer.append(event.errorCode).append("\t");
        logBuffer.append(event.errorMessage).append("\t");
        logBuffer.append(event.queryTime).append("\t");
        logBuffer.append(event.scanBytes).append("\t");
        logBuffer.append(event.scanRows).append("\t");
        logBuffer.append(event.returnRows).append("\t");
        logBuffer.append(event.shuffleSendRows).append("\t");
        logBuffer.append(event.shuffleSendBytes).append("\t");
        logBuffer.append(event.scanBytesFromLocalStorage).append("\t");
        logBuffer.append(event.scanBytesFromRemoteStorage).append("\t");
        logBuffer.append(event.stmtId).append("\t");
        logBuffer.append(event.stmtType).append("\t");
        logBuffer.append(event.isQuery ? 1 : 0).append("\t");
        logBuffer.append(event.isNereids ? 1 : 0).append("\t");
        logBuffer.append(event.feIp).append("\t");
        logBuffer.append(event.cpuTimeMs).append("\t");
        logBuffer.append(event.sqlHash).append("\t");
        logBuffer.append(event.sqlDigest).append("\t");
        logBuffer.append(event.peakMemoryBytes).append("\t");
        logBuffer.append(event.workloadGroup).append("\t");
        logBuffer.append(event.cloudClusterName).append("\t");
        // already trim the query in org.apache.doris.qe.AuditLogHelper#logAuditLog
        String stmt = event.stmt;
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive audit event with stmt: {}", stmt);
        }
        logBuffer.append(stmt).append("\n");
    }

    private void load(String data) throws Exception {
        String token = Env.getCurrentEnv().getTokenManager().acquireToken();
        AuditStreamLoader.LoadResponse response = streamLoader.loadBatch(data, token);
        if (response.status != 200 || "Fail".equals(response.respContentObj.status)) {
            throw new Exception(response.respContent);
        }
    }

    // public for external call.
    // synchronized to avoid concurrent load.
    public synchronized void loadIfNecessary(boolean force) {
        long currentTime = System.currentTimeMillis();

        if (auditLogBuffer.length() != 0 && (force || auditLogBuffer.length() >= GlobalVariable.auditPluginMaxBatchBytes
                || currentTime - lastLoadTimeAuditLog >= GlobalVariable.auditPluginMaxBatchInternalSec * 1000)) {
            // begin to load
            try {
                load(auditLogBuffer.toString());
            } catch (Exception e) {
                LOG.warn("encounter exception when loading audit logs into audit table", e);
                assistantOfLoadWorker.addLoadFailAuditData(auditLogBuffer.toString(), auditLogNum);
            } finally {
                // make a new string builder to receive following events.
                resetBatch(currentTime);
            }
        }
    }

    private void resetBatch(long currentTime) {
        this.auditLogBuffer = new StringBuilder();
        this.lastLoadTimeAuditLog = currentTime;
        this.auditLogNum = 0;
    }

    public void processAuditEvents() throws Exception {
        AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
        if (event != null) {
            assembleAudit(event);
        }
        // process all audit logs
        loadIfNecessary(false);
    }

    // for ut
    public int getFailedQueueSize() {
        return assistantOfLoadWorker.failedAuditBatchQueue.size();
    }

    // for ut
    public void reLoadFailedAuditData() {
        assistantOfLoadWorker.processFailedBatch();
    }

    private class LoadWorker implements Runnable {

        public LoadWorker() {
        }

        public void run() {
            while (!isClosed) {
                try {
                    processAuditEvents();
                } catch (Exception e) {
                    LOG.error("encounter exception when loading current audit batch", e);
                }
            }
        }
    }

    private class AssistantOfLoadWorker extends Thread {
        private static final int MAX_RETRY_TIME_S = 600;
        private static final int RETRY_INTERVAL_S = 5;

        private final AtomicLong failedDataBytes = new AtomicLong(0);
        private final ConcurrentLinkedDeque<AuditLogBatch> failedAuditBatchQueue = new ConcurrentLinkedDeque<>();

        public AssistantOfLoadWorker(String name) {
            super(name);
        }

        public void addLoadFailAuditData(String data, int discardCount) {
            AuditLogBatch batch = new AuditLogBatch(data, discardCount);
            failedAuditBatchQueue.add(batch);
            failedDataBytes.addAndGet(data.length());
            LOG.info("add failed audit data to queue");
        }

        @Override
        public void run() {
            while (!isClosed) {
                try {
                    processFailedBatch();
                } catch (Exception e) {
                    LOG.error("Failed to process audit logs batch", e);
                }
            }
        }

        private void processFailedBatch() {
            AuditLogBatch batch = failedAuditBatchQueue.poll();
            if (batch == null) {
                return;
            }

            try {
                load(batch.data);
                LOG.info("Successfully loaded previously failed audit log");
            } catch (Exception e) {
                handleLoadFailure(batch, e);
            }
        }

        private void handleLoadFailure(AuditLogBatch batch, Exception e)  {
            if (shouldDiscardBatch(batch)) {
                failedDataBytes.addAndGet(-batch.data.length());
                discardLogNum += batch.discardCount;
                MetricRepo.COUNTER_AUDIT_LOG_DISCARD_NUM.update(batch.discardCount);
                LOG.warn("discarded {}/{} audit log", batch.discardCount, discardLogNum);
                return;
            }
            failedAuditBatchQueue.addFirst(batch);
            LOG.warn("Failed to load audit logs, will retry", e);
            try {
                TimeUnit.SECONDS.sleep(RETRY_INTERVAL_S);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean shouldDiscardBatch(AuditLogBatch batch) {
            return System.currentTimeMillis() - batch.timestamp > MAX_RETRY_TIME_S * 1000L
                || failedDataBytes.get() > 3 * GlobalVariable.auditPluginMaxBatchBytes;
        }

        private class AuditLogBatch {
            public String data;
            public long discardCount;
            public long timestamp;

            public AuditLogBatch(String data, int discardCount) {
                this.data = data;
                this.discardCount = discardCount;
                this.timestamp = System.currentTimeMillis();
            }
        }
    }
}

