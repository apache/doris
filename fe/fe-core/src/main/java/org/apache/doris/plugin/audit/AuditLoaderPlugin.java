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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    public static final String AUDIT_LOG_TABLE = "audit_log";

    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

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

    public AuditLoaderPlugin() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLoader", PluginType.AUDIT,
                "builtin audit loader, to load audit log to internal table", DigitalVersion.fromString("2.1.0"),
                DigitalVersion.fromString("1.8.31"), AuditLoaderPlugin.class.getName(), null, null);
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
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "audit loader thread");
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
        logBuffer.append(event.stmtId).append("\t");
        logBuffer.append(event.stmtType).append("\t");
        logBuffer.append(event.isQuery ? 1 : 0).append("\t");
        logBuffer.append(event.feIp).append("\t");
        logBuffer.append(event.cpuTimeMs).append("\t");
        logBuffer.append(event.sqlHash).append("\t");
        logBuffer.append(event.sqlDigest).append("\t");
        logBuffer.append(event.peakMemoryBytes).append("\t");
        logBuffer.append(event.workloadGroup).append("\t");
        // already trim the query in org.apache.doris.qe.AuditLogHelper#logAuditLog
        String stmt = event.stmt;
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive audit event with stmt: {}", stmt);
        }
        logBuffer.append(stmt).append("\n");
    }

    private void loadIfNecessary(AuditStreamLoader loader) {
        long currentTime = System.currentTimeMillis();

        if (auditLogBuffer.length() >= GlobalVariable.auditPluginMaxBatchBytes
                || currentTime - lastLoadTimeAuditLog >= GlobalVariable.auditPluginMaxBatchInternalSec * 1000) {
            // begin to load
            try {
                String token = "";
                try {
                    // Acquire token from master
                    token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
                } catch (Exception e) {
                    LOG.warn("Failed to get auth token: {}", e);
                    discardLogNum += auditLogNum;
                    return;
                }
                AuditStreamLoader.LoadResponse response = loader.loadBatch(auditLogBuffer, token);
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
        this.auditLogBuffer = new StringBuilder();
        this.lastLoadTimeAuditLog = currentTime;
        this.auditLogNum = 0;
    }

    private class LoadWorker implements Runnable {
        private AuditStreamLoader loader;

        public LoadWorker(AuditStreamLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                        // process all audit logs
                        loadIfNecessary(loader);
                    }
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
}
