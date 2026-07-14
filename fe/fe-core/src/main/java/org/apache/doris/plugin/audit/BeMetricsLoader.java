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
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr.BeMetrics;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Built-in audit plugin that persists per-BE resource consumption metrics
 * to the __internal_schema.query_be_metrics table.
 *
 * Data flow:
 *   AuditEvent.beMetricsMap (set by WorkloadRuntimeStatusMgr)
 *     -> exec() enqueues event
 *     -> LoadWorker calls assembleRow() to build delimiter-separated rows
 *     -> loadIfNecessary() triggers BeMetricsStreamLoader HTTP PUT
 *
 * Runs in parallel with AuditLoader/AuditLogBuilder via AuditEventProcessor.
 */
public class BeMetricsLoader extends Plugin implements AuditPlugin {

    private static final Logger LOG = LogManager.getLogger(BeMetricsLoader.class);

    public static final String BE_METRICS_TABLE = "query_be_metrics";

    public static final char BE_METRICS_COL_SEPARATOR = 0x1F;
    public static final char BE_METRICS_LINE_DELIMITER = 0x1E;
    public static final String BE_METRICS_COL_SEPARATOR_STR = "\\x1F";
    public static final String BE_METRICS_LINE_DELIMITER_STR = "\\x1E";

    private StringBuilder buffer = new StringBuilder();
    private int bufferedRowCount = 0;
    private long lastLoadTime = 0;
    private long discardRowNum = 0;

    private BlockingQueue<AuditEvent> eventQueue;
    private BeMetricsStreamLoader streamLoader;
    private Thread loadThread;

    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    private final PluginInfo pluginInfo;

    public BeMetricsLoader() {
        pluginInfo = new PluginInfo(
            PluginMgr.BUILTIN_PLUGIN_PREFIX + "BeMetricsLoader",
            PluginType.AUDIT,
            "builtin be metrics loader, to load per-BE resource metrics to internal table",
            DigitalVersion.fromString("2.1.0"),
            DigitalVersion.fromString("1.8.31"),
            BeMetricsLoader.class.getName(),
            null, null);
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
            this.lastLoadTime = System.currentTimeMillis();
            this.eventQueue = Queues.newLinkedBlockingDeque(100000);
            this.streamLoader = new BeMetricsStreamLoader();
            this.loadThread = new Thread(new LoadWorker(), "be-metrics-loader");
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
                    LOG.debug("encounter exception when closing the be metrics loader", e);
                }
            }
        }
    }

    @Override
    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    @Override
    public void exec(AuditEvent event) {
        if (!GlobalVariable.enableBeMetricsLoader) {
            return;
        }
        Map<Long, BeMetrics> beMetricsMap = event.beMetricsMap;
        if (beMetricsMap == null || beMetricsMap.isEmpty()) {
            return;
        }
        try {
            eventQueue.add(event);
        } catch (Exception e) {
            ++discardRowNum;
            if (LOG.isDebugEnabled()) {
                LOG.debug("encounter exception when putting be metrics event,"
                    + " total discard: {}", discardRowNum, e);
            }
        }
    }

    /**
     * Convert one AuditEvent into N delimiter-separated rows (one per BE).
     * Column order MUST match InternalSchema.QUERY_BE_METRICS_SCHEMA:
     *   query_id, be_id, time, scan_rows, scan_bytes, cpu_ms,
     *   peak_memory_bytes, shuffle_send_rows, shuffle_send_bytes,
     *   scan_bytes_from_local_storage, scan_bytes_from_remote_storage
     */
    private void assembleRow(AuditEvent event) {
        Map<Long, BeMetrics> beMetricsMap = event.beMetricsMap;
        if (beMetricsMap == null) {
            return;
        }
        String timeStr = TimeUtils.longToTimeStringWithms(event.timestamp);
        for (Map.Entry<Long, BeMetrics> entry : beMetricsMap.entrySet()) {
            Long beId = entry.getKey();
            BeMetrics m = entry.getValue();
            buffer.append(event.queryId).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(beId).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(timeStr).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.scan_rows).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.scan_bytes).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.cpu_ms).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.max_peak_memory_bytes).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.shuffle_send_rows).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.shuffle_send_bytes).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.scan_bytes_from_local_storage).append(BE_METRICS_COL_SEPARATOR);
            buffer.append(m.scan_bytes_from_remote_storage).append(BE_METRICS_LINE_DELIMITER);
            ++bufferedRowCount;
        }
    }

    public synchronized void loadIfNecessary(boolean force) {
        long currentTime = System.currentTimeMillis();
        if (buffer.length() != 0 && (force
            || buffer.length() >= GlobalVariable.beMetricsPluginMaxBatchBytes
            || currentTime - lastLoadTime
            >= GlobalVariable.beMetricsPluginMaxBatchInternalSec * 1000)) {
            try {
                String token = "";
                try {
                    token = Env.getCurrentEnv().getTokenManager().acquireToken();
                } catch (Exception e) {
                    LOG.warn("BeMetricsLoader failed to get auth token: {}", e.getMessage());
                    discardRowNum += bufferedRowCount;
                    return;
                }
                BeMetricsStreamLoader.LoadResponse response = streamLoader.loadBatch(buffer, token);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("be metrics loader response: {}", response);
                }
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when loading be metrics batch,"
                        + " discard current batch", e);
                }
                discardRowNum += bufferedRowCount;
            } finally {
                resetBatch(currentTime);
                if (discardRowNum > 0) {
                    LOG.info("num of total discarded be metrics rows: {}", discardRowNum);
                }
            }
        }
    }

    private void resetBatch(long currentTime) {
        this.buffer = new StringBuilder();
        this.lastLoadTime = currentTime;
        this.bufferedRowCount = 0;
    }

    private class LoadWorker implements Runnable {
        @Override
        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleRow(event);
                    }
                    loadIfNecessary(false);
                } catch (InterruptedException ie) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when loading be metrics batch", ie);
                    }
                } catch (Exception e) {
                    LOG.error("run be metrics loader error:", e);
                }
            }
        }
    }
}
