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

package org.apache.doris.qe;

import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.plugin.audit.AuditEvent;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class for processing all audit events.
 * It will receive audit events and handle them to all AUDIT type plugins.
 */
public class AuditEventProcessor {
    private static final Logger LOG = LogManager.getLogger(AuditEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min

    private PluginMgr pluginMgr;

    private List<Plugin> auditPlugins;
    private long lastUpdateTime = 0;

    private BlockingQueue<AuditEvent> eventQueue = Queues.newLinkedBlockingDeque(10000);
    private Thread workerThread;

    private volatile boolean isStopped = false;

    public AuditEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void start() {
        workerThread = new Thread(new Worker(), "AuditEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void stop() {
        isStopped = true;
        if (workerThread != null) {
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                LOG.warn("join worker join failed.", e);
            }
        }
    }

    public boolean handleAuditEvent(AuditEvent auditEvent) {
        return handleAuditEvent(auditEvent, false);
    }

    public boolean handleAuditEvent(AuditEvent auditEvent, boolean ignoreQueueFullLog) {
        boolean isAddSucc = true;
        try {
            eventQueue.add(auditEvent);
        } catch (Exception e) {
            isAddSucc = false;
            if (!ignoreQueueFullLog) {
                LOG.warn("encounter exception when handle audit event {}, ignore", auditEvent.type, e);
            }
        }
        return isAddSucc;
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            AuditEvent auditEvent;
            while (!isStopped) {
                // update audit plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                // because some plugins may be installed or uninstalled at runtime.
                if (auditPlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    auditPlugins = pluginMgr.getActivePluginList(PluginType.AUDIT);
                    lastUpdateTime = System.currentTimeMillis();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("update audit plugins. num: {}", auditPlugins.size());
                    }
                }

                try {
                    auditEvent = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (auditEvent == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when getting audit event from queue, ignore", e);
                    }
                    continue;
                }

                try {
                    for (Plugin plugin : auditPlugins) {
                        if (((AuditPlugin) plugin).eventFilter(auditEvent.type)) {
                            ((AuditPlugin) plugin).exec(auditEvent);
                        }
                    }
                } catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when processing audit event.", e);
                    }
                }
            }
        }

    }
}
