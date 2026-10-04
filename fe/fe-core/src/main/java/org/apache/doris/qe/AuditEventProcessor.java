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

import org.apache.doris.common.Config;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import com.google.common.base.Strings;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

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

    private BlockingQueue<AuditEvent> eventQueue = Queues.newLinkedBlockingDeque();
    private Thread workerThread;

    /**
     * How long an idle worker waits outside the in-flight lock before re-checking the
     * queue: short enough to keep the pickup latency of a submitted event negligible,
     * long enough not to spin.
     */
    private static final long QUEUE_POLL_INTERVAL_MILLIS = 100L;

    /**
     * Guards {@link #processingEvent} TOGETHER with {@link #eventQueue}: the
     * queue-to-in-flight transfer, the in-flight release and the fence read are one
     * linearized state (round-37 #2). Producers never take it (the queue has its own
     * lock), so a busy fence reader can never block an event submission, and the worker
     * only holds it for the transfer / release - its idle wait happens outside.
     */
    private final Object inFlightLock = new Object();

    /**
     * The event the worker has DEQUEUED and is currently handing to the audit plugins,
     * or null between events. A plugin (​{@link AuditLogBuilder}, the builtin audit
     * loader, ...) runs with the event OUT of the queue, so a horizon built from the
     * queue alone would report "nothing outstanding" while an accepted event is still
     * unpublished (round-36 #3). Written and read under {@link #inFlightLock}.
     */
    private volatile AuditEvent processingEvent;

    private volatile boolean isStopped = false;

    private Set<String> skipAuditUsers = Sets.newHashSet();

    public AuditEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void start() {
        initSkipAuditUsers();
        workerThread = new Thread(new Worker(), "AuditEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    private void initSkipAuditUsers() {
        if (Strings.isNullOrEmpty(Config.skip_audit_user_list)) {
            return;
        }
        String[] users = Config.skip_audit_user_list.replaceAll(" ", "").split(",");
        for (String user : users) {
            skipAuditUsers.add(user);
        }
        LOG.info("skip audit users: {}", skipAuditUsers);
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

    /**
     * Start time (epoch millis, the {@code time} column of {@code audit_log}) of the
     * OLDEST audit event this processor has QUEUED or is currently processing, 0 when it
     * has neither. Part of the SPM capture's publication fence: a completed query enters
     * this queue before any audit loader sees it, and a plugin can stall while its event
     * is already dequeued (round-36 #3).
     *
     * <p>The queue and the in-flight slot are read as ONE atomic snapshot (round-37 #2):
     * the worker moves an event from the queue into the in-flight slot under the same
     * lock, so the read can never see the event in neither and report "nothing
     * outstanding" for an accepted-but-unpublished event.
     *
     * <p>INTERNAL events are excluded: the capture only scans {@code is_internal = false}
     * rows, so an internal statement (e.g. the horizon reporter's own SQL) must not keep
     * fencing progress (round-37 #7).
     */
    public long oldestQueuedOrInFlightEventTime() {
        long oldest = 0;
        synchronized (inFlightLock) {
            AuditEvent processing = processingEvent;
            if (processing != null && !processing.isInternal && processing.timestamp > 0) {
                oldest = processing.timestamp;
            }
            for (AuditEvent event : eventQueue) {
                if (event == null || event.isInternal || event.timestamp <= 0) {
                    continue;
                }
                if (oldest == 0 || event.timestamp < oldest) {
                    oldest = event.timestamp;
                }
            }
        }
        return oldest;
    }

    public boolean handleAuditEvent(AuditEvent auditEvent) {
        if (skipAuditUsers.contains(auditEvent.user)) {
            // return true to ignore this event
            return true;
        }
        boolean isAddSucc = true;
        try {
            if (eventQueue.size() >= Config.audit_event_log_queue_size) {
                isAddSucc = false;
                LOG.warn("the audit event queue is full with size {}, discard the audit event: {}",
                        eventQueue.size(), auditEvent.queryId);
            } else {
                eventQueue.add(auditEvent);
            }
        } catch (Exception e) {
            isAddSucc = false;
            LOG.warn("encounter exception when handle audit event {}, discard the event",
                    auditEvent.queryId, e);
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

                synchronized (inFlightLock) {
                    auditEvent = eventQueue.poll();
                    if (auditEvent != null) {
                        // The dequeue and the in-flight publication are ONE step: a fence
                        // read must never observe the event in NEITHER the queue (poll
                        // already returned) NOR the in-flight slot (not yet assigned)
                        // (round-37 #2).
                        processingEvent = auditEvent;
                    }
                }
                if (auditEvent == null) {
                    // Nothing queued: wait OUTSIDE the lock so the fence read is never
                    // blocked by an idle worker; the pickup latency of an arriving event
                    // is bounded by QUEUE_POLL_INTERVAL_MILLIS.
                    try {
                        Thread.sleep(QUEUE_POLL_INTERVAL_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.warn("encounter exception when getting audit event from queue, ignore", e);
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
                    LOG.warn("encounter exception when processing audit events. ignore", e);
                } finally {
                    // The plugins have run: the builtin loader accepted the event (its own
                    // fence covers it from the enqueue) or every plugin filtered it out
                    // (it will never be published). Only now does the event stop being
                    // in flight - atomically with the read that looks for it.
                    synchronized (inFlightLock) {
                        processingEvent = null;
                    }
                }
            }
        }

    }
}
