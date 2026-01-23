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

package org.apache.doris.nereids.lineage;

import org.apache.doris.common.Config;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.plugin.lineage.AbstractLineagePlugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor that queues lineage events and dispatches them to lineage plugins.
 */
public class LineageEventProcessor {

    private static final Logger LOG = LogManager.getLogger(LineageEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min
    private final PluginMgr pluginMgr;
    private List<Plugin> lineagePlugins;
    private long lastUpdateTime = 0;
    private final BlockingQueue<LineageEvent> eventQueue =
            new LinkedBlockingDeque<>(Config.lineage_event_queue_size);
    private final AtomicBoolean isInit = new AtomicBoolean(false);
    private Thread workerThread;

    /**
     * Create a lineage event processor.
     *
     * @param pluginMgr plugin manager to load lineage plugins
     */
    public LineageEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    /**
     * Start the background worker thread.
     */
    public void start() {
        if (!isInit.compareAndSet(false, true)) {
            return;
        }
        workerThread = new Thread(new Worker(), "LineageEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Submit a lineage event to the processing queue.
     *
     * @param lineageEvent lineage event to submit
     * @return true if accepted, false otherwise
     */
    public boolean submitLineageEvent(LineageEvent lineageEvent) {
        if (lineageEvent == null) {
            return false;
        }
        try {
            if (!eventQueue.offer(lineageEvent)) {
                String queryId = getQueryId(lineageEvent);
                LOG.warn("the lineage event queue is full with size {}, discard the lineage event: {}",
                        eventQueue.size(), queryId);
                return false;
            }
            return true;
        } catch (Exception e) {
            String queryId = getQueryId(lineageEvent);
            LOG.warn("encounter exception when handle lineage event {}, discard the event",
                    queryId, e);
            return false;
        }
    }

    /**
     * Worker that polls events and invokes lineage plugins.
     */
    public class Worker implements Runnable {
        /**
         * Run the lineage processing loop.
         */
        @Override
        public void run() {
            LineageEvent lineageEvent;
            while (true) {
                // update lineage plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                if (lineagePlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    lineagePlugins = pluginMgr.getActivePluginList(PluginType.LINEAGE);
                    lastUpdateTime = System.currentTimeMillis();
                    if (lineagePlugins == null) {
                        lineagePlugins = Collections.emptyList();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("update lineage plugins. num: {}", lineagePlugins.size());
                    }
                }

                try {
                    lineageEvent = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (lineageEvent == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("encounter exception when getting lineage event from queue, ignore", e);
                    continue;
                }
                for (Plugin plugin : lineagePlugins) {
                    try {
                        AbstractLineagePlugin lineagePlugin = (AbstractLineagePlugin) plugin;
                        if (!lineagePlugin.eventFilter()) {
                            continue;
                        }
                        LineageInfo lineageInfo = lineageEvent.getLineageInfo();
                        if (lineageInfo == null) {
                            LOG.warn("lineage info is null for event {}, skip", getQueryId(lineageEvent));
                            continue;
                        }
                        lineagePlugin.exec(lineageInfo);
                    } catch (Throwable e) {
                        LOG.warn("encounter exception when processing lineage event {}, ignore",
                                getQueryId(lineageEvent), e);
                    }
                }
            }
        }
    }

    private static String getQueryId(LineageEvent lineageEvent) {
        if (lineageEvent == null || lineageEvent.getLineageInfo() == null) {
            return "";
        }
        LineageContext context = lineageEvent.getLineageInfo().getContext();
        return context == null ? "" : context.getQueryId();
    }
}
