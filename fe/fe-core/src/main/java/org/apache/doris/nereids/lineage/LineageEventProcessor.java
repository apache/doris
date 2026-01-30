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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Processor that queues lineage events and dispatches them to lineage plugins.
 */
public class LineageEventProcessor {

    private static final Logger LOG = LogManager.getLogger(LineageEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min
    private final PluginMgr pluginMgr;
    private final AtomicReference<List<Plugin>> lineagePlugins = new AtomicReference<>(Collections.emptyList());
    private volatile long lastUpdateTime = 0;
    private final BlockingQueue<LineageInfo> eventQueue =
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
        refreshPlugins(pluginMgr.getActivePluginList(PluginType.LINEAGE));
        workerThread = new Thread(new Worker(), "LineageEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void refreshPlugins(List<Plugin> plugins) {
        List<Plugin> safePlugins = plugins == null ? Collections.emptyList() : plugins;
        lineagePlugins.set(safePlugins);
        lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * Submit a lineage event to the processing queue.
     *
     * @param lineageInfo lineage info to submit
     * @return true if accepted, false otherwise
     */
    public boolean submitLineageEvent(LineageInfo lineageInfo) {
        if (lineageInfo == null) {
            return false;
        }
        try {
            if (!eventQueue.offer(lineageInfo)) {
                String queryId = getQueryId(lineageInfo);
                LOG.warn("the lineage event queue is full with size {}, discard the lineage event: {}",
                        eventQueue.size(), queryId);
                return false;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lineage event enqueued: queryId={}, queueSize={}",
                        getQueryId(lineageInfo), eventQueue.size());
            }
            return true;
        } catch (Exception e) {
            String queryId = getQueryId(lineageInfo);
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
            LineageInfo lineageInfo;
            while (true) {
                // update lineage plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                List<Plugin> currentPlugins = lineagePlugins.get();
                if (System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    refreshPlugins(pluginMgr.getActivePluginList(PluginType.LINEAGE));
                    currentPlugins = lineagePlugins.get();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("update lineage plugins. num: {}, names: {}", currentPlugins.size(),
                                currentPlugins.stream().map(plugin -> {
                                            if (plugin instanceof AbstractLineagePlugin) {
                                                return ((AbstractLineagePlugin) plugin).getName();
                                            } else {
                                                return "";
                                            }
                                        }
                                ).collect(Collectors.toList()));
                    }
                }

                try {
                    lineageInfo = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (lineageInfo == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("encounter exception when getting lineage event from queue, ignore", e);
                    continue;
                }
                for (Plugin plugin : currentPlugins) {
                    try {
                        AbstractLineagePlugin lineagePlugin = (AbstractLineagePlugin) plugin;
                        if (lineageInfo == null) {
                            LOG.warn("lineage info is null for event {}, skip", getQueryId(lineageInfo));
                            continue;
                        }
                        if (!lineagePlugin.eventFilter()) {
                            continue;
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Lineage plugin start: plugin={}, queryId={}",
                                    lineagePlugin.getName(), getQueryId(lineageInfo));
                        }
                        lineagePlugin.exec(lineageInfo);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Lineage plugin end: plugin={}, queryId={}",
                                    lineagePlugin.getName(), getQueryId(lineageInfo));
                        }
                    } catch (Throwable e) {
                        LOG.warn("encounter exception when processing lineage event {}, ignore",
                                getQueryId(lineageInfo), e);
                    }
                }
            }
        }
    }

    private static String getQueryId(LineageInfo lineageInfo) {
        if (lineageInfo == null) {
            return "";
        }
        LineageContext context = lineageInfo.getContext();
        return context == null ? "" : context.getQueryId();
    }
}
