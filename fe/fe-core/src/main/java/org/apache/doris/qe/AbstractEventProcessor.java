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

import org.apache.doris.plugin.Event;
import org.apache.doris.plugin.Plugin;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public abstract class AbstractEventProcessor<T extends Event> {

    public enum ProcessorType {
        AUDIT_EVENT_PROCESSOR("auditEventProcessor"),
        PROFILE_EVENT_PROCESSOR("profileEventProcessor");

        private String name;

        ProcessorType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final Logger LOG = LogManager.getLogger(AbstractEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min
    private static final int DEFAULT_EVENT_QUEUE_SIZE = 10000;

    public AbstractEventProcessor() {
    }

    private BlockingQueue<T> eventQueue = Queues.newLinkedBlockingDeque(getEventQueueSize());
    private Thread workerThread;

    private volatile boolean isStopped = false;
    // if there isn't any installed plugin, we should skip event build process and handleEvent method
    private volatile boolean needHandle = false;

    public void start() {
        workerThread = new Thread(new Worker(), getType().getName());
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

    public void handleEvent(T event) {
        try {
            eventQueue.add(event);
        } catch (Exception e) {
            LOG.warn("encounter exception when handle event, ignore", e);
        }
    }

    public boolean needHandle() {
        return needHandle;
    }

    // should return a not null value
    protected abstract @NotNull List<Plugin> getPlugins();

    protected abstract ProcessorType getType();

    protected abstract void handle(Plugin plugin, T event);

    protected int getEventQueueSize() {
        return DEFAULT_EVENT_QUEUE_SIZE;
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            long lastUpdateTime = 0;
            T event;
            List<Plugin> plugins = null;
            while (!isStopped) {
                // update plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                // because some of plugins may be installed or uninstalled at runtime.
                if (plugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    try {
                        plugins = getPlugins();
                        if (plugins == null) {
                            LOG.warn("Method getPlugins() of {} returns null", this.getClass().getSimpleName());
                            plugins = Collections.emptyList();
                        }
                    } catch (Exception e) {
                        LOG.warn("Method getPlugins() of {} execute failed", this.getClass().getSimpleName(), e);
                        plugins = Collections.emptyList();
                    }

                    needHandle = !plugins.isEmpty();
                    lastUpdateTime = System.currentTimeMillis();
                    LOG.debug("update audit plugins. num: {}", plugins.size());
                }

                try {
                    event = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (event == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when getting audit event from queue, ignore", e);
                    continue;
                }

                try {
                    for (Plugin plugin : plugins) {
                        AbstractEventProcessor.this.handle(plugin, event);
                    }
                } catch (Exception e) {
                    LOG.debug("encounter exception when processing event.", e);
                }

            }
        }

    }

}
