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
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.extension.spi.PluginContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processor that queues lineage events and dispatches them to lineage plugins.
 * <p>
 * Plugins are discovered via two mechanisms (aligned with
 * {@code AuthenticationPluginManager} pattern):
 * <ol>
 *   <li>Built-in: {@link ServiceLoader} on classpath</li>
 *   <li>External: {@link DirectoryPluginRuntimeManager} from
 *       {@code $plugin_dir/lineage/} directory</li>
 * </ol>
 * </p>
 */
public class LineageEventProcessor {

    private static final Logger LOG = LogManager.getLogger(LineageEventProcessor.class);
    private static final long EVENT_POLL_TIMEOUT_SECONDS = 5L;

    /** Parent-first prefixes for child-first classloading isolation. */
    private static final List<String> LINEAGE_PARENT_FIRST_PREFIXES =
            Collections.singletonList("org.apache.doris.nereids.lineage.");

    private final AtomicReference<List<LineagePlugin>> lineagePlugins = new AtomicReference<>(Collections.emptyList());
    private final BlockingQueue<LineageInfo> eventQueue =
            new LinkedBlockingDeque<>(Config.lineage_event_queue_size);
    private final AtomicBoolean isInit = new AtomicBoolean(false);

    /** Factory registry by plugin name (like AuthenticationPluginManager.factories). */
    private final Map<String, LineagePluginFactory> factories = new ConcurrentHashMap<>();
    private final DirectoryPluginRuntimeManager<LineagePluginFactory> runtimeManager =
            new DirectoryPluginRuntimeManager<>();

    private Thread workerThread;

    /**
     * Create a lineage event processor.
     */
    public LineageEventProcessor() {
    }

    /**
     * Start the background worker thread.
     */
    public void start() {
        if (!isInit.compareAndSet(false, true)) {
            return;
        }
        discoverPlugins();
        workerThread = new Thread(new Worker(), "LineageEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Discover lineage plugins via dual mechanism:
     * 1. ServiceLoader for built-in (classpath) factories
     * 2. DirectoryPluginRuntimeManager for external (directory) plugins
     *
     * <p>Aligned with {@code AuthenticationPluginManager} pattern.
     */
    private void discoverPlugins() {
        // 1. Built-in discovery (classpath ServiceLoader)
        try {
            ServiceLoader<LineagePluginFactory> serviceLoader = ServiceLoader.load(LineagePluginFactory.class);
            Iterator<LineagePluginFactory> iterator = serviceLoader.iterator();
            while (true) {
                LineagePluginFactory factory;
                try {
                    if (!iterator.hasNext()) {
                        break;
                    }
                    factory = iterator.next();
                } catch (ServiceConfigurationError e) {
                    LOG.warn("Failed to load built-in lineage plugin factory from ServiceLoader, skip provider", e);
                    continue;
                }
                String pluginName = safeFactoryName(factory);
                if (pluginName.isEmpty()) {
                    LOG.warn("Skip built-in lineage plugin factory with empty name: {}",
                            factory == null ? "null" : factory.getClass().getName());
                    continue;
                }
                LineagePluginFactory existing = factories.putIfAbsent(pluginName, factory);
                if (existing != null) {
                    LOG.warn("Skip duplicated built-in lineage plugin name: {}", pluginName);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to discover built-in lineage plugin factories via ServiceLoader", e);
        }

        // 2. External discovery (plugin_dir/lineage/ directory)
        try {
            List<Path> pluginRoots = Collections.singletonList(
                    Paths.get(Config.plugin_dir, "lineage"));
            ClassLoadingPolicy policy = new ClassLoadingPolicy(LINEAGE_PARENT_FIRST_PREFIXES);
            LoadReport<LineagePluginFactory> report = runtimeManager.loadAll(
                    pluginRoots, getClass().getClassLoader(),
                    LineagePluginFactory.class, policy);

            for (LoadFailure failure : report.getFailures()) {
                LOG.warn("Skip lineage plugin directory due to load failure:"
                                + " pluginDir={}, stage={}, message={}, cause={}",
                        failure.getPluginDir(), failure.getStage(),
                        failure.getMessage(), failure.getCause());
            }

            for (PluginHandle<LineagePluginFactory> handle : report.getSuccesses()) {
                String pluginName = handle.getPluginName();
                LineagePluginFactory existing = factories.putIfAbsent(pluginName, handle.getFactory());
                if (existing != null) {
                    LOG.warn("Skip duplicated lineage plugin name: {} from directory {}", pluginName,
                            handle.getPluginDir());
                } else {
                    LOG.info("Loaded external lineage plugin factory: name={}, pluginDir={}, jarCount={}",
                            pluginName, handle.getPluginDir(), handle.getResolvedJars().size());
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to discover external lineage plugins from plugin directory", e);
        }

        // 3. Create and initialize plugin instances from all discovered factories
        Set<String> activeNames = new HashSet<>(Arrays.asList(Config.activate_lineage_plugin));
        List<LineagePlugin> plugins = new ArrayList<>();
        for (Map.Entry<String, LineagePluginFactory> entry : factories.entrySet()) {
            String pluginName = entry.getKey();
            if (!activeNames.isEmpty() && !activeNames.contains(pluginName)) {
                LOG.info("Skip lineage plugin not in activate_lineage_plugin: {}", pluginName);
                continue;
            }
            try {
                Map<String, String> props = new HashMap<>();
                props.put("plugin.path", resolvePluginPath(pluginName));
                props.put("plugin.name", pluginName);
                PluginContext context = new PluginContext(props);
                LineagePlugin plugin = entry.getValue().create(context);
                if (plugin != null) {
                    plugin.initialize(context);
                    plugins.add(plugin);
                    LOG.info("Loaded lineage plugin: {}, pluginPath={}", pluginName, props.get("plugin.path"));
                }
            } catch (Exception e) {
                LOG.warn("Failed to create/initialize lineage plugin: {}", pluginName, e);
            }
        }
        initPlugins(plugins);
    }

    private String safeFactoryName(LineagePluginFactory factory) {
        if (factory == null) {
            return "";
        }
        try {
            String name = factory.name();
            return name == null ? "" : name.trim();
        } catch (Throwable t) {
            LOG.warn("Failed to get lineage plugin factory name, skip factory class={}",
                    factory.getClass().getName(), t);
            return "";
        }
    }

    /**
     * Resolve plugin path: prefer the directory from DirectoryPluginRuntimeManager,
     * fallback to convention path.
     */
    private String resolvePluginPath(String pluginName) {
        return runtimeManager.get(pluginName)
                .map(handle -> handle.getPluginDir().toString())
                .orElse(Config.plugin_dir + "/lineage/" + pluginName);
    }

    /**
     * Update the active lineage plugin list.
     */
    public void initPlugins(List<LineagePlugin> plugins) {
        List<LineagePlugin> safePlugins = plugins == null ? Collections.emptyList() : plugins;
        lineagePlugins.set(safePlugins);
        if (safePlugins.isEmpty()) {
            clearPendingEvents();
        }
    }

    /**
     * Returns true when at least one loaded plugin is currently willing to receive lineage events.
     */
    public boolean hasActivePlugins() {
        List<LineagePlugin> plugins = lineagePlugins.get();
        if (plugins.isEmpty()) {
            return false;
        }
        for (LineagePlugin plugin : plugins) {
            if (plugin == null) {
                continue;
            }
            try {
                if (plugin.eventFilter()) {
                    return true;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to evaluate lineage plugin event filter: {}", plugin.getClass().getName(), t);
            }
        }
        return false;
    }

    private void clearPendingEvents() {
        int dropped = 0;
        while (eventQueue.poll() != null) {
            dropped++;
        }
        if (dropped > 0) {
            LOG.warn("Lineage event queue cleared because no active plugins. dropped={}", dropped);
        }
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
                try {
                    lineageInfo = eventQueue.poll(EVENT_POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (lineageInfo == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("encounter exception when getting lineage event from queue, ignore", e);
                    continue;
                }
                List<LineagePlugin> currentPlugins = lineagePlugins.get();
                for (LineagePlugin lineagePlugin : currentPlugins) {
                    try {
                        if (lineagePlugin == null) {
                            continue;
                        }
                        if (!lineagePlugin.eventFilter()) {
                            continue;
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Lineage plugin start: plugin={}, queryId={}",
                                    lineagePlugin.name(), getQueryId(lineageInfo));
                        }
                        lineagePlugin.exec(lineageInfo);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Lineage plugin end: plugin={}, queryId={}",
                                    lineagePlugin.name(), getQueryId(lineageInfo));
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
