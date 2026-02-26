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

package org.apache.doris.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodically cleans up Hadoop metrics2 registrations via reflection to prevent memory leak
 * in multi-ClassLoader environments.
 *
 * Root cause: Trino creates HdfsClassLoader instances (parent = PlatformClassLoader) that each
 * load their own DefaultMetricsSystem. FileSystem instances created under these ClassLoaders
 * register metrics (MetricCounterLong, MBeanAttributeInfo, etc.) that are never unregistered,
 * causing unbounded growth.
 *
 * Fix: A periodic task scans all thread ContextClassLoaders, and for each unique ClassLoader
 * clears the MetricsSystemImpl.sources map and unregisters associated MBeans.
 */
public class HadoopMetricsDisabler {

    private static final Logger LOG = LogManager.getLogger(HadoopMetricsDisabler.class);
    private static final long CLEANUP_INTERVAL_SECONDS = 300;
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static ScheduledExecutorService cleanupExecutor;

    /**
     * Starts a periodic background task that scans all thread ContextClassLoaders
     * and cleans up their DefaultMetricsSystem sources.
     */
    public static void startPeriodicCleanup() {
        if (started.compareAndSet(false, true)) {
            cleanupExecutor = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "hadoop-metrics-cleanup");
                t.setDaemon(true);
                return t;
            });
            cleanupExecutor.scheduleAtFixedRate(() -> {
                try {
                    Set<ClassLoader> classLoaders = collectAllClassLoaders();
                    int cleaned = 0;
                    for (ClassLoader cl : classLoaders) {
                        if (disable(cl)) {
                            cleaned++;
                        }
                    }
                    LOG.info("Hadoop metrics2 cleanup: scanned {} ClassLoaders, cleaned {}",
                            classLoaders.size(), cleaned);
                } catch (Throwable e) {
                    LOG.warn("Periodic Hadoop metrics2 cleanup failed", e);
                }
            }, CLEANUP_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, TimeUnit.SECONDS);
            LOG.info("Started periodic Hadoop metrics2 cleanup, interval={}s", CLEANUP_INTERVAL_SECONDS);
        }
    }

    /**
     * Collect unique ClassLoaders from all threads' ContextClassLoader chains.
     */
    private static Set<ClassLoader> collectAllClassLoaders() {
        Set<ClassLoader> classLoaders = new HashSet<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            ClassLoader cl = t.getContextClassLoader();
            while (cl != null) {
                if (!classLoaders.add(cl)) {
                    break;
                }
                cl = cl.getParent();
            }
        }
        return classLoaders;
    }

    /**
     * Clean up accumulated Hadoop metrics2 sources and MBeans for the given ClassLoader.
     *
     * @param cl the ClassLoader whose DefaultMetricsSystem should be cleaned up
     * @return true if any sources were cleaned up
     */
    public static boolean disable(ClassLoader cl) {
        if (cl == null) {
            return false;
        }
        try {
            Class<?> dmsClass = Class.forName(
                    "org.apache.hadoop.metrics2.lib.DefaultMetricsSystem", true, cl);
            Object instance = dmsClass.getField("INSTANCE").get(null);

            Field implField = dmsClass.getDeclaredField("impl");
            implField.setAccessible(true);
            AtomicReference<?> implRef = (AtomicReference<?>) implField.get(instance);
            Object metricsSystemImpl = implRef.get();
            if (metricsSystemImpl == null) {
                return false;
            }

            // Get the sources map: Map<String, MetricsSourceAdapter>
            Field sourcesField = metricsSystemImpl.getClass().getDeclaredField("sources");
            sourcesField.setAccessible(true);
            Map<?, ?> sources = (Map<?, ?>) sourcesField.get(metricsSystemImpl);

            // Synchronize on MetricsSystemImpl because Hadoop's sources map is a LinkedHashMap
            // and all access in MetricsSystemImpl.register()/unregisterSource() is synchronized(this).
            synchronized (metricsSystemImpl) {
                int sizeBefore = sources.size();
                if (sizeBefore == 0) {
                    return false;
                }
                for (Object adapter : sources.values()) {
                    try {
                        Method closeMethod = adapter.getClass().getDeclaredMethod("close");
                        closeMethod.setAccessible(true);
                        closeMethod.invoke(adapter);
                    } catch (Exception e) {
                        // best-effort per adapter
                    }
                }
                sources.clear();
                LOG.info("Cleaned up Hadoop metrics2: sources {} -> 0, ClassLoader: {}", sizeBefore, cl);
            }

            return true;
        } catch (ClassNotFoundException e) {
            // Hadoop metrics2 not present in this ClassLoader, nothing to do
            return false;
        } catch (Exception e) {
            LOG.warn("Failed to clean up Hadoop metrics2 for ClassLoader: {}", cl, e);
            return false;
        }
    }
}
