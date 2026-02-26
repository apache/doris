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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cleans up Hadoop metrics2 registrations via reflection to prevent memory leak
 * in multi-ClassLoader environments.
 *
 * Each Hadoop FileSystem registers metrics with its ClassLoader's DefaultMetricsSystem singleton.
 * These metrics (MetricCounterLong, MBeanAttributeInfo, etc.) are never unregistered on close(),
 * causing unbounded growth. Since each ClassLoader has its own DefaultMetricsSystem enum instance,
 * a single cleanup point cannot cover all ClassLoaders.
 *
 * This class clears the MetricsSystemImpl.sources map and unregisters associated MBeans
 * after each FileSystem creation, without calling shutdown() which would break the
 * MetricsSystem's ability to inject metric fields into FileSystem instances.
 */
public class HadoopMetricsDisabler {

    private static final Logger LOG = LogManager.getLogger(HadoopMetricsDisabler.class);

    /**
     * Clean up accumulated Hadoop metrics2 sources and MBeans for the given ClassLoader.
     * This unregisters MBeans and clears the sources map, but keeps the MetricsSystem
     * in a functional state so FileSystem metric fields (MutableRate, etc.) remain valid.
     *
     * Must be called after each FileSystem.get() because each creation registers new metrics.
     *
     * @param cl the ClassLoader whose DefaultMetricsSystem should be cleaned up
     */
    public static void disable(ClassLoader cl) {
        if (cl == null) {
            return;
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
                LOG.info("MetricsSystem impl is null for ClassLoader: {}", cl);
                return;
            }

            LOG.info("MetricsSystem impl type: {}, ClassLoader: {}", metricsSystemImpl.getClass().getName(), cl);

            // Get the sources map: Map<String, MetricsSourceAdapter>
            Field sourcesField = metricsSystemImpl.getClass().getDeclaredField("sources");
            sourcesField.setAccessible(true);
            Map<?, ?> sources = (Map<?, ?>) sourcesField.get(metricsSystemImpl);

            int sizeBefore = sources.size();

            // Synchronize on MetricsSystemImpl because Hadoop's sources map is a LinkedHashMap
            // and all access in MetricsSystemImpl.register()/unregisterSource() is synchronized(this).
            synchronized (metricsSystemImpl) {
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
            }

            LOG.info("Cleaned up Hadoop metrics2: sources {} -> 0, ClassLoader: {}", sizeBefore, cl);
        } catch (ClassNotFoundException e) {
            // Hadoop metrics2 not present in this ClassLoader, nothing to do
        } catch (Exception e) {
            LOG.warn("Failed to clean up Hadoop metrics2 for ClassLoader: {}", cl, e);
        }
    }
}
