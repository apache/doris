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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Disables Hadoop metrics2 via reflection to prevent memory leak in multi-ClassLoader environments.
 *
 * Each Hadoop FileSystem registers metrics with its ClassLoader's DefaultMetricsSystem singleton.
 * These metrics (MetricCounterLong, MBeanAttributeInfo, etc.) are never unregistered on close(),
 * causing unbounded growth. Since each ClassLoader has its own DefaultMetricsSystem enum instance,
 * setting NopMetricsSystem on the main ClassLoader doesn't help other ClassLoaders.
 *
 * This class calls MetricsSystemImpl.shutdown() via reflection after each FileSystem creation,
 * which unregisters all MBeans from the JMX MBeanServer and clears the sources map.
 */
public class HadoopMetricsDisabler {

    private static final Logger LOG = LogManager.getLogger(HadoopMetricsDisabler.class);

    /**
     * Shut down Hadoop metrics2 for the given ClassLoader's DefaultMetricsSystem.
     * This unregisters all MBeans and clears accumulated metrics sources.
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
                return;
            }

            // shutdown() unregisters all MBeans from JMX and clears the sources map,
            // preventing unbounded accumulation of MetricCounterLong and MBeanAttributeInfo.
            Method shutdownMethod = metricsSystemImpl.getClass().getDeclaredMethod("shutdown");
            shutdownMethod.setAccessible(true);
            shutdownMethod.invoke(metricsSystemImpl);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Shut down Hadoop metrics2 for ClassLoader: {}", cl);
            }
        } catch (ClassNotFoundException e) {
            // Hadoop metrics2 not present in this ClassLoader, nothing to do
        } catch (Exception e) {
            LOG.warn("Failed to shut down Hadoop metrics2 for ClassLoader: {}", cl, e);
        }
    }
}
