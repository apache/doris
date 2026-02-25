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

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies that the Hadoop internal fields and methods used by {@link HadoopMetricsDisabler}
 * exist and are accessible via reflection. If Hadoop upgrades change these internals,
 * these tests will fail early rather than silently breaking the metrics cleanup at runtime.
 */
public class HadoopMetricsDisablerTest {

    @Test
    public void testDefaultMetricsSystemHasInstanceField() throws Exception {
        Object instance = DefaultMetricsSystem.class.getField("INSTANCE").get(null);
        Assertions.assertNotNull(instance, "DefaultMetricsSystem.INSTANCE should exist");
    }

    @Test
    public void testDefaultMetricsSystemHasImplField() throws Exception {
        Object instance = DefaultMetricsSystem.class.getField("INSTANCE").get(null);
        Field implField = instance.getClass().getDeclaredField("impl");
        implField.setAccessible(true);
        Object implRef = implField.get(instance);
        Assertions.assertInstanceOf(AtomicReference.class, implRef,
                "DefaultMetricsSystem.impl should be AtomicReference");
    }

    @Test
    public void testMetricsSystemImplHasSourcesField() throws Exception {
        Object instance = DefaultMetricsSystem.class.getField("INSTANCE").get(null);
        Field implField = instance.getClass().getDeclaredField("impl");
        implField.setAccessible(true);
        AtomicReference<?> implRef = (AtomicReference<?>) implField.get(instance);
        Object metricsSystemImpl = implRef.get();
        Assertions.assertNotNull(metricsSystemImpl, "MetricsSystemImpl should not be null");

        Field sourcesField = metricsSystemImpl.getClass().getDeclaredField("sources");
        sourcesField.setAccessible(true);
        Object sources = sourcesField.get(metricsSystemImpl);
        Assertions.assertInstanceOf(Map.class, sources,
                "MetricsSystemImpl.sources should be a Map");
    }

    @Test
    public void testMetricsSourceAdapterHasCloseMethod() throws Exception {
        // Verify the close() method exists on MetricsSourceAdapter
        Class<?> adapterClass = Class.forName(
                "org.apache.hadoop.metrics2.impl.MetricsSourceAdapter");
        Method closeMethod = adapterClass.getDeclaredMethod("close");
        Assertions.assertNotNull(closeMethod,
                "MetricsSourceAdapter should have a close() method");
    }

    @Test
    public void testDisableWithNullClassLoaderIsNoOp() {
        // Should not throw
        HadoopMetricsDisabler.disable(null);
    }

    @Test
    public void testDisableWithCurrentClassLoader() throws Exception {
        // Run disable on the current ClassLoader — should clean up without error
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        HadoopMetricsDisabler.disable(cl);

        // Verify sources map is empty after disable
        Object instance = DefaultMetricsSystem.class.getField("INSTANCE").get(null);
        Field implField = instance.getClass().getDeclaredField("impl");
        implField.setAccessible(true);
        AtomicReference<?> implRef = (AtomicReference<?>) implField.get(instance);
        Object msi = implRef.get();
        Field sourcesField = msi.getClass().getDeclaredField("sources");
        sourcesField.setAccessible(true);
        Map<?, ?> sources = (Map<?, ?>) sourcesField.get(msi);
        Assertions.assertTrue(sources.isEmpty(),
                "sources map should be empty after disable()");
    }
}
