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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Isolated SDK statics make startup-order tests independent of other tests' Iceberg use. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class IcebergWorkerPoolTest {
    private static final String SDK_SIZE_PROPERTY = "iceberg.worker.num-threads";
    private String previousSize;

    @BeforeEach
    public void saveProperty() {
        previousSize = System.getProperty(SDK_SIZE_PROPERTY);
        // The scope lifecycle assertion uses parent-loaded classes. Keep their SDK statics on the
        // original startup value before changing the property for the isolated SDK copies below.
        ThreadPools.getWorkerPool();
    }

    @AfterEach
    public void restoreProperty() {
        if (previousSize == null) {
            System.clearProperty(SDK_SIZE_PROPERTY);
        } else {
            System.setProperty(SDK_SIZE_PROPERTY, previousSize);
        }
    }

    @Test
    public void racingCatalogsShareTheConfiguredSdkPool() throws Exception {
        System.setProperty(SDK_SIZE_PROPERTY, "8");
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try (URLClassLoader loader = isolatedLoader()) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<ExecutorService>> pools = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                final int catalogId = i;
                pools.add(callers.submit(() -> {
                    start.await();
                    initialize(loader, context(catalogId, Map.of(IcebergConf.CONF_WORKER_THREAD_POOL_SIZE, "2")));
                    return workerPool(loader);
                }));
            }
            start.countDown();
            ExecutorService shared = pools.get(0).get(10, TimeUnit.SECONDS);
            try {
                for (Future<ExecutorService> pool : pools) {
                    Assertions.assertSame(shared, pool.get(10, TimeUnit.SECONDS));
                }
                Assertions.assertEquals(2, workerPoolSize(loader));
                Assertions.assertEquals("2", System.getProperty(SDK_SIZE_PROPERTY));
                // Pinning must still reach every worker at the configured size, including a size below CPUs.
                Assertions.assertTrue(IcebergConnector.pinPoolThreadsToClassLoader(shared, 2, loader, 5));
                Assertions.assertSame(loader,
                        shared.submit(() -> Thread.currentThread().getContextClassLoader()).get(5, TimeUnit.SECONDS));
                ConnectorScanContext scanContext = new ConnectorScanContext();
                try (IcebergScanPlanningScope scope = new IcebergScanPlanningScope(scanContext, shared)) {
                    Assertions.assertEquals(42, scope.submit(() -> 42).get(5, TimeUnit.SECONDS));
                    scanContext.cancel();
                }
                Assertions.assertFalse(shared.isShutdown(), "a scan must not own the FE-wide pool");
                Assertions.assertEquals(42, shared.submit(() -> 42).get(5, TimeUnit.SECONDS));
            } finally {
                shared.shutdownNow();
            }
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    public void unsetPluginConfigurationPreservesSdkStartupSetting() throws Exception {
        System.setProperty(SDK_SIZE_PROPERTY, "3");
        try (URLClassLoader loader = isolatedLoader()) {
            initialize(loader, context(1, Map.of()));
            Assertions.assertEquals(3, workerPoolSize(loader));
            Assertions.assertEquals("3", System.getProperty(SDK_SIZE_PROPERTY));
        }
    }

    @Test
    public void alreadyInitializedSdkCannotSilentlyIgnoreTheLimit() throws Exception {
        System.setProperty(SDK_SIZE_PROPERTY, "3");
        try (URLClassLoader loader = isolatedLoader()) {
            Assertions.assertEquals(3, workerPoolSize(loader));
            InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                    () -> initialize(loader, context(1, Map.of(IcebergConf.CONF_WORKER_THREAD_POOL_SIZE, "2"))));
            Assertions.assertInstanceOf(IllegalStateException.class, error.getCause());
            Assertions.assertTrue(error.getCause().getMessage().contains("before iceberg.conf"));
            Assertions.assertEquals(3, workerPoolSize(loader));
        }
    }

    private static void initialize(ClassLoader loader, ConnectorContext context) throws Exception {
        Class<?> owner = loader.loadClass(IcebergWorkerPool.class.getName());
        Method initialize = owner.getDeclaredMethod("initialize", ConnectorContext.class);
        initialize.setAccessible(true);
        initialize.invoke(null, context);
    }

    private static ExecutorService workerPool(ClassLoader loader) throws Exception {
        return (ExecutorService) loader.loadClass(ThreadPools.class.getName()).getMethod("getWorkerPool").invoke(null);
    }

    private static int workerPoolSize(ClassLoader loader) throws Exception {
        return loader.loadClass(ThreadPools.class.getName()).getField("WORKER_THREAD_POOL_SIZE").getInt(null);
    }

    private static URLClassLoader isolatedLoader() {
        URL[] urls = {IcebergWorkerPool.class.getProtectionDomain().getCodeSource().getLocation(),
                ThreadPools.class.getProtectionDomain().getCodeSource().getLocation()};
        return new URLClassLoader(urls, IcebergWorkerPoolTest.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                synchronized (getClassLoadingLock(name)) {
                    if (name.equals(IcebergWorkerPool.class.getName())
                            || name.startsWith("org.apache.iceberg.SystemConfigs")
                            || name.startsWith(ThreadPools.class.getName())) {
                        Class<?> loaded = findLoadedClass(name);
                        Class<?> type = loaded == null ? findClass(name) : loaded;
                        if (resolve) {
                            resolveClass(type);
                        }
                        return type;
                    }
                    return super.loadClass(name, resolve);
                }
            }
        };
    }

    private static ConnectorContext context(long catalogId, Map<String, String> config) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "catalog_" + catalogId;
            }

            @Override
            public long getCatalogId() {
                return catalogId;
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                return config;
            }
        };
    }
}
