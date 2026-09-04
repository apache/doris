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

package org.apache.doris.datasource;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CatalogPropertyTest {

    @Test
    public void testStorageAdaptersArePublishedAfterInitialization() throws Exception {
        CountDownLatch initializationStarted = new CountDownLatch(1);
        CountDownLatch allowInitialization = new CountDownLatch(1);
        CatalogProperty catalogProperty = new CatalogProperty(
                null, Collections.singletonMap("fs.defaultFS", "hdfs://test-ns"));
        catalogProperty.setPluginDerivedStorageDefaultsSupplier(() -> {
            initializationStarted.countDown();
            awaitInitialization(allowInitialization);
            return Collections.emptyMap();
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Map<StorageTypeId, StorageAdapter>> readerResult = new AtomicReference<>();
        Thread concurrentReader = new Thread(
                () -> readerResult.set(catalogProperty.getStorageAdaptersMap()));
        try {
            Future<Map<StorageTypeId, StorageAdapter>> initializer =
                    executor.submit(catalogProperty::getStorageAdaptersMap);
            Assert.assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));

            concurrentReader.start();
            Assert.assertTrue(waitUntilBlockedOrTerminated(concurrentReader, 5, TimeUnit.SECONDS));
            Assert.assertEquals("The reader must block until initialization publishes the completed map",
                    Thread.State.BLOCKED, concurrentReader.getState());

            allowInitialization.countDown();
            Map<StorageTypeId, StorageAdapter> initialized = initializer.get(5, TimeUnit.SECONDS);
            concurrentReader.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse(concurrentReader.isAlive());
            Assert.assertSame(initialized, readerResult.get());
        } finally {
            allowInitialization.countDown();
            concurrentReader.interrupt();
            executor.shutdownNow();
        }
    }

    @Test
    public void testStorageAdaptersCacheIsImmutable() {
        CatalogProperty catalogProperty = new CatalogProperty(
                null, Collections.singletonMap("fs.defaultFS", "hdfs://test-ns"));
        catalogProperty.setPluginDerivedStorageDefaultsSupplier(Collections::emptyMap);

        Map<StorageTypeId, StorageAdapter> storageAdapters = catalogProperty.getStorageAdaptersMap();
        Assert.assertThrows(UnsupportedOperationException.class, storageAdapters::clear);
    }

    private static boolean waitUntilBlockedOrTerminated(Thread thread, long timeout, TimeUnit timeUnit) {
        long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        return !thread.isAlive() || thread.getState() == Thread.State.BLOCKED;
    }

    private static void awaitInitialization(CountDownLatch allowInitialization) {
        try {
            if (!allowInitialization.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to continue storage adapter initialization");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while initializing storage adapters", e);
        }
    }
}
