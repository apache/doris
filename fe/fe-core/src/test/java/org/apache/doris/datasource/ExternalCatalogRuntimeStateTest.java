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

import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergCatalogResourceTracker;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergRestExternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExternalCatalogRuntimeStateTest {

    @Test
    public void testIcebergRuntimeStateRestoredAfterGsonReplay() throws Exception {
        IcebergExternalCatalog restored = roundTrip(
                new IcebergRestExternalCatalog(1L, "iceberg", null, Collections.emptyMap(), ""),
                IcebergExternalCatalog.class);

        assertTrackerCanRetainAndRelease(restored, IcebergExternalCatalog.class, "resourceTracker");
    }

    @Test
    public void testHmsRuntimeStateRestoredAfterGsonReplay() throws Exception {
        HMSExternalCatalog restored = roundTrip(
                new HMSExternalCatalog(2L, "hms", null, Collections.emptyMap(), ""),
                HMSExternalCatalog.class);

        Assertions.assertEquals(0L, restored.getRuntimeGeneration());
        assertTrackerCanRetainAndRelease(restored, HMSExternalCatalog.class, "icebergResourceTracker");
    }

    @Test
    public void testHmsRuntimeGenerationCannotObservePropertyCommitMidReset() throws Exception {
        CountDownLatch resetEntered = new CountDownLatch(1);
        CountDownLatch allowResetToFinish = new CountDownLatch(1);
        HMSExternalCatalog catalog = new HMSExternalCatalog(
                3L, "hms", null,
                new HashMap<>(Collections.singletonMap("s3.access_key", "old")), "") {
            @Override
            public synchronized void notifyPropertiesUpdated(java.util.Map<String, String> updatedProps) {
                resetEntered.countDown();
                try {
                    Assertions.assertTrue(allowResetToFinish.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> modifier = executor.submit(() ->
                    catalog.modifyCatalogProps(Collections.singletonMap("s3.access_key", "new")));
            Assertions.assertTrue(resetEntered.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals("new", catalog.getProperties().get("s3.access_key"));

            CountDownLatch readerStarted = new CountDownLatch(1);
            Future<Long> reader = executor.submit(() -> {
                readerStarted.countDown();
                return catalog.getRuntimeGeneration();
            });
            Assertions.assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> reader.get(200, TimeUnit.MILLISECONDS));

            allowResetToFinish.countDown();
            modifier.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1L, reader.get(5, TimeUnit.SECONDS));
        } finally {
            allowResetToFinish.countDown();
            executor.shutdownNow();
        }
    }

    private static <T extends CatalogIf<?>> T roundTrip(CatalogIf<?> catalog, Class<T> expectedType) {
        String json = GsonUtils.GSON.toJson(catalog, CatalogIf.class);
        CatalogIf<?> restored = GsonUtils.GSON.fromJson(json, CatalogIf.class);
        return expectedType.cast(restored);
    }

    private static void assertTrackerCanRetainAndRelease(Object catalog, Class<?> owner, String fieldName)
            throws Exception {
        Field trackerField = owner.getDeclaredField(fieldName);
        trackerField.setAccessible(true);
        IcebergCatalogResourceTracker tracker = (IcebergCatalogResourceTracker) trackerField.get(catalog);
        Assertions.assertNotNull(tracker);
        Assertions.assertDoesNotThrow(() -> {
            try (IcebergCatalogResourceTracker.LoadGuard ignored = tracker.beginLoad()) {
                // Closing the guard exercises the same retain/release pair used by table loading.
            }
        });
    }
}
