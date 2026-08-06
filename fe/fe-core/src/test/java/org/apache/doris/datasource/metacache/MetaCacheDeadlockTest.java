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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.ThreadPoolManager;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Test to verify sync removal listeners do not deadlock when nested invalidation happens.
 */
public class MetaCacheDeadlockTest {

    @Test
    public void testNestedInvalidateAllWithBoundedExecutor() throws InterruptedException {
        ExecutorService executor = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1),
                new ThreadPoolManager.BlockedPolicy("TestExecutor", 200)
        );

        CacheSpec cacheSpec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100);
        MetaCacheEntry<String, String> tableCache = MetaCacheEntry.withSyncRemovalListener(
                "tableCache",
                key -> "table-" + key,
                cacheSpec,
                executor,
                (key, value, cause) -> {
                });
        for (int i = 0; i < 100; i++) {
            tableCache.put("table" + i, "table-" + i);
        }

        MetaCacheEntry<String, String> dbCache = MetaCacheEntry.withSyncRemovalListener(
                "databaseCache",
                key -> "db-" + key,
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1),
                executor,
                (key, value, cause) -> tableCache.invalidateAll());
        dbCache.put("db0", "db-0");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(5);
        for (int i = 1; i <= 5; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    dbCache.put("db" + idx, "db-" + idx);
                } catch (Exception e) {
                    // Ignore
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(2, TimeUnit.SECONDS);

        executor.shutdown();
        boolean terminated = executor.awaitTermination(1, TimeUnit.SECONDS);

        Assert.assertTrue("MetaCacheEntry deadlock detected. Ensure sync removal listeners use direct execution.",
                completed && terminated);
    }
}
