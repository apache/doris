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

import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Test to verify MetaCache does not deadlock when removal listener calls invalidateAll().
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

        CacheLoader<String, List<Pair<String, String>>> namesCacheLoader = key -> Lists.newArrayList();
        CacheLoader<String, Optional<String>> tableLoader = key -> Optional.of("table-" + key);

        MetaCache<String> tableCache = new MetaCache<>(
                "tableCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                100,
                namesCacheLoader,
                tableLoader,
                (key, value, cause) -> { });

        for (int i = 0; i < 100; i++) {
            tableCache.updateCache("table" + i, "table" + i, "table-" + i, i);
        }

        CacheLoader<String, Optional<String>> dbLoader = key -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.of("db-" + key);
        };

        MetaCache<String> dbCache = new MetaCache<>(
                "databaseCache",
                executor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                namesCacheLoader,
                dbLoader,
                (key, value, cause) -> tableCache.invalidateAll());

        dbCache.updateCache("db0", "db0", "db-0", 0);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(5);
        for (int i = 1; i <= 5; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    dbCache.updateCache("db" + idx, "db" + idx, "db-" + idx, idx);
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

        Assert.assertTrue("MetaCache deadlock detected. Ensure metaObjCache uses buildCacheWithSyncRemovalListener.",
                completed && terminated);
    }
}
