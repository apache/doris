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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.ExternalRowCountCache.RowCountCacheLoader;
import org.apache.doris.datasource.ExternalRowCountCache.RowCountKey;
import org.apache.doris.statistics.BasicAsyncCacheLoader;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ExternalRowCountCacheTest {
    private ExternalRowCountCache cache;
    private ExecutorService executorService;

    public static class TestLoader extends BasicAsyncCacheLoader<RowCountKey, Optional<Long>> {

        private AtomicLong incr = new AtomicLong(333);

        @Override
        protected Optional<Long> doLoad(RowCountKey rowCountKey) {
            if (rowCountKey.getTableId() == 1) {
                return Optional.of(111L);
            } else if (rowCountKey.getTableId() == 2) {
                return Optional.of(222L);
            } else if (rowCountKey.getTableId() == 3) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("load: " + incr.get());
                return Optional.of(incr.incrementAndGet());
            }
            return Optional.empty();
        }
    }

    @BeforeEach
    public void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        cache = new ExternalRowCountCache(executorService, 2, new TestLoader());
    }

    @Test
    public void test() throws Exception {
        // table 1
        long rowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount);
        Thread.sleep(1000);
        rowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(111, rowCount);

        // table 2
        rowCount = cache.getCachedRowCount(1, 1, 2);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount);
        Thread.sleep(1000);
        rowCount = cache.getCachedRowCount(1, 1, 2);
        Assertions.assertEquals(222, rowCount);

        // table 3
        rowCount = cache.getCachedRowCount(1, 1, 3);
        // first get, it should be 0 because the loader is async
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount);
        // After sleep 2 sec and then get, it should be 1
        Thread.sleep(2000);
        rowCount = cache.getCachedRowCount(1, 1, 3);
        Assertions.assertEquals(334, rowCount);
        // sleep 3 sec to trigger refresh
        Thread.sleep(3000);
        rowCount = cache.getCachedRowCount(1, 1, 3);
        // the refresh will be triggered only when query it, so it should still be 1
        Assertions.assertEquals(334, rowCount);
        // sleep 2 sec to wait for the doLoad
        Thread.sleep(2000);
        rowCount = cache.getCachedRowCount(1, 1, 3);
        // refresh done, value should be 2
        Assertions.assertEquals(335, rowCount);
    }

    @Test
    public void testLoadWithException() throws Exception {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, Integer.MAX_VALUE, "TEST", true);
        AtomicInteger counter = new AtomicInteger(0);

        new MockUp<RowCountCacheLoader>() {
            @Mock
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                counter.incrementAndGet();
                return null;
            }
        };
        ExternalRowCountCache cache = new ExternalRowCountCache(executor, 2, null);
        long cachedRowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cachedRowCount);
        for (int i = 0; i < 60; i++) {
            if (counter.get() == 1) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertEquals(1, counter.get());

        new MockUp<ExternalRowCountCache.RowCountCacheLoader>() {
            @Mock
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                counter.incrementAndGet();
                return Optional.of(100L);
            }
        };
        cache.getCachedRowCount(1, 1, 1);
        for (int i = 0; i < 60; i++) {
            cachedRowCount = cache.getCachedRowCount(1, 1, 1);
            if (cachedRowCount != TableIf.UNKNOWN_ROW_COUNT) {
                Assertions.assertEquals(100, cachedRowCount);
                break;
            }
            Thread.sleep(1000);
        }
        cachedRowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(100, cachedRowCount);
        Assertions.assertEquals(2, counter.get());

        new MockUp<ExternalRowCountCache.RowCountCacheLoader>() {
            @Mock
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                counter.incrementAndGet();
                try {
                    Thread.sleep(1000000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return Optional.of(100L);
            }
        };
        cachedRowCount = cache.getCachedRowCount(2, 2, 2);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cachedRowCount);
        Thread.sleep(1000);
        cachedRowCount = cache.getCachedRowCount(2, 2, 2);
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cachedRowCount);
        for (int i = 0; i < 60; i++) {
            if (counter.get() == 3) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertEquals(3, counter.get());
    }
}
