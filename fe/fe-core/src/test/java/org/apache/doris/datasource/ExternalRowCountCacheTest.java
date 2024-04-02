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

import org.apache.doris.datasource.ExternalRowCountCache.RowCountKey;
import org.apache.doris.statistics.BasicAsyncCacheLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        Assertions.assertEquals(0, rowCount);
        Thread.sleep(1000);
        rowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(111, rowCount);

        // table 2
        rowCount = cache.getCachedRowCount(1, 1, 2);
        Assertions.assertEquals(0, rowCount);
        Thread.sleep(1000);
        rowCount = cache.getCachedRowCount(1, 1, 2);
        Assertions.assertEquals(222, rowCount);

        // table 3
        rowCount = cache.getCachedRowCount(1, 1, 3);
        // first get, it should be 0 because the loader is async
        Assertions.assertEquals(0, rowCount);
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
}
