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

import org.apache.doris.common.ThreadPoolManager;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalRowCountCacheTest {
    @Test
    public void testLoadWithException() throws Exception {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, Integer.MAX_VALUE, "TEST", true);
        AtomicInteger counter = new AtomicInteger(0);

        new MockUp<ExternalRowCountCache.RowCountCacheLoader>() {
            @Mock
            protected Optional<Long> doLoad(ExternalRowCountCache.RowCountKey rowCountKey) {
                counter.incrementAndGet();
                return null;
            }
        };
        ExternalRowCountCache cache = new ExternalRowCountCache(executor);
        long cachedRowCount = cache.getCachedRowCount(1, 1, 1);
        Assertions.assertEquals(-1, cachedRowCount);
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
            if (cachedRowCount != -1) {
                Assertions.assertEquals(100, cachedRowCount);
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertEquals(2, counter.get());
    }
}
