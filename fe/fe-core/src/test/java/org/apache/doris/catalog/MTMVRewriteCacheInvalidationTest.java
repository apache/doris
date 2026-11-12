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

package org.apache.doris.catalog;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MTMVRewriteCacheInvalidationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedMetadataChangesBoundCacheConstruction(boolean sameSessionVariables) {
        AtomicInteger builds = new AtomicInteger();
        MTMVCache stale = Mockito.mock(MTMVCache.class);
        MTMV view = new MTMV() {
            @Override
            protected MTMVCache createRewriteCache(ConnectContext context, boolean needLock, boolean addGuard) {
                Assertions.assertTrue(builds.incrementAndGet() <= 8, "cache construction retried indefinitely");
                invalidateRewriteCache();
                return stale;
            }
        };
        if (!sameSessionVariables) {
            Deencapsulation.setField(view, "sessionVariables", ImmutableMap.of("test", "different"));
        }
        ConnectContext connection = Mockito.mock(ConnectContext.class);
        Mockito.when(connection.getSessionVariable()).thenReturn(new SessionVariable());
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> view.getOrGenerateCache(connection));
        Assertions.assertTrue(builds.get() <= 4);
        Assertions.assertNull(Deencapsulation.getField(view, "cacheWithGuard"));
        Assertions.assertNull(Deencapsulation.getField(view, "cacheWithoutGuard"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidationRejectsAnOlderBuildForBothCacheVariants(boolean sameSessionVariables) throws Exception {
        CountDownLatch firstBuild = new CountDownLatch(1);
        CountDownLatch finishBuild = new CountDownLatch(1);
        AtomicInteger builds = new AtomicInteger();
        MTMVCache old = Mockito.mock(MTMVCache.class);
        MTMVCache fresh = Mockito.mock(MTMVCache.class);
        MTMV mv = new MTMV() {
            @Override
            protected MTMVCache createRewriteCache(ConnectContext context, boolean needLock, boolean addGuard) {
                Assertions.assertEquals(!sameSessionVariables, addGuard);
                if (builds.incrementAndGet() == 1) {
                    firstBuild.countDown();
                    try {
                        Assertions.assertTrue(finishBuild.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    return old;
                }
                return fresh;
            }
        };
        Deencapsulation.setField(mv, "status", new MTMVStatus());
        Deencapsulation.setField(mv, "refreshSnapshot", new MTMVRefreshSnapshot());
        if (!sameSessionVariables) {
            Deencapsulation.setField(mv, "sessionVariables", ImmutableMap.of("test", "different"));
        }
        ConnectContext connection = Mockito.mock(ConnectContext.class);
        Mockito.when(connection.getSessionVariable()).thenReturn(new SessionVariable());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<MTMVCache> result = worker.submit(() -> mv.getOrGenerateCache(connection));
            Assertions.assertTrue(firstBuild.await(5, TimeUnit.SECONDS));
            mv.invalidateRewriteCache();
            finishBuild.countDown();
            Assertions.assertSame(fresh, result.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(fresh, mv.getOrGenerateCache(connection));
            Assertions.assertEquals(2, builds.get());
        } finally {
            finishBuild.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
