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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MTMVRelationCacheInvalidationTest {
    @Test
    void tableInvalidationUsesTheExistingGenerationBarrier() {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        addDependency(manager, source, "dependent");
        addDependency(manager, name("lake", "source", "other"), "unrelated");
        MTMV dependent = Mockito.mock(MTMV.class);
        MTMV unrelated = Mockito.mock(MTMV.class);
        try (MockedStatic<Env> env = localViews(dependent, unrelated)) {
            manager.invalidateRewriteCaches("lake", "source", "t");
            Mockito.verify(dependent).invalidateRewriteCache();
            Mockito.verify(unrelated, Mockito.never()).invalidateRewriteCache();
        }
    }

    @Test
    void namespaceInvalidationDoesNotRequireWarmBaseObjects() {
        MTMVRelationManager manager = new MTMVRelationManager();
        addDependency(manager, name("lake", "first", "t"), "dependent");
        addDependency(manager, name("lake", "second", "t"), "unrelated");
        MTMV first = Mockito.mock(MTMV.class);
        MTMV second = Mockito.mock(MTMV.class);
        try (MockedStatic<Env> env = localViews(first, second)) {
            manager.invalidateRewriteCaches("lake", "first", null);
            Mockito.verify(first).invalidateRewriteCache();
            Mockito.verify(second, Mockito.never()).invalidateRewriteCache();
            manager.invalidateRewriteCaches("lake", null, null);
            Mockito.verify(first, Mockito.times(2)).invalidateRewriteCache();
            Mockito.verify(second).invalidateRewriteCache();
        }
    }

    @Test
    void oneFailingViewDoesNotBlockTheOtherViews() {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        addDependency(manager, source, "dependent");
        addDependency(manager, source, "unrelated");
        MTMV failing = Mockito.mock(MTMV.class);
        MTMV healthy = Mockito.mock(MTMV.class);
        Mockito.doThrow(new IllegalStateException("cache failure")).when(failing).invalidateRewriteCache();
        try (MockedStatic<Env> env = localViews(failing, healthy)) {
            manager.invalidateRewriteCaches("lake", "source", "t");
            Mockito.verify(failing).invalidateRewriteCache();
            Mockito.verify(healthy).invalidateRewriteCache();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"tableMTMVs", "viewMTMVs", "tableMTMVsOneLevelAndFromView"})
    void concurrentRegistrationKeepsBothDependentViews(String indexField) throws Exception {
        MTMVRelationManager manager = new MTMVRelationManager();
        FirstRegistrationMap index = new FirstRegistrationMap();
        Deencapsulation.setField(manager, indexField, index);
        BaseTableInfo source = name("lake", "source", "t");
        Set<BaseTableInfo> tables = indexField.equals("viewMTMVs") ? ImmutableSet.of() : ImmutableSet.of(source);
        Set<BaseTableInfo> views = indexField.equals("viewMTMVs") ? ImmutableSet.of(source) : ImmutableSet.of();
        MTMVRelation relation = new MTMVRelation(tables, tables, tables, views, views);
        BaseTableInfo first = name("internal", "views", "dependent");
        BaseTableInfo second = name("internal", "views", "unrelated");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstRegistration = workers.submit(() -> register(manager, relation, first, index));
            Future<?> secondRegistration = workers.submit(() -> register(manager, relation, second, index));
            firstRegistration.get(10, TimeUnit.SECONDS);
            secondRegistration.get(10, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
        Assertions.assertEquals(ImmutableSet.of(first, second), index.get(source),
                "concurrent first registrations must not replace another MV's dependency set");
        if (!indexField.equals("tableMTMVsOneLevelAndFromView")) {
            MTMV firstView = Mockito.mock(MTMV.class);
            MTMV secondView = Mockito.mock(MTMV.class);
            try (MockedStatic<Env> ignored = localViews(firstView, secondView)) {
                manager.invalidateRewriteCaches("lake", "source", "t");
                Mockito.verify(firstView).invalidateRewriteCache();
                Mockito.verify(secondView).invalidateRewriteCache();
            }
        }
    }

    private void register(MTMVRelationManager manager, MTMVRelation relation,
            BaseTableInfo view, FirstRegistrationMap index) {
        try {
            manager.refreshMTMVCache(relation, view);
        } finally {
            index.firstRegistered.countDown();
        }
    }

    private static class FirstRegistrationMap extends ConcurrentHashMap<BaseTableInfo, Set<BaseTableInfo>> {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger misses = new AtomicInteger();
        private final CountDownLatch secondMiss = new CountDownLatch(1);
        private final CountDownLatch firstRegistered = new CountDownLatch(1);

        @Override
        public boolean containsKey(Object key) {
            boolean present = super.containsKey(key);
            if (!present) {
                // Both old check-then-put callers observe absence. Let one finish registering before
                // the other resumes its stale miss. Atomic computeIfAbsent does not take this path.
                if (misses.incrementAndGet() == 1) {
                    await(secondMiss);
                } else {
                    secondMiss.countDown();
                    await(firstRegistered);
                }
            }
            return present;
        }

        private static void await(CountDownLatch latch) {
            try {
                Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "registration interleaving timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private BaseTableInfo name(String catalog, String db, String table) {
        return new BaseTableInfo(new TableNameInfo(catalog, db, table));
    }

    private void addDependency(MTMVRelationManager manager, BaseTableInfo source, String view) {
        manager.refreshMTMVCache(new MTMVRelation(ImmutableSet.of(source), ImmutableSet.of(source),
                        ImmutableSet.of(source), ImmutableSet.of(), ImmutableSet.of()),
                name("internal", "views", view));
    }

    private MockedStatic<Env> localViews(MTMV dependent, MTMV unrelated) {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("views")).thenReturn(database);
        Mockito.when(database.getTableNullable("dependent")).thenReturn(dependent);
        Mockito.when(database.getTableNullable("unrelated")).thenReturn(unrelated);
        MockedStatic<Env> env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return env;
    }
}
