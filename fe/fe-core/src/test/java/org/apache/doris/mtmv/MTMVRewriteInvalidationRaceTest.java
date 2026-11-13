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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class MTMVRewriteInvalidationRaceTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void tableInvalidationPreservesCanonicalExternalNames(boolean globalLowerCase) {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("lake");
        Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(2);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(catalog).when(database).getCatalog();
        Mockito.when(database.getFullName()).thenReturn("source");
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.when(table.getName()).thenReturn("Orders");
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMV view = new MTMV();
        MTMVCache cached = Mockito.mock(MTMVCache.class);
        Deencapsulation.setField(view, "cacheWithGuard", cached);
        Deencapsulation.setField(view, "cacheWithoutGuard", cached);
        try (MockedStatic<Env> env = localView(view)) {
            env.when(Env::isStoredTableNamesLowerCase).thenReturn(globalLowerCase);
            BaseTableInfo source = new BaseTableInfo(table);
            Assertions.assertEquals("Orders", source.getTableName());
            Assertions.assertEquals(2, catalog.getLowerCaseTableNames());
            manager.refreshMTMVCache(relation(source, false), name("internal", "views", "dependent"));

            manager.invalidateRewriteCaches("lake", "source", "Orders");

            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithGuard"));
            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithoutGuard"));
        }
    }

    @ParameterizedTest
    @CsvSource({"table,false", "database,false", "catalog,false", "table,true", "database,true", "catalog,true"})
    void invalidationCannotMissAMaterializedViewDuringDependencyReplacement(String scope, boolean baseView)
            throws Exception {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        BaseTableInfo viewName = name("internal", "views", "dependent");
        MTMVRelation relation = relation(source, baseView);
        manager.refreshMTMVCache(relation, viewName);
        Map<BaseTableInfo, Set<BaseTableInfo>> index = Deencapsulation.getField(manager,
                baseView ? "viewMTMVs" : "tableMTMVs");
        PausingRemovalSet dependencies = new PausingRemovalSet(index.get(source));
        index.put(source, dependencies);
        MTMV view = new MTMV();
        MTMVCache cached = Mockito.mock(MTMVCache.class);
        AtomicReference<Thread> updaterThread = new AtomicReference<>();
        AtomicReference<Thread> invalidatorThread = new AtomicReference<>();
        CountDownLatch invalidationStarted = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> update = workers.submit(() -> {
                updaterThread.set(Thread.currentThread());
                view.writeMvLock();
                try {
                    // addTaskResult publishes its plan before replacing the dependency index.
                    Deencapsulation.setField(view, "cacheWithGuard", cached);
                    Deencapsulation.setField(view, "cacheWithoutGuard", cached);
                    manager.refreshMTMVCache(relation, viewName);
                } finally {
                    view.writeMvUnlock();
                }
            });
            Assertions.assertTrue(dependencies.removed.await(5, TimeUnit.SECONDS));
            Future<?> invalidate = workers.submit(() -> {
                invalidatorThread.set(Thread.currentThread());
                try (MockedStatic<Env> ignored = localView(view)) {
                    invalidationStarted.countDown();
                    manager.invalidateRewriteCaches("lake", scope.equals("catalog") ? null : "source",
                            scope.equals("table") ? "t" : null);
                }
            });
            Assertions.assertTrue(invalidationStarted.await(5, TimeUnit.SECONDS));
            awaitDiscoveryOrIndexLock(invalidate, invalidatorThread.get(), updaterThread.get());
            dependencies.continueUpdate.countDown();
            update.get(5, TimeUnit.SECONDS);
            invalidate.get(5, TimeUnit.SECONDS);

            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithGuard"));
            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithoutGuard"));
            Assertions.assertEquals(1L, (long) Deencapsulation.getField(view, "rewriteCacheGeneration"));
        } finally {
            dependencies.continueUpdate.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cacheInvalidationDoesNotHoldTheDependencyIndexLock() throws Exception {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        manager.refreshMTMVCache(relation(source, false), name("internal", "views", "dependent"));
        MTMV view = Mockito.mock(MTMV.class);
        CountDownLatch cacheInvalidation = new CountDownLatch(1);
        CountDownLatch finishInvalidation = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            cacheInvalidation.countDown();
            Assertions.assertTrue(finishInvalidation.await(5, TimeUnit.SECONDS));
            return null;
        }).when(view).invalidateRewriteCache();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> invalidate = workers.submit(() -> {
                try (MockedStatic<Env> ignored = localView(view)) {
                    manager.invalidateRewriteCaches("lake", "source", "t");
                }
            });
            Assertions.assertTrue(cacheInvalidation.await(5, TimeUnit.SECONDS));
            // Refresh completion can already own an MV write lock. It must still update the index
            // while another invalidation waits for that MV lock.
            workers.submit(() -> manager.refreshMTMVCache(relation(source, false),
                    name("internal", "views", "another"))).get(5, TimeUnit.SECONDS);
            finishInvalidation.countDown();
            invalidate.get(5, TimeUnit.SECONDS);
        } finally {
            finishInvalidation.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void awaitDiscoveryOrIndexLock(Future<?> discovery, Thread reader, Thread writer)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (discovery.isDone()) {
                // The unfenced implementation finishes the empty lookup before registration resumes.
                return;
            }
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(reader.getId());
            if (info != null && info.getLockOwnerId() == writer.getId()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(1);
        }
        Assertions.fail("invalidation neither completed discovery nor waited for dependency replacement");
    }

    private static class PausingRemovalSet extends AbstractSet<BaseTableInfo> {
        private final Set<BaseTableInfo> entries = ConcurrentHashMap.newKeySet();
        private final CountDownLatch removed = new CountDownLatch(1);
        private final CountDownLatch continueUpdate = new CountDownLatch(1);

        PausingRemovalSet(Set<BaseTableInfo> entries) {
            this.entries.addAll(entries);
        }

        @Override
        public boolean remove(Object entry) {
            boolean changed = entries.remove(entry);
            removed.countDown();
            try {
                Assertions.assertTrue(continueUpdate.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            return changed;
        }

        @Override
        public boolean add(BaseTableInfo entry) {
            return entries.add(entry);
        }

        @Override
        public Iterator<BaseTableInfo> iterator() {
            return entries.iterator();
        }

        @Override
        public int size() {
            return entries.size();
        }
    }

    private static BaseTableInfo name(String catalog, String db, String table) {
        return new BaseTableInfo(new TableNameInfo(catalog, db, table));
    }

    private static MTMVRelation relation(BaseTableInfo source, boolean view) {
        Set<BaseTableInfo> tables = view ? ImmutableSet.of() : ImmutableSet.of(source);
        Set<BaseTableInfo> views = view ? ImmutableSet.of(source) : ImmutableSet.of();
        return new MTMVRelation(tables, tables, tables, views, views);
    }

    private static MockedStatic<Env> localView(MTMV view) {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("views")).thenReturn(database);
        Mockito.when(database.getTableNullable("dependent")).thenReturn(view);
        MockedStatic<Env> env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return env;
    }
}
