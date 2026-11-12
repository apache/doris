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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.cache.SqlCacheMetadataVersions;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class CollectRelationMTMVLockTest {
    @Test
    void catalogRetirementCompletesDuringCandidateBaseLookup() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        LookupCatalog catalog = new LookupCatalog();
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        ExternalTable base = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(database).when(base).getDatabase();
        Mockito.when(base.getName()).thenReturn("base");
        Mockito.when(base.getId()).thenReturn(3L);
        Mockito.doReturn(catalog).when(database).getCatalog();
        Mockito.when(database.getFullName()).thenReturn("source");
        Mockito.doReturn(base).when(database).getTableNullable("base");
        catalog.database = database;
        Mockito.doReturn(catalog).when(catalogs).getCatalog("lake");
        Mockito.doReturn(catalog).when(catalogs).getCatalog(1L);
        Mockito.doReturn(catalogs).when(env).getCatalogMgr();
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(access.detachAccessController("lake", 1L)).thenReturn(() -> { });
        Mockito.doReturn(access).when(env).getAccessManager();
        Mockito.doReturn(new ExternalMetaCacheMgr(true)).when(env).getExtMetaCacheMgr();
        NereidsSqlCacheManager sqlCaches = new NereidsSqlCacheManager();
        Mockito.doReturn(sqlCaches).when(env).getSqlCacheManager();
        Mockito.doReturn(Mockito.mock(NereidsSortedPartitionsCacheManager.class))
                .when(env).getSortedPartitionsCacheManager();
        MTMVCacheManager mvCaches = new MTMVCacheManager();
        Mockito.doReturn(mvCaches).when(env).getMtmvCacheManager();
        MTMV mv = view(ImmutableSet.of(name("base")));
        MTMVRelationManager relations = Mockito.spy(new MTMVRelationManager());
        relations.refreshMTMVCache(mv.getRelation(),
                new BaseTableInfo(new TableNameInfo("internal", "views", "mv")));
        Mockito.doReturn(ImmutableSet.of(mv)).when(relations).getCandidateMTMVs(Mockito.anyList());
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(Thread.holdsLock(catalog), "retirement must keep its catalog generation barrier");
            return invocation.callRealMethod();
        }).when(relations).invalidateRewriteCaches("lake", null, null);
        MTMVService service = Mockito.mock(MTMVService.class);
        Mockito.when(service.getRelationManager()).thenReturn(relations);
        Mockito.doReturn(service).when(env).getMtmvService();
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Database views = Mockito.mock(Database.class);
        Mockito.when(internal.getDbNullable("views")).thenReturn(views);
        Mockito.when(views.getTableNullable("mv")).thenReturn(mv);
        ConnectContext connection = Mockito.mock(ConnectContext.class);
        Mockito.when(connection.getEnv()).thenReturn(env);
        Mockito.when(connection.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connection.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        AtomicReference<Throwable> resetFailure = new AtomicReference<>();
        Thread reset = new Thread(() -> {
            try (MockedStatic<Env> ignored = localEnv(env, internal)) {
                Assertions.assertTrue(catalog.lookupStarted.await(5, TimeUnit.SECONDS));
                catalog.resetToUninitialized(false);
            } catch (Throwable failure) {
                resetFailure.set(failure);
            } finally {
                catalog.retirementFinished.countDown();
            }
        }, "catalog-retirement-during-mv-collection");
        reset.setDaemon(true);
        try (MockedStatic<Env> ignored = localEnv(env, internal)) {
            StatementContext statement = new StatementContext(connection, null);
            List<String> qualifier = name("base").toList();
            // QUERY and MTMV have separate lookup maps, even for the same single base table.
            Assertions.assertSame(base, statement.getAndCacheTable(qualifier, TableFrom.QUERY, Optional.empty()));
            Assertions.assertTrue(statement.getMtmvRelatedTables().isEmpty());
            SqlCacheMetadataVersions.Snapshot snapshot = sqlCaches.newMetadataSnapshot();
            snapshot.addTable(3L, new FullTableName("lake", "source", "base"));
            MTMVCache cached = Mockito.mock(MTMVCache.class);
            mvCaches.put(mv.getId(), true, cached);
            mvCaches.put(mv.getId(), false, cached);
            Assertions.assertSame(cached, mvCaches.getIfPresent(mv.getId(), true));
            Assertions.assertSame(cached, mvCaches.getIfPresent(mv.getId(), false));
            Assertions.assertTrue(snapshot.isCurrent());
            long generation = Deencapsulation.getField(mv, "rewriteCacheGeneration");
            catalog.pauseLookup = true;
            reset.start();
            try {
                collect(base, statement);
            } finally {
                reset.join(TimeUnit.SECONDS.toMillis(10));
            }
            Assertions.assertFalse(reset.isAlive(), "catalog retirement did not finish");
            Assertions.assertNull(resetFailure.get());
            Assertions.assertFalse(catalog.retirementBlocked.get(), "MV read lock blocked catalog retirement");
            Assertions.assertSame(base, statement.getMtmvRelatedTables().get(qualifier));
            Assertions.assertNull(mvCaches.getIfPresent(mv.getId(), true));
            Assertions.assertNull(mvCaches.getIfPresent(mv.getId(), false));
            Assertions.assertEquals(generation + 1, (long) Deencapsulation.getField(mv, "rewriteCacheGeneration"));
            Assertions.assertFalse(snapshot.isCurrent(), "retirement must still fence old SQL dependencies");
        }
    }

    @Test
    void dependencyNamesAreCopiedBeforeResolvingTables() throws Exception {
        BaseTableInfo first = name("first");
        BaseTableInfo second = name("second");
        Set<BaseTableInfo> dependencies = new LinkedHashSet<>(Arrays.asList(first, second));
        MTMV mv = view(dependencies);
        StatementContext statement = Mockito.spy(new StatementContext(0));
        AtomicReference<Throwable> mutationFailure = new AtomicReference<>();
        Thread mutate = new Thread(() -> {
            mv.writeMvLock();
            try {
                second.setTableName("changed");
                dependencies.clear();
            } catch (Throwable failure) {
                mutationFailure.set(failure);
            } finally {
                mv.writeMvUnlock();
            }
        }, "mv-dependency-update-during-collection");
        mutate.setDaemon(true);
        TableIf collected = Mockito.mock(TableIf.class);
        Mockito.doAnswer(invocation -> {
            mutate.start();
            mutate.join(TimeUnit.SECONDS.toMillis(5));
            Assertions.assertFalse(mutate.isAlive(), "table resolution must not hold the MV lock");
            return collected;
        }).when(statement).getAndCacheTable(first.toList(), TableFrom.MTMV, Optional.empty());
        Mockito.doReturn(collected).when(statement)
                .getAndCacheTable(second.toList(), TableFrom.MTMV, Optional.empty());
        try {
            collectWithCandidate(mv, statement);
        } finally {
            mutate.join(TimeUnit.SECONDS.toMillis(10));
        }
        Assertions.assertNull(mutationFailure.get());
        Mockito.verify(statement).getAndCacheTable(ImmutableList.of("lake", "source", "second"),
                TableFrom.MTMV, Optional.empty());
    }

    @Test
    void invalidAndMissingDependenciesDoNotPreventCollectingOtherTables() {
        BaseTableInfo invalid = name("invalid");
        invalid.setTableName(null);
        MTMV mv = view(new LinkedHashSet<>(Arrays.asList(invalid, name("missing"), name("valid"))));
        StatementContext statement = Mockito.spy(new StatementContext(0));
        Mockito.doThrow(new AnalysisException("missing table")).when(statement)
                .getAndCacheTable(name("missing").toList(), TableFrom.MTMV, Optional.empty());
        Mockito.doReturn(Mockito.mock(TableIf.class)).when(statement)
                .getAndCacheTable(name("valid").toList(), TableFrom.MTMV, Optional.empty());
        collectWithCandidate(mv, statement);
        Mockito.verify(statement).getAndCacheTable(name("valid").toList(), TableFrom.MTMV, Optional.empty());
        Mockito.verify(statement, Mockito.never())
                .getAndCacheTable(invalid.toList(), TableFrom.MTMV, Optional.empty());
    }

    private void collectWithCandidate(MTMV mv, StatementContext statement) {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getMtmvService().getRelationManager().getCandidateMTMVs(Mockito.anyList()))
                .thenReturn(ImmutableSet.of(mv));
        ExternalTable base = Mockito.mock(ExternalTable.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(base).getDatabase();
        Mockito.doReturn(Mockito.mock(ExternalCatalog.class)).when(database).getCatalog();
        try (MockedStatic<Env> ignored = localEnv(env, Mockito.mock(InternalCatalog.class))) {
            collect(base, statement);
        }
    }

    private void collect(TableIf base, StatementContext statement) {
        statement.addPlannerHook(InitMaterializationContextHook.INSTANCE);
        CascadesContext cascades = Mockito.mock(CascadesContext.class);
        Mockito.when(cascades.getStatementContext()).thenReturn(statement);
        Deencapsulation.invoke(new CollectRelation(true), "collectMTMVCandidates", base, cascades);
    }

    private static MockedStatic<Env> localEnv(Env env, InternalCatalog internal) {
        MockedStatic<Env> scope = Mockito.mockStatic(Env.class);
        scope.when(Env::getCurrentEnv).thenReturn(env);
        scope.when(Env::getCurrentInternalCatalog).thenReturn(internal);
        return scope;
    }

    private static BaseTableInfo name(String table) {
        return new BaseTableInfo(new TableNameInfo("lake", "source", table));
    }

    private static MTMV view(Set<BaseTableInfo> tables) {
        MTMV mv = Mockito.spy(new MTMV());
        Deencapsulation.setField(mv, "name", "mv");
        mv.setRelation(relation(tables));
        Database database = Mockito.mock(Database.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("views");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.doReturn(database).when(mv).getDatabase();
        return mv;
    }

    private static MTMVRelation relation(Set<BaseTableInfo> tables) {
        return new MTMVRelation(tables, tables, tables, ImmutableSet.of(), ImmutableSet.of());
    }

    private static class LookupCatalog extends ExternalCatalog {
        private ExternalDatabase<?> database;
        private volatile boolean pauseLookup;
        private final CountDownLatch lookupStarted = new CountDownLatch(1);
        private final CountDownLatch retirementFinished = new CountDownLatch(1);
        private final AtomicBoolean retirementBlocked = new AtomicBoolean();

        LookupCatalog() {
            super(1L, "lake", InitCatalogLog.Type.TEST, "");
            initialized = true;
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String name) {
            if (pauseLookup) {
                lookupStarted.countDown();
                try {
                    // Bound the old lock cycle so a failing test releases its MV read lock.
                    if (!retirementFinished.await(5, TimeUnit.SECONDS)) {
                        retirementBlocked.set(true);
                        throw new IllegalStateException("MV collection blocked catalog retirement");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            makeSureInitialized();
            return database;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return ImmutableList.of();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tableName) {
            return false;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected Runnable detachResources() {
            return () -> { };
        }

        @Override
        public void onRefreshCache(boolean invalidCache) {
            initialized = true;
        }
    }
}
