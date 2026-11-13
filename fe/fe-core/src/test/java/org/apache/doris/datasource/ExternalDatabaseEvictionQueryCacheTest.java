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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Pair;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.cache.SqlCacheMetadataVersions;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

class ExternalDatabaseEvictionQueryCacheTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void crossDatabaseCacheBuildSurvivesObjectCapacityPressure(int capacity) {
        Fixture fixture = new Fixture(capacity);
        AtomicInteger builds = new AtomicInteger();
        MTMVCache generated = Mockito.mock(MTMVCache.class);
        MTMV view = new MTMV() {
            @Override
            protected MTMVCache createRewriteCache(ConnectContext connection, boolean needLock, boolean addGuard) {
                // Stop the old unbounded loop deterministically rather than leaking a planner thread.
                Assertions.assertTrue(builds.incrementAndGet() <= 8, "MV cache build did not converge");
                try (StatementContext statement = new StatementContext(connection, null)) {
                    for (String db : ImmutableList.of("one", "two")) {
                        Assertions.assertNotNull(statement.getAndCacheTable(
                                ImmutableList.of("lake", db, "t"), TableFrom.QUERY, Optional.empty()));
                    }
                    fixture.catalog.objects.getMetaObjCache().cleanUp();
                }
                return generated;
            }
        };
        fixture.setView(view);
        try (MockedStatic<Env> ignored = fixture.currentEnv()) {
            SqlCacheMetadataVersions.Snapshot snapshot = fixture.snapshot();
            Assertions.assertSame(generated, view.getOrGenerateCache(fixture.connection));
            Assertions.assertSame(generated, view.getOrGenerateCache(fixture.connection));
            Assertions.assertEquals(1, builds.get());
            Assertions.assertTrue(snapshot.isCurrent(), "object eviction is not a metadata mutation");
            if (capacity == 1) {
                Assertions.assertTrue(fixture.catalog.evictions.get() > 0, "the real DB cache must evict");
                Mockito.verify(fixture.engine, Mockito.atLeastOnce()).invalidateDb(Mockito.any(ExternalDatabase.class));
            } else {
                Assertions.assertEquals(0, fixture.catalog.evictions.get());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RemovalCause.class, names = {"SIZE", "EXPIRED", "COLLECTED"})
    void automaticRemovalRetiresMetadataWithoutInvalidatingQueryDependencies(RemovalCause cause) {
        Fixture fixture = new Fixture(2);
        MTMV view = new MTMV();
        fixture.setView(view);
        MTMVCache cached = Mockito.mock(MTMVCache.class);
        Deencapsulation.setField(view, "cacheWithGuard", cached);
        Deencapsulation.setField(view, "cacheWithoutGuard", cached);
        try (MockedStatic<Env> ignored = fixture.currentEnv()) {
            ExternalDatabase<?> db = fixture.catalog.getDbNullable("one");
            SqlCacheMetadataVersions.Snapshot snapshot = fixture.snapshot();

            fixture.catalog.handleDatabaseMetaCacheRemoval(Optional.of(db), cause);

            Mockito.verify(fixture.engine).invalidateDb(db);
            Assertions.assertTrue(snapshot.isCurrent());
            Assertions.assertSame(cached, Deencapsulation.getField(view, "cacheWithGuard"));
            Assertions.assertSame(cached, Deencapsulation.getField(view, "cacheWithoutGuard"));
        }
    }

    @ParameterizedTest
    @EnumSource(value = RemovalCause.class, names = {"EXPLICIT", "REPLACED"})
    void explicitRemovalStillInvalidatesQueryDependencies(RemovalCause cause) {
        Fixture fixture = new Fixture(2);
        MTMV view = new MTMV();
        fixture.setView(view);
        MTMVCache cached = Mockito.mock(MTMVCache.class);
        Deencapsulation.setField(view, "cacheWithGuard", cached);
        Deencapsulation.setField(view, "cacheWithoutGuard", cached);
        try (MockedStatic<Env> ignored = fixture.currentEnv()) {
            ExternalDatabase<?> db = fixture.catalog.getDbNullable("one");
            SqlCacheMetadataVersions.Snapshot snapshot = fixture.snapshot();

            fixture.catalog.handleDatabaseMetaCacheRemoval(Optional.of(db), cause);

            Mockito.verify(fixture.engine).invalidateDb(db);
            Assertions.assertFalse(snapshot.isCurrent());
            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithGuard"));
            Assertions.assertNull(Deencapsulation.getField(view, "cacheWithoutGuard"));
        }
    }

    private static class Fixture {
        private final Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        private final EvictingCatalog catalog;
        private final NereidsSqlCacheManager sql = new NereidsSqlCacheManager();
        private final MTMVRelationManager relations = new MTMVRelationManager();
        private final InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        private final Database views = Mockito.mock(Database.class);
        private final ExternalMetaCache engine = Mockito.mock(ExternalMetaCache.class);
        private final ConnectContext connection = Mockito.mock(ConnectContext.class);

        Fixture(int capacity) {
            catalog = new EvictingCatalog(capacity);
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.doReturn(catalog).when(catalogs).getCatalog("lake");
            Mockito.doReturn(catalog).when(catalogs).getCatalog(catalog.getId());
            Mockito.doReturn(catalogs).when(env).getCatalogMgr();
            Mockito.doReturn(sql).when(env).getSqlCacheManager();
            Mockito.doReturn(Mockito.mock(NereidsSortedPartitionsCacheManager.class))
                    .when(env).getSortedPartitionsCacheManager();
            MTMVService service = Mockito.mock(MTMVService.class);
            Mockito.when(service.getRelationManager()).thenReturn(relations);
            Mockito.doReturn(service).when(env).getMtmvService();
            Mockito.when(internal.getDbNullable("views")).thenReturn(views);
            Mockito.when(connection.getEnv()).thenReturn(env);
            Mockito.when(connection.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
            Mockito.when(connection.getSessionVariable()).thenReturn(new SessionVariable());
            Mockito.when(engine.engine()).thenReturn("default");
            Mockito.when(engine.isCatalogInitialized(catalog.getId())).thenReturn(true);
            ExternalMetaCacheMgr caches = new ExternalMetaCacheMgr(true);
            caches.replaceEngineCachesForTest(ImmutableList.of(engine));
            Mockito.doReturn(caches).when(env).getExtMetaCacheMgr();
        }

        void setView(MTMV view) {
            Deencapsulation.setField(view, "status", new MTMVStatus());
            Deencapsulation.setField(view, "refreshSnapshot", new MTMVRefreshSnapshot());
            Mockito.when(views.getTableNullable("dependent")).thenReturn(view);
            ImmutableSet<BaseTableInfo> sources = ImmutableSet.of(
                    new BaseTableInfo("lake", "one", "t"), new BaseTableInfo("lake", "two", "t"));
            relations.refreshMTMVCache(new MTMVRelation(sources, sources, sources,
                    ImmutableSet.of(), ImmutableSet.of()), new BaseTableInfo("internal", "views", "dependent"));
        }

        SqlCacheMetadataVersions.Snapshot snapshot() {
            SqlCacheMetadataVersions.Snapshot snapshot = sql.newMetadataSnapshot();
            for (String db : ImmutableList.of("one", "two")) {
                snapshot.addTable(Util.genIdByName("lake", db, "t"), new FullTableName("lake", db, "t"));
            }
            return snapshot;
        }

        MockedStatic<Env> currentEnv() {
            MockedStatic<Env> current = Mockito.mockStatic(Env.class);
            current.when(Env::getCurrentEnv).thenReturn(env);
            current.when(Env::getCurrentInternalCatalog).thenReturn(internal);
            return current;
        }
    }

    private static class EvictingCatalog extends ExternalCatalog {
        private final MetaCache<ExternalDatabase<? extends ExternalTable>> objects;
        private final AtomicInteger evictions = new AtomicInteger();

        EvictingCatalog(int capacity) {
            super(1L, "lake", InitCatalogLog.Type.TEST, "");
            catalogProperty = new CatalogProperty(null, Collections.emptyMap());
            objects = new MetaCache<>("databases", MoreExecutors.newDirectExecutorService(),
                    OptionalLong.empty(), OptionalLong.empty(), capacity,
                    ignored -> ImmutableList.of(Pair.of("one", "one"), Pair.of("two", "two")),
                    db -> Optional.of(new DummyDatabase(this, db)),
                    (key, value, cause) -> {
                        if (cause.wasEvicted()) {
                            evictions.incrementAndGet();
                        }
                        handleDatabaseMetaCacheRemoval(value, cause);
                    });
            metaCache = objects;
            initialized = true;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String db) {
            return ImmutableList.of("t");
        }

        @Override
        public boolean tableExist(SessionContext context, String db, String table) {
            return true;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }
    }

    private static class DummyDatabase extends ExternalDatabase<ExternalTable> {
        private final ExternalTable table;

        DummyDatabase(ExternalCatalog catalog, String name) {
            super(catalog, Util.genIdByName(catalog.getName(), name), name, name, InitDatabaseLog.Type.TEST);
            table = Mockito.mock(ExternalTable.class);
            Mockito.when(table.getName()).thenReturn("t");
            Mockito.when(table.getId()).thenReturn(Util.genIdByName(catalog.getName(), name, "t"));
            Mockito.doReturn(this).when(table).getDatabase();
            Mockito.doReturn(catalog).when(table).getCatalog();
        }

        @Override
        public ExternalTable getTableNullable(String name) {
            return table;
        }

        @Override
        protected ExternalTable buildTableInternal(String remoteName, String localName, long id,
                ExternalCatalog catalog, ExternalDatabase database) {
            return table;
        }
    }
}
