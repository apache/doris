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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.cache.SqlCacheMetadataVersions;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.ExternalMetaCacheRegistry;
import org.apache.doris.datasource.metacache.ExternalMetaCacheRouteResolver;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;

class ExternalQueryCacheInvalidationTest {
    @ParameterizedTest
    @CsvSource({"catalog,false", "catalog,true", "remove,false", "remove,true",
            "context,false", "context,true", "database,false", "database,true",
            "table,false", "table,true", "typed,false", "typed,true",
            "engine,false", "engine,true", "partitions,false", "partitions,true"})
    void metadataInvalidationFencesQueryDependenciesAndKeepsTheFinalRowCountFence(
            String operation, boolean failure) {
        Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("lake");
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.doReturn(catalog).when(catalogs).getCatalog(1L);
        Mockito.doReturn(catalogs).when(env).getCatalogMgr();
        NereidsSqlCacheManager sql = new NereidsSqlCacheManager();
        Mockito.doReturn(sql).when(env).getSqlCacheManager();
        Mockito.doReturn(Mockito.mock(NereidsSortedPartitionsCacheManager.class))
                .when(env).getSortedPartitionsCacheManager();
        MTMVRelationManager relations = Mockito.mock(MTMVRelationManager.class);
        MTMVService service = Mockito.mock(MTMVService.class);
        Mockito.when(service.getRelationManager()).thenReturn(relations);
        Mockito.doReturn(service).when(env).getMtmvService();
        SqlCacheMetadataVersions.Snapshot affected = sql.newMetadataSnapshot();
        affected.addTable(3L, new FullTableName("lake", "db", "t"));
        SqlCacheMetadataVersions.Snapshot unrelated = sql.newMetadataSnapshot();
        unrelated.addTable(4L, new FullTableName("other", "db", "t"));
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(catalog).when(database).getCatalog();
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getFullName()).thenReturn("db");
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(catalog).when(table).getCatalog();
        Mockito.doReturn(database).when(table).getDb();
        Mockito.when(table.getId()).thenReturn(3L);
        Mockito.when(table.getName()).thenReturn("t");

        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            ExternalMetaCacheMgr caches = new ExternalMetaCacheMgr(true);
            AtomicBoolean closingFence = new AtomicBoolean();
            ExternalRowCountCache rows = Mockito.mock(ExternalRowCountCache.class, invocation -> {
                if (!affected.isCurrent()) {
                    closingFence.set(true);
                }
                return Mockito.RETURNS_DEFAULTS.answer(invocation);
            });
            Deencapsulation.setField(caches, "rowCountCache", rows);
            ExternalMetaCache engine = Mockito.mock(ExternalMetaCache.class, invocation -> {
                String method = invocation.getMethod().getName();
                if (method.equals("engine")) {
                    return "test";
                }
                if (method.equals("isCatalogInitialized")) {
                    return true;
                }
                if (method.startsWith("invalidate")) {
                    Assertions.assertTrue(affected.isCurrent(), "engine invalidation precedes derived invalidation");
                    if (failure) {
                        throw new IllegalStateException("engine failure");
                    }
                }
                return Mockito.RETURNS_DEFAULTS.answer(invocation);
            });
            ExternalMetaCacheRegistry registry = Mockito.mock(ExternalMetaCacheRegistry.class);
            Mockito.when(registry.resolve("test")).thenReturn(engine);
            Deencapsulation.setField(caches, "cacheRegistry", registry);
            ExternalMetaCacheRouteResolver routes = Mockito.mock(ExternalMetaCacheRouteResolver.class);
            Mockito.when(routes.resolveCatalogCaches(1L, catalog)).thenReturn(ImmutableList.of(engine));
            Deencapsulation.setField(caches, "routeResolver", routes);
            Runnable invalidate = () -> {
                switch (operation) {
                    case "catalog":
                        caches.invalidateCatalog(1L);
                        break;
                    case "remove":
                        caches.removeCatalog(1L, "lake");
                        break;
                    case "context":
                        caches.onCatalogOperationalContextChanged(1L);
                        break;
                    case "database":
                        caches.invalidateDb(1L, 2L, "db");
                        break;
                    case "table":
                        caches.invalidateTable(1L, "db", "t");
                        break;
                    case "typed":
                        caches.invalidateTableCache(table);
                        break;
                    case "engine":
                        caches.invalidateTableByEngine(1L, "test", "db", "t");
                        break;
                    case "partitions":
                        caches.invalidatePartitions(1L, "db", "t", ImmutableList.of("p"));
                        break;
                    default:
                        throw new AssertionError(operation);
                }
            };
            if (failure) {
                Assertions.assertThrows(IllegalStateException.class, invalidate::run);
            } else {
                invalidate.run();
            }
            Assertions.assertFalse(affected.isCurrent());
            Assertions.assertTrue(unrelated.isCurrent());
            Mockito.verify(relations).invalidateRewriteCaches(Mockito.eq("lake"), Mockito.any(), Mockito.any());
            // Every scope must leave row counts fenced after the derived-cache callback completes.
            Assertions.assertTrue(closingFence.get(), "row-count invalidation must follow derived invalidation");
        }
    }
}
