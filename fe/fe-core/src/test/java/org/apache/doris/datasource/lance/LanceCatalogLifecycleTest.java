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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.lance.job.LanceIndexDatasetLocator;
import org.apache.doris.persist.EditLog;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LanceCatalogLifecycleTest {
    @BeforeAll
    public static void initializeEnvironment() {
        // onRefreshCache(true) invalidates the global metadata cache. Initialize Env before
        // timed concurrency checks so first-use FE startup is not mistaken for a blocked refresh.
        Env.getCurrentEnv().getExtMetaCacheMgr();
    }

    @Test
    public void testCatalogRefreshPreservesAdmittedReadAndSwitchesNewReads() throws Exception {
        Session oldSession = Mockito.mock(Session.class);
        LanceCatalogClient oldClient = client(oldSession);
        LanceCatalogClient replacement = client(Mockito.mock(Session.class));
        LanceExternalCatalog catalog = catalog(oldClient);
        Mockito.doReturn(replacement).when(catalog).createClient();
        try {
            try (LanceCatalogClient.Lease admitted = catalog.acquireClient()) {
                catalog.onRefreshCache(false);
                Mockito.verify(catalog, Mockito.never()).createClient();
                catalog.onRefreshCache(true);
                Mockito.verify(oldSession, Mockito.never()).close();
                Assertions.assertTrue(admitted.client().tableExists("default", "table"));
                try (LanceCatalogClient.Lease next = catalog.acquireClient()) {
                    Assertions.assertSame(replacement, next.client());
                }
            }
            Mockito.verify(oldSession).close();
            Assertions.assertThrows(IllegalStateException.class, oldClient::acquire);
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testSlowRefreshDoesNotBlockReadsAndCannotResurrectClosedCatalog() throws Exception {
        LanceCatalogClient oldClient = client(Mockito.mock(Session.class));
        Session replacementSession = Mockito.mock(Session.class);
        LanceCatalogClient replacement = client(replacementSession);
        LanceExternalCatalog catalog = catalog(oldClient);
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            building.countDown();
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            return replacement;
        }).when(catalog).createClient();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> refresh = executor.submit(catalog::refreshSessionCache);
            Assertions.assertTrue(building.await(10, TimeUnit.SECONDS));
            Future<Boolean> read = executor.submit(() -> catalog.tableExist(null, "default", "table"));
            Assertions.assertTrue(read.get(10, TimeUnit.SECONDS));
            catalog.onClose();
            finish.countDown();
            refresh.get(10, TimeUnit.SECONDS);
            Mockito.verify(replacementSession).close();
            Assertions.assertThrows(IllegalStateException.class, catalog::acquireClient);
        } finally {
            finish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            catalog.onClose();
        }
    }

    @Test
    public void testRefreshDoesNotWaitForAnInFlightNamespaceRead() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Session oldSession = Mockito.mock(Session.class);
        LanceCatalogClient oldClient = new LanceCatalogClient(namespace, Mockito.mock(BufferAllocator.class),
                oldSession, "filesystem", "default", Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), Collections.emptyList());
        LanceExternalCatalog catalog = catalog(oldClient);
        Mockito.doReturn(client(Mockito.mock(Session.class))).when(catalog).createClient();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            entered.countDown();
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            return null;
        }).when(namespace).tableExists(Mockito.any());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> read = executor.submit(() -> catalog.tableExist(null, "default", "table"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<?> refresh = executor.submit(() -> catalog.onRefreshCache(true));
            refresh.get(10, TimeUnit.SECONDS);
            Mockito.verify(oldSession, Mockito.never()).close();
            Assertions.assertTrue(catalog.tableExist(null, "default", "table"));
            finish.countDown();
            Assertions.assertTrue(read.get(10, TimeUnit.SECONDS));
            Mockito.verify(oldSession).close();
        } finally {
            finish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            catalog.onClose();
        }
    }

    @Test
    public void testConcurrentRefreshDiscardsSupersededReplacement() throws Exception {
        LanceCatalogClient oldClient = client(Mockito.mock(Session.class));
        Session discardedSession = Mockito.mock(Session.class);
        LanceCatalogClient discarded = client(discardedSession);
        LanceCatalogClient winner = client(Mockito.mock(Session.class));
        LanceExternalCatalog catalog = catalog(oldClient);
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            building.countDown();
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            return discarded;
        }).doReturn(winner).when(catalog).createClient();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> refresh = executor.submit(catalog::refreshSessionCache);
            Assertions.assertTrue(building.await(10, TimeUnit.SECONDS));
            catalog.refreshSessionCache();
            finish.countDown();
            refresh.get(10, TimeUnit.SECONDS);
            try (LanceCatalogClient.Lease next = catalog.acquireClient()) {
                Assertions.assertSame(winner, next.client());
            }
            Mockito.verify(discardedSession).close();
        } finally {
            finish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            catalog.onClose();
        }
    }

    @Test
    public void testFailedRefreshLeavesCurrentClientUsable() throws Exception {
        LanceCatalogClient oldClient = client(Mockito.mock(Session.class));
        LanceExternalCatalog catalog = catalog(oldClient);
        Mockito.doThrow(new IllegalStateException("creation failed")).when(catalog).createClient();
        try {
            Assertions.assertThrows(IllegalStateException.class, catalog::refreshSessionCache);
            try (LanceCatalogClient.Lease next = catalog.acquireClient()) {
                Assertions.assertSame(oldClient, next.client());
                Assertions.assertTrue(next.client().tableExists("default", "table"));
            }
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testMetadataRefreshInvalidatesAccessWithoutClosingSession() throws Exception {
        Session session = Mockito.mock(Session.class);
        LanceCatalogClient client = Mockito.spy(client(session));
        LanceExternalCatalog catalog = catalog(client);
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        long catalogId = catalog.getId();
        Mockito.doReturn(catalog).when(catalogs).getCatalog(catalogId);
        ExternalMetaCacheMgr caches = new ExternalMetaCacheMgr(true);
        try (MockedStatic<Env> currentEnv = Mockito.mockStatic(Env.class)) {
            currentEnv.when(Env::getCurrentEnv).thenReturn(env);
            caches.invalidateTable(catalog.getId(), "mapped_db", "mapped_table");
            caches.invalidateDb(catalog.getId(), "mapped_db");
            caches.invalidateCatalog(catalog.getId());
            Mockito.verify(client, Mockito.times(2)).invalidateTableAccessCache();
            Mockito.verify(session, Mockito.never()).close();
            Mockito.verify(catalog, Mockito.never()).createClient();
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testIndexJobLocatorBypassesQueryAccessCache() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("file:///warehouse/original.lance"),
                new DescribeTableResponse().tableUri("file:///warehouse/replacement.lance"));
        try (LanceCatalogClient client = new LanceCatalogClient(namespace, Mockito.mock(BufferAllocator.class),
                Mockito.mock(Session.class), "filesystem", "default", Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), Collections.emptyList())) {
            Field field = LanceCatalogClient.class.getDeclaredField("namespaceClient");
            field.setAccessible(true);
            LanceNamespaceClient namespaceClient = (LanceNamespaceClient) field.get(client);
            namespaceClient.resolveTableAccess("default", "items");
            Assertions.assertEquals(LanceIndexDatasetLocator.normalize("file:///warehouse/replacement.lance"),
                    client.resolveCurrentIndexJobLocator("default", "items"));
            Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
        }
    }

    @Test
    public void testRoutineDatabaseObjectCleanupPreservesHotAccess() throws Exception {
        try (AccessFixture fixture = new AccessFixture()) {
            LanceNamespaceClient access = fixture.access();
            access.resolveTableAccess("default", "items");
            new LanceExternalDatabase(fixture.catalog, 1, "cold_db", "cold_db").resetMetaToUninitialized();
            access.resolveTableAccess("default", "items");
            Mockito.verify(fixture.namespace).describeTable(Mockito.any());
        }
    }

    @Test
    public void testRefreshReplayInvalidatesAccessWithMissingObjects() throws Exception {
        for (boolean missingDatabase : new boolean[] {false, true}) {
            for (boolean legacyIds : new boolean[] {false, true}) {
                try (AccessFixture fixture = new AccessFixture()) {
                    LanceNamespaceClient access = fixture.access();
                    access.resolveTableAccess("default", "items");
                    LanceExternalDatabase database = Mockito.mock(LanceExternalDatabase.class);
                    Mockito.doReturn(missingDatabase ? Optional.empty() : Optional.of(database))
                            .when(fixture.catalog).getDbForReplay("mapped_db");
                    Mockito.doReturn(missingDatabase ? Optional.empty() : Optional.of(database))
                            .when(fixture.catalog).getDbForReplay(1L);
                    ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                            fixture.catalog.getId(), "mapped_db", "mapped_table", 0);
                    if (legacyIds) {
                        log.setDbName(null);
                        log.setTableName(null);
                        log.setDbId(1L);
                        log.setTableId(2L);
                    }
                    // Access entries can survive eviction of the smaller database/table object caches.
                    new RefreshManager().replayRefreshTable(log);
                    access.resolveTableAccess("default", "items");
                    Mockito.verify(fixture.namespace, Mockito.times(2)).describeTable(Mockito.any());
                    Mockito.verify(fixture.catalog, Mockito.never()).createClient();
                }
            }
        }
    }

    @Test
    public void testDatabaseRefreshReplayWithoutDatabaseObject() throws Exception {
        try (AccessFixture fixture = new AccessFixture()) {
            LanceNamespaceClient access = fixture.access();
            access.resolveTableAccess("default", "items");
            Mockito.doReturn(Optional.empty()).when(fixture.catalog).getDbForReplay("mapped_db");
            new RefreshManager().replayRefreshDb(
                    ExternalObjectLog.createForRefreshDb(fixture.catalog.getId(), "mapped_db"));
            access.resolveTableAccess("default", "items");
            Mockito.verify(fixture.namespace, Mockito.times(2)).describeTable(Mockito.any());
        }
    }

    @Test
    public void testExplicitDatabaseRefreshInvalidatesAccess() throws Exception {
        try (AccessFixture fixture = new AccessFixture()) {
            LanceNamespaceClient access = fixture.access();
            access.resolveTableAccess("default", "items");
            LanceExternalDatabase database = new LanceExternalDatabase(fixture.catalog, 1, "mapped_db", "default");
            Mockito.doReturn(database).when(fixture.catalog).getDbOrDdlException("mapped_db");
            new RefreshManager().handleRefreshDb("lifecycle", "mapped_db");
            access.resolveTableAccess("default", "items");
            Mockito.verify(fixture.namespace, Mockito.times(2)).describeTable(Mockito.any());
        }
    }

    @Test
    public void testNamespaceRemovalInvalidatesAccessWithoutDatabaseObjects() throws Exception {
        try (AccessFixture fixture = new AccessFixture()) {
            LanceNamespaceClient access = fixture.access();
            access.resolveTableAccess("default", "items");
            setField(ExternalCatalog.class, fixture.catalog, "initialized", false);
            fixture.catalog.unregisterDatabase("mapped_db");
            access.resolveTableAccess("default", "items");
            Mockito.verify(fixture.namespace, Mockito.times(2)).describeTable(Mockito.any());
            Mockito.verify(fixture.catalog, Mockito.never()).createClient();
        }
    }

    private static final class AccessFixture implements AutoCloseable {
        private final LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        private final LanceCatalogClient client = new LanceCatalogClient(namespace,
                Mockito.mock(BufferAllocator.class), Mockito.mock(Session.class), "filesystem", "default",
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyList());
        private final LanceExternalCatalog catalog = catalog(client);
        private final MockedStatic<Env> currentEnv;

        private AccessFixture() throws Exception {
            Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                    new DescribeTableResponse().tableUri("file:///warehouse/items.lance"));
            ExternalMetaCacheMgr caches = Env.getCurrentEnv().getExtMetaCacheMgr();
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(caches);
            Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
            Mockito.doReturn(catalog).when(catalogs).getCatalog("lifecycle");
            long catalogId = catalog.getId();
            Mockito.doReturn(catalog).when(catalogs).getCatalog(catalogId);
            currentEnv = Mockito.mockStatic(Env.class);
            currentEnv.when(Env::getCurrentEnv).thenReturn(env);
        }

        private LanceNamespaceClient access() throws Exception {
            Field field = LanceCatalogClient.class.getDeclaredField("namespaceClient");
            field.setAccessible(true);
            return (LanceNamespaceClient) field.get(client);
        }

        @Override
        public void close() {
            currentEnv.close();
            catalog.onClose();
        }
    }

    private static LanceCatalogClient client(Session session) {
        return new LanceCatalogClient(Mockito.mock(LanceNamespace.class), Mockito.mock(BufferAllocator.class),
                session, "filesystem", "default", Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), Collections.emptyList());
    }

    private static LanceExternalCatalog catalog(LanceCatalogClient client) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, "/unused/lance-warehouse");
        LanceExternalCatalog catalog = Mockito.spy(new LanceExternalCatalog(902, "lifecycle", null, properties, ""));
        // Isolate the resource lifecycle from external metadata-cache initialization.
        setField(ExternalCatalog.class, catalog, "objectCreated", true);
        setField(ExternalCatalog.class, catalog, "initialized", true);
        setField(LanceExternalCatalog.class, catalog, "client", client);
        return catalog;
    }

    private static void setField(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
