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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import com.google.common.collect.Lists;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

class IcebergDatabaseResolutionTest {
    @Test
    void nativeDdlResolutionDoesNotDeadlockWithEvictedParentRepair() throws Exception {
        verifyEvictionRace(false);
    }

    @Test
    void hmsDdlResolutionDoesNotDeadlockWithEvictedParentRepair() throws Exception {
        verifyEvictionRace(true);
    }

    private void verifyEvictionRace(boolean hms) throws Exception {
        try (Fixture fixture = new Fixture(hms)) {
            Future<?> otherLoad = fixture.executor.submit(() -> fixture.parents.getMetaObj("other", 20L));
            Assertions.assertTrue(fixture.otherStarted.await(5, TimeUnit.SECONDS));
            fixture.beforeLookup.set(() -> {
                fixture.resolutionEntered.countDown();
                await(fixture.allowLookup);
            });
            Future<Boolean> ddl = fixture.executor.submit(() -> fixture.ops.performCreateTable(fixture.createInfo));
            Assertions.assertTrue(fixture.resolutionEntered.await(5, TimeUnit.SECONDS));
            Future<?> repair = fixture.executor.submit(fixture.repair);
            Assertions.assertTrue(fixture.repairHasKey.await(5, TimeUnit.SECONDS));
            fixture.allowOtherPublication.countDown();
            otherLoad.get(5, TimeUnit.SECONDS);
            fixture.parents.getMetaObjCache().cleanUp();
            Assertions.assertFalse(fixture.parents.tryGetMetaObj("db").isPresent(),
                    "The other DB publication must capacity-evict the retained parent");
            fixture.allowLookup.countDown();
            fixture.allowRepairMonitor.countDown();
            Assertions.assertTrue(ddl.get(5, TimeUnit.SECONDS));
            repair.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void nativeRuntimeChangeDuringResolutionIsRejected() throws Exception {
        verifyRuntimeChange(false);
    }

    @Test
    void hmsRuntimeChangeDuringResolutionIsRejected() throws Exception {
        verifyRuntimeChange(true);
    }

    private void verifyRuntimeChange(boolean hms) throws Exception {
        try (Fixture fixture = new Fixture(hms)) {
            fixture.beforeLookup.set(fixture::replaceRuntime);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> fixture.ops.performCreateTable(fixture.createInfo));
            Mockito.verify(fixture.sdk, Mockito.never()).tableExists(Mockito.any());
        }
    }

    @Test
    void retiredRuntimeDoesNotResolveDatabase() throws Exception {
        for (boolean hms : new boolean[] {false, true}) {
            try (Fixture fixture = new Fixture(hms)) {
                fixture.replaceRuntime();
                Assertions.assertThrows(IllegalStateException.class,
                        () -> fixture.ops.performCreateTable(fixture.createInfo));
                Assertions.assertEquals(0, fixture.lookupCalls.get());
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class Fixture implements AutoCloseable {
        private final CountDownLatch resolutionEntered = new CountDownLatch(1);
        private final CountDownLatch allowLookup = new CountDownLatch(1);
        private final CountDownLatch repairHasKey = new CountDownLatch(1);
        private final CountDownLatch allowRepairMonitor = new CountDownLatch(1);
        private final CountDownLatch otherStarted = new CountDownLatch(1);
        private final CountDownLatch allowOtherPublication = new CountDownLatch(1);
        private final AtomicReference<Runnable> beforeLookup = new AtomicReference<>(() -> { });
        private final AtomicInteger lookupCalls = new AtomicInteger();
        private final ExecutorService executor = Executors.newFixedThreadPool(3, task -> {
            Thread thread = new Thread(task, "iceberg-db-resolution-test");
            // A failing old implementation can leave synchronized deadlock participants behind.
            thread.setDaemon(true);
            return thread;
        });
        private final ExternalCatalog catalog;
        private final IcebergMetadataOps ops;
        private final Catalog sdk;
        private final MetaCache<ExternalDatabase<? extends ExternalTable>> parents;
        private final CreateTableInfo createInfo = Mockito.mock(CreateTableInfo.class);
        private final Runnable repair;
        private final boolean hms;

        Fixture(boolean hms) {
            Env.getCurrentEnv();
            this.hms = hms;
            catalog = hms ? new HMSExternalCatalog(990002L, "hms-lock-test", null, Collections.emptyMap(), "") {
                @Override
                public ExternalDatabase<? extends ExternalTable> getDbNullable(String name) {
                    lookupCalls.incrementAndGet();
                    beforeLookup.get().run();
                    return super.getDbNullable(name);
                }
            } : new IcebergExternalCatalog(990001L, "iceberg-lock-test", "") {
                @Override
                public ExternalDatabase<? extends ExternalTable> getDbNullable(String name) {
                    lookupCalls.incrementAndGet();
                    beforeLookup.get().run();
                    return super.getDbNullable(name);
                }
            };
            Deencapsulation.setField(catalog, "catalogProperty", new CatalogProperty(null, Collections.emptyMap()));
            Deencapsulation.setField(catalog, "executionAuthenticator", new ExecutionAuthenticator() { });
            Deencapsulation.setField(catalog, "objectCreated", true);
            Deencapsulation.setField(catalog, "initialized", true);
            sdk = Mockito.mock(Catalog.class, Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
            ops = new IcebergMetadataOps(catalog, sdk);
            Deencapsulation.setField(catalog, hms ? "icebergMetadataOps" : "metadataOps", ops);
            ExternalDatabase<? extends ExternalTable> retained = newDatabase("db", 10L);
            ExternalDatabase<? extends ExternalTable> reloaded = newDatabase("db", 10L);
            ExternalDatabase<? extends ExternalTable> other = newDatabase("other", 20L);
            parents = new MetaCache<ExternalDatabase<? extends ExternalTable>>("parents", executor,
                    OptionalLong.empty(), OptionalLong.empty(), 1,
                    ignored -> Lists.newArrayList(Pair.of("db", "db"), Pair.of("other", "other")),
                    key -> {
                        if (key.equals("other")) {
                            otherStarted.countDown();
                            await(allowOtherPublication);
                            return Optional.of(other);
                        }
                        synchronized (catalog) {
                            return Optional.of(reloaded);
                        }
                    }, null) {
                @Override
                public boolean executeIfMetaObjCurrent(String name,
                        ExternalDatabase<? extends ExternalTable> expected, BooleanSupplier action) {
                    return super.executeIfMetaObjCurrent(name, expected, () -> {
                        repairHasKey.countDown();
                        await(allowRepairMonitor);
                        return action.getAsBoolean();
                    });
                }
            };
            parents.refreshNames();
            parents.updateCache("db", "db", retained, 10L);
            Deencapsulation.setField(catalog, "metaCache", parents);
            MetaCache<ExternalTable> tables = new MetaCache<>("tables", executor,
                    OptionalLong.empty(), OptionalLong.empty(), 10,
                    ignored -> Lists.newArrayList(Pair.of("t", "t")), ignored -> Optional.empty(), null);
            ExternalTable table = Mockito.mock(ExternalTable.class);
            Mockito.when(table.getName()).thenReturn("t");
            Mockito.when(table.getRemoteName()).thenReturn("t");
            tables.refreshNames();
            tables.updateCache("t", "t", table, 1L);
            Deencapsulation.setField(retained, "metaCache", tables);
            Deencapsulation.setField(retained, "initialized", true);
            Deencapsulation.invoke(retained, "acquireTableMetadataLoadEpoch");
            repair = retained.observeTableLookup(table).get();
            Mockito.when(createInfo.getDbName()).thenReturn("db");
            Mockito.when(createInfo.getTableName()).thenReturn("t");
            Mockito.when(createInfo.isIfNotExists()).thenReturn(true);
            Mockito.when(sdk.tableExists(TableIdentifier.of("db", "t"))).thenReturn(true);
        }

        private ExternalDatabase<? extends ExternalTable> newDatabase(String name, long id) {
            return hms ? new HMSExternalDatabase(catalog, id, name, name)
                    : new IcebergExternalDatabase(catalog, id, name, name);
        }

        private void replaceRuntime() {
            synchronized (catalog) {
                Deencapsulation.setField(catalog, hms ? "icebergMetadataOps" : "metadataOps",
                        Mockito.mock(IcebergMetadataOps.class));
            }
        }

        @Override
        public void close() {
            allowLookup.countDown();
            allowRepairMonitor.countDown();
            allowOtherPublication.countDown();
            executor.shutdownNow();
        }
    }
}
