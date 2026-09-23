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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheBudgetManager;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegeManager;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.AllTableOptionsTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class PaimonMetaCacheCatalogTest {
    private static final Identifier TABLE = Identifier.create("db", "t");

    @Test
    void disabledCacheOnlyKeepsTheInvalidationDecorator() throws Exception {
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<Identifier, String> sibling = owner.create(MetaCacheDefinition
                    .<Identifier, String>builder("paimon-sibling", CacheSpec.of(true, -1, 10),
                            id -> ScopePath.table(id.getDatabaseName(), id.getTableName()))
                    .build());
            Catalog result = PaimonMetaCacheCatalog.tryToCreate(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ZERO, Duration.ZERO),
                    false, false);

            Assertions.assertNotSame(recording.catalog(), result);
            Assertions.assertNotSame(result.getTable(TABLE), result.getTable(TABLE));
            Assertions.assertEquals(2, recording.tableLoads.get());
            sibling.put(TABLE, "stale");
            result.dropTable(TABLE, true);
            Assertions.assertNull(sibling.getIfPresent(TABLE));
        }
    }

    @Test
    void failedDecorationRollsBackCacheRegistrationsAndClosesCatalog() {
        RecordingCatalog failed = new RecordingCatalog();
        RecordingCatalog retry = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Assertions.assertThrows(IllegalStateException.class,
                    () -> PaimonMetaCacheCatalog.tryToCreate(failed.catalog(), owner,
                            100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                            true, false, ignored -> {
                                throw new IllegalStateException("privilege metadata is temporarily unavailable");
                            }));

            Assertions.assertTrue(owner.entries().isEmpty());
            Assertions.assertEquals(1, failed.closeCalls.get());
            Assertions.assertDoesNotThrow(() -> PaimonMetaCacheCatalog.tryToCreate(retry.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, false, catalog -> catalog));
            Assertions.assertEquals(2, owner.entries().size());
        }
    }

    @Test
    void disabledCacheDoesNotParseUnusedSettings() {
        Options options = new Options();
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS.key(), "invalid-duration");
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE.key(), "invalid-duration");
        options.set(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE.key(), "invalid-integer");
        options.set(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY.key(), "invalid-memory");
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Assertions.assertDoesNotThrow(() -> PaimonMetaCacheCatalog.tryToCreate(
                    recording.catalog(), owner, 100, 100, options,
                    false, false, catalog -> catalog));
        }
    }

    @Test
    void enclosingWeightLimitDoesNotParseUnusedSdkCacheSettings() {
        Options options = cacheOptions(Duration.ofDays(1), Duration.ofDays(1));
        options.set(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE.key(), "invalid-integer");
        options.set(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY.key(), "invalid-memory");
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Assertions.assertDoesNotThrow(() -> new PaimonMetaCacheCatalog(
                    recording.catalog(), owner, 100, 100, options, true, System::nanoTime));
        }
    }

    @Test
    void tableAndDatabaseHonorAccessAndWriteExpiry() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog accessRecording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(accessRecording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofSeconds(5), Duration.ofSeconds(100)),
                    false, clock::get);

            Assertions.assertSame(catalog.getTable(TABLE), catalog.getTable(TABLE));
            Assertions.assertSame(catalog.getDatabase("db"), catalog.getDatabase("db"));
            clock.set(Duration.ofSeconds(6).toNanos());
            catalog.getTable(TABLE);
            catalog.getDatabase("db");

            Assertions.assertEquals(2, accessRecording.tableLoads.get());
            Assertions.assertEquals(2, accessRecording.databaseLoads.get());
        }

        clock.set(0);
        RecordingCatalog writeRecording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(writeRecording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofSeconds(100), Duration.ofSeconds(10)),
                    false, clock::get);

            Table first = catalog.getTable(TABLE);
            clock.set(Duration.ofSeconds(5).toNanos());
            Assertions.assertSame(first, catalog.getTable(TABLE));
            clock.set(Duration.ofSeconds(11).toNanos());
            Assertions.assertNotSame(first, catalog.getTable(TABLE));
            Assertions.assertEquals(2, writeRecording.tableLoads.get());
        }
    }

    @Test
    void tableMissRaceDoesNotReturnAValueThatExpiredBeforePublicationWasObserved() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger misses = new AtomicInteger();
        CountDownLatch firstMiss = new CountDownLatch(1);
        CountDownLatch releaseFirstMiss = new CountDownLatch(1);
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofSeconds(1), Duration.ofDays(1)),
                    false, clock::get, (cache, key) -> {
                        if (cache.equals("table") && misses.incrementAndGet() == 1) {
                            firstMiss.countDown();
                            await(releaseFirstMiss);
                        }
                    });
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Table> racing = executor.submit(() -> catalog.getTable(TABLE));
                Assertions.assertTrue(firstMiss.await(10, TimeUnit.SECONDS));
                Table expired = catalog.getTable(TABLE);
                clock.set(Duration.ofSeconds(2).toNanos());
                releaseFirstMiss.countDown();

                Assertions.assertNotSame(expired, racing.get(10, TimeUnit.SECONDS));
                Assertions.assertEquals(2, recording.tableLoads.get());
            } finally {
                releaseFirstMiss.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void databaseMissRaceDoesNotReturnAValueThatExpiredBeforePublicationWasObserved() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger misses = new AtomicInteger();
        CountDownLatch firstMiss = new CountDownLatch(1);
        CountDownLatch releaseFirstMiss = new CountDownLatch(1);
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofSeconds(1), Duration.ofDays(1)),
                    false, clock::get, (cache, key) -> {
                        if (cache.equals("database") && misses.incrementAndGet() == 1) {
                            firstMiss.countDown();
                            await(releaseFirstMiss);
                        }
                    });
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Database> racing = executor.submit(() -> catalog.getDatabase("db"));
                Assertions.assertTrue(firstMiss.await(10, TimeUnit.SECONDS));
                Database expired = catalog.getDatabase("db");
                clock.set(Duration.ofSeconds(2).toNanos());
                releaseFirstMiss.countDown();

                Assertions.assertNotSame(expired, racing.get(10, TimeUnit.SECONDS));
                Assertions.assertEquals(2, recording.databaseLoads.get());
            } finally {
                releaseFirstMiss.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void acceptedLargeDurationsSaturateInsteadOfFailingCatalogCreation() throws Exception {
        RecordingCatalog recording = new RecordingCatalog();
        Options options = new Options();
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS.key(), "9223372037s");
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE.key(), "9223372037s");
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, Long.MAX_VALUE, options, false, System::nanoTime);

            Assertions.assertSame(catalog.getTable(TABLE), catalog.getTable(TABLE));
            Assertions.assertEquals(1, recording.tableLoads.get());
        }
    }

    @Test
    void lastAccessTimeNeverMovesBackward() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofSeconds(10), Duration.ofSeconds(100)),
                    false, clock::get);

            Table first = catalog.getTable(TABLE);
            clock.set(Duration.ofSeconds(5).toNanos());
            catalog.getTable(TABLE);
            clock.set(Duration.ofSeconds(1).toNanos());
            catalog.getTable(TABLE);
            clock.set(Duration.ofSeconds(12).toNanos());

            Assertions.assertSame(first, catalog.getTable(TABLE));
            Assertions.assertEquals(1, recording.tableLoads.get());
        }
    }

    @Test
    void baseTableInvalidationEvictsEveryBranchVariant() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        Identifier branch = new Identifier("db", "t", "dev", null);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, clock::get);

            Table mainBefore = catalog.getTable(TABLE);
            Table branchBefore = catalog.getTable(branch);
            owner.invalidateTable("db", "t");

            Assertions.assertNotSame(mainBefore, catalog.getTable(TABLE));
            Assertions.assertNotSame(branchBefore, catalog.getTable(branch));
            Assertions.assertEquals(4, recording.tableLoads.get());
        }
    }

    @Test
    void systemTableIsRebuiltFromTheCachedOriginTable() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        recording.fileStoreTables = true;
        Identifier systemTable = Identifier.create("db", "t$snapshots");
        Assertions.assertTrue(systemTable.isSystemTable());
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, clock::get);

            Assertions.assertNotNull(catalog.getTable(systemTable));
            Assertions.assertEquals(TABLE, recording.lastLoadedTable.get());
            Assertions.assertEquals(1, recording.tableLoads.get());
        }
    }

    @Test
    void nonFileStoreSystemTableIsDelegatedToTheWrappedCatalog() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        Identifier systemTable = Identifier.create("db", "t$snapshots");
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, clock::get);

            Assertions.assertNotNull(catalog.getTable(systemTable));
            Assertions.assertEquals(systemTable, recording.lastLoadedTable.get());
            Assertions.assertEquals(2, recording.tableLoads.get());
        }
    }

    @Test
    void eachSuccessfulDropEvictsBeforeALaterDropFails() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        Identifier first = Identifier.create("db", "first");
        Identifier second = Identifier.create("db", "second");
        recording.failDrop.set(second);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, clock::get);

            Table staleFirst = catalog.getTable(first);
            catalog.getTable(second);
            catalog.dropTable(first, true);
            Assertions.assertThrows(Catalog.TableNotExistException.class,
                    () -> catalog.dropTable(second, true));

            Assertions.assertNotSame(staleFirst, catalog.getTable(first));
        }
    }

    @Test
    void mutationEvictsWhenTheDelegateThrowsAfterTheRemoteChange() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog recording = new RecordingCatalog();
        recording.failAfterDrop.set(TABLE);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, clock::get);

            Table stale = catalog.getTable(TABLE);
            Assertions.assertThrows(IllegalStateException.class, () -> catalog.dropTable(TABLE, true));

            Assertions.assertNotSame(stale, catalog.getTable(TABLE));
            Assertions.assertEquals(2, recording.tableLoads.get());
        }
    }

    @Test
    void enclosingWeightLimitDoesNotAttachMutableSdkCaches() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingCatalog governedRecording = new RecordingCatalog();
        governedRecording.fileStoreTables = true;
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog governed = new PaimonMetaCacheCatalog(governedRecording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, clock::get);
            governed.getTable(TABLE);
            Assertions.assertEquals(0, governedRecording.sdkCacheAttachments.get());
        }

        RecordingCatalog ungovernedRecording = new RecordingCatalog();
        ungovernedRecording.fileStoreTables = true;
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog ungoverned = new PaimonMetaCacheCatalog(ungovernedRecording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, clock::get);
            ungoverned.getTable(TABLE);
            Assertions.assertEquals(3, ungovernedRecording.sdkCacheAttachments.get());
        }
    }

    @Test
    void realFileStoreTableIsAdmittedByTheWeightGovernedCache(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(
                warehouse.resolve("weighted-table").toUri());
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("payload", DataTypes.STRING())
                .option("file.format", "parquet")
                .build();
        new SchemaManager(fileIO, tablePath).createTable(schema);

        RecordingCatalog recording = new RecordingCatalog();
        recording.tableSupplier = () -> FileStoreTableFactory.create(fileIO, tablePath);
        MetaCacheBudgetManager budgetManager = new MetaCacheBudgetManager(OptionalLong.of(1024L * 1024L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                budgetManager, 67996L, "paimon", Collections.emptyMap())) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, System::nanoTime);

            Table first = catalog.getTable(TABLE);
            Assertions.assertSame(first, catalog.getTable(TABLE));
            Assertions.assertEquals(1, recording.tableLoads.get());
            Assertions.assertTrue(budgetManager.getGlobalUsedWeight() > 0L);
        }
        Assertions.assertEquals(0L, budgetManager.getGlobalUsedWeight());
    }

    @Test
    void allTableOptionsIsNotAdmittedWithoutACompleteRetainedSizeEstimate() throws Exception {
        Map<Identifier, Map<String, String>> allOptions = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            allOptions.put(Identifier.create("db", "table_" + i),
                    Collections.singletonMap("large-option", "x".repeat(100)));
        }
        RecordingCatalog recording = new RecordingCatalog();
        recording.tableSupplier = () -> new AllTableOptionsTable(allOptions);
        MetaCacheBudgetManager budgetManager = new MetaCacheBudgetManager(OptionalLong.of(512L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                budgetManager, 67996L, "paimon", Collections.emptyMap())) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, System::nanoTime);

            catalog.getTable(Identifier.create("sys", AllTableOptionsTable.ALL_TABLE_OPTIONS));
            catalog.getTable(Identifier.create("sys", AllTableOptionsTable.ALL_TABLE_OPTIONS));
            Assertions.assertEquals(2, recording.tableLoads.get());
            Assertions.assertEquals(0L, budgetManager.getGlobalUsedWeight());
        }
    }

    @Test
    void fallbackBranchesAreIncludedInWeightGovernedAdmission(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        FileStoreTable main = createFileStoreTable(fileIO, warehouse.resolve("main"), "main_payload");
        FileStoreTable fallback = createFileStoreTable(fileIO, warehouse.resolve("fallback"), "fallback_payload");
        FileStoreTable decorated = new FallbackReadFileStoreTable(main, fallback);
        long mainWeight = PaimonCacheSizeEstimator.estimateTable(
                TABLE, main, PaimonMetaCacheCatalog.TABLE_ENTRY_OVERHEAD_BYTES).getBytes();
        long decoratedWeight = PaimonCacheSizeEstimator.estimateTable(
                TABLE, decorated, PaimonMetaCacheCatalog.TABLE_ENTRY_OVERHEAD_BYTES).getBytes();
        long sharedBranchWeight = PaimonCacheSizeEstimator.estimateTable(
                TABLE, new FallbackReadFileStoreTable(main, main),
                PaimonMetaCacheCatalog.TABLE_ENTRY_OVERHEAD_BYTES).getBytes();

        Assertions.assertTrue(decoratedWeight > mainWeight);
        Assertions.assertTrue(decoratedWeight > sharedBranchWeight,
                "the same branch object must be counted once by identity");
        Assertions.assertFalse(PaimonCacheSizeEstimator.estimateTable(TABLE,
                PrivilegedFileStoreTable.wrap(decorated, privilegeChecker(true), TABLE),
                PaimonMetaCacheCatalog.TABLE_ENTRY_OVERHEAD_BYTES).isComplete(),
                "an authorization snapshot must never be admitted to the raw metadata cache");

        RecordingCatalog recording = new RecordingCatalog();
        recording.tableSupplier = () -> new FallbackReadFileStoreTable(main, fallback);
        MetaCacheBudgetManager budgetManager = new MetaCacheBudgetManager(
                OptionalLong.of(mainWeight));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                budgetManager, 67996L, "paimon", Collections.emptyMap())) {
            PaimonMetaCacheCatalog catalog = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    true, System::nanoTime);

            Assertions.assertNotSame(catalog.getTable(TABLE), catalog.getTable(TABLE));
            Assertions.assertEquals(2, recording.tableLoads.get());
            Assertions.assertEquals(0L, budgetManager.getGlobalUsedWeight());
        }
    }

    @Test
    void privilegeCheckerIsRefreshedOutsideTheRawTableCache() throws Exception {
        AtomicReference<Boolean> canSelect = new AtomicReference<>(true);
        RecordingCatalog recording = new RecordingCatalog();
        recording.fileStoreTables = true;
        PrivilegeManager privilegeManager = (PrivilegeManager) Proxy.newProxyInstance(
                PrivilegeManager.class.getClassLoader(), new Class<?>[] {PrivilegeManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getPrivilegeChecker")) {
                        boolean snapshot = canSelect.get();
                        return privilegeChecker(snapshot);
                    }
                    return defaultValue(method.getReturnType());
                });
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog cached = new PaimonMetaCacheCatalog(recording.catalog(), owner,
                    100, 100, cacheOptions(Duration.ofDays(1), Duration.ofDays(1)),
                    false, System::nanoTime);
            Catalog privileged = new PrivilegedCatalog(cached, () -> privilegeManager);

            FileStoreTable beforeRevoke = (FileStoreTable) privileged.getTable(TABLE);
            Assertions.assertDoesNotThrow(beforeRevoke::newScan);
            canSelect.set(false);
            FileStoreTable afterRevoke = (FileStoreTable) privileged.getTable(TABLE);

            Assertions.assertNotSame(beforeRevoke, afterRevoke);
            Assertions.assertThrows(IllegalStateException.class, afterRevoke::newScan);
            Assertions.assertEquals(1, recording.tableLoads.get(),
                    "revocation must refresh authorization without reloading raw metadata");
        }
    }

    @Test
    void restDispatchSeesThroughTheMetaCacheWrapper() {
        Options options = cacheOptions(Duration.ofDays(1), Duration.ofDays(1));
        options.set("uri", "http://localhost:1");
        options.set("prefix", "test-prefix");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        RESTCatalog rest = new RESTCatalog(CatalogContext.create(options), false);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            PaimonMetaCacheCatalog wrapped = new PaimonMetaCacheCatalog(rest, owner,
                    100, 100, options, true, System::nanoTime);

            Assertions.assertSame(rest,
                    PaimonCatalogOps.CatalogBackedPaimonCatalogOps.restCatalog(wrapped));
        }
    }

    private static Options cacheOptions(Duration access, Duration write) {
        Options options = new Options();
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS, access);
        options.set(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE, write);
        return options;
    }

    private static FileStoreTable createFileStoreTable(
            LocalFileIO fileIO, java.nio.file.Path path, String payloadColumn) throws Exception {
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(path.toUri());
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column(payloadColumn, DataTypes.STRING())
                .option("file.format", "parquet")
                .build();
        new SchemaManager(fileIO, tablePath).createTable(schema);
        return FileStoreTableFactory.create(fileIO, tablePath);
    }

    private static PrivilegeChecker privilegeChecker(boolean canSelect) {
        return (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(), new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("assertCanSelect") && !canSelect) {
                        throw new IllegalStateException("SELECT privilege was revoked");
                    }
                    return null;
                });
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            if (returnType == Optional.class) {
                return Optional.empty();
            }
            if (returnType == Map.class) {
                return Collections.emptyMap();
            }
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == char.class) {
            return '\0';
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        return 0D;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for cache race");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for cache race", e);
        }
    }

    private static final class RecordingCatalog {
        private final AtomicInteger tableLoads = new AtomicInteger();
        private final AtomicInteger databaseLoads = new AtomicInteger();
        private final AtomicInteger sdkCacheAttachments = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicReference<Identifier> lastLoadedTable = new AtomicReference<>();
        private final AtomicReference<Identifier> failDrop = new AtomicReference<>();
        private final AtomicReference<Identifier> failAfterDrop = new AtomicReference<>();
        private final Catalog catalog;
        private boolean fileStoreTables;
        private Supplier<Table> tableSupplier = this::newTable;

        private RecordingCatalog() {
            AtomicReference<Catalog> self = new AtomicReference<>();
            this.catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                    new Class<?>[] {Catalog.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getTable":
                                Identifier identifier = (Identifier) args[0];
                                lastLoadedTable.set(identifier);
                                tableLoads.incrementAndGet();
                                return tableSupplier.get();
                            case "getDatabase":
                                databaseLoads.incrementAndGet();
                                return Database.of((String) args[0]);
                            case "dropTable":
                                if (args[0].equals(failDrop.get())) {
                                    throw new Catalog.TableNotExistException((Identifier) args[0]);
                                }
                                if (args[0].equals(failAfterDrop.get())) {
                                    throw new IllegalStateException("post-mutation cleanup failed");
                                }
                                return null;
                            case "catalogLoader":
                                return (org.apache.paimon.catalog.CatalogLoader) self::get;
                            case "close":
                                closeCalls.incrementAndGet();
                                return null;
                            case "options":
                                return Collections.emptyMap();
                            case "toString":
                                return "RecordingCatalog";
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
            self.set(catalog);
        }

        private Catalog catalog() {
            return catalog;
        }

        private Table newTable() {
            Class<?> tableType = fileStoreTables ? FileStoreTable.class : Table.class;
            return (Table) Proxy.newProxyInstance(tableType.getClassLoader(), new Class<?>[] {tableType},
                    (proxy, method, args) -> {
                        if (method.getName().startsWith("set") && method.getName().endsWith("Cache")) {
                            sdkCacheAttachments.incrementAndGet();
                            return null;
                        }
                        if (method.getName().equals("toString")) {
                            return "RecordingTable";
                        }
                        return defaultValue(method.getReturnType());
                    });
        }
    }
}
