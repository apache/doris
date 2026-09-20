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
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    private static final class RecordingCatalog {
        private final AtomicInteger tableLoads = new AtomicInteger();
        private final AtomicInteger databaseLoads = new AtomicInteger();
        private final AtomicInteger sdkCacheAttachments = new AtomicInteger();
        private final AtomicReference<Identifier> lastLoadedTable = new AtomicReference<>();
        private final AtomicReference<Identifier> failDrop = new AtomicReference<>();
        private final AtomicReference<Identifier> failAfterDrop = new AtomicReference<>();
        private final Catalog catalog;
        private boolean fileStoreTables;

        private RecordingCatalog() {
            AtomicReference<Catalog> self = new AtomicReference<>();
            this.catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                    new Class<?>[] {Catalog.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getTable":
                                Identifier identifier = (Identifier) args[0];
                                lastLoadedTable.set(identifier);
                                tableLoads.incrementAndGet();
                                return newTable();
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
