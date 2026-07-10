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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link HiveConnector#invalidateTable}/{@link HiveConnector#invalidateAll} — the REFRESH TABLE / REFRESH
 * CATALOG hooks that arm the connector-owned D2 caches.
 *
 * <p>WHY (Rule 9): a flipped hive catalog's caches never expire on external change (event sync is off until the
 * event Model B step), so {@code REFRESH TABLE} / {@code REFRESH CATALOG} are the user's explicit way to see new
 * data. fe-core already routes them to {@code connector.invalidateTable} / {@code invalidateAll}; these tests pin
 * that the hive connector then drops BOTH cache layers — the metastore-metadata cache ({@link CachingHmsClient})
 * AND the directory-listing cache ({@link HiveFileListingCache}) — because a hive table's schema, partitions and
 * files are all mutable (unlike iceberg's immutable manifests). {@code invalidateTable} is scoped to one table;
 * {@code invalidateAll} clears everything; and the public hooks never force-build a metastore client just to
 * flush (a REFRESH on a never-scanned catalog must be a cheap no-op on the metastore side).</p>
 *
 * <p>Dormant: {@code "hms"} is not in {@code SPI_READY_TYPES}; this drives the hooks directly.</p>
 */
public class HiveConnectorInvalidateTest {

    private static final Configuration CONF = new Configuration();

    private static Map<String, String> props() {
        Map<String, String> m = new HashMap<>();
        m.put("hive.metastore.uris", "thrift://host:9083");
        return m;
    }

    @Test
    public void invalidateTableFlushesBothCachesForThatTableOnly(@TempDir java.nio.file.Path dirA,
            @TempDir java.nio.file.Path dirB) throws Exception {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        // Metastore-metadata cache: populate t1 and t2 (each one RPC, then cached).
        cachingClient.getTable("db", "t1");
        cachingClient.getTable("db", "t2");
        Assertions.assertEquals(2, raw.getTableCalls);

        // Directory-listing cache: populate t1 (dirA) and t2 (dirB) from real local dirs.
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        Files.write(dirA.resolve("f"), new byte[10]);
        Files.write(dirB.resolve("f"), new byte[10]);
        fileCache.listDataFiles("db", "t1", dirA.toUri().toString(), CONF);
        fileCache.listDataFiles("db", "t2", dirB.toUri().toString(), CONF);
        Assertions.assertEquals(2, fileCache.size());

        connector.invalidateTable(cachingClient, "db", "t1");

        // Metastore cache: t1 re-fetches; t2 (a different table) is still served from the cache.
        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(3, raw.getTableCalls, "REFRESH TABLE must drop t1's metastore entry");
        cachingClient.getTable("db", "t2");
        Assertions.assertEquals(3, raw.getTableCalls, "REFRESH TABLE must NOT drop another table's metastore entry");

        // File cache: t1's listing dropped, t2's survives (invalidateTable is scoped by (db, table)).
        Assertions.assertEquals(1, fileCache.size(), "REFRESH TABLE must drop only that table's directory listings");
    }

    @Test
    public void invalidateAllFlushesBothCachesEntirely(@TempDir java.nio.file.Path dir) throws Exception {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(1, raw.getTableCalls);
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        Files.write(dir.resolve("f"), new byte[10]);
        fileCache.listDataFiles("db", "t1", dir.toUri().toString(), CONF);
        Assertions.assertEquals(1, fileCache.size());

        connector.invalidateAll(cachingClient);

        // Both caches fully cleared: the metastore entry re-fetches and the file cache is empty.
        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(2, raw.getTableCalls, "REFRESH CATALOG must drop the metastore cache");
        Assertions.assertEquals(0, fileCache.size(), "REFRESH CATALOG must drop the directory-listing cache");
    }

    @Test
    public void publicHooksAreNoThrowAndClearFileCacheWithoutBuildingAClient(@TempDir java.nio.file.Path dir)
            throws Exception {
        // A fresh connector never built its metastore client (hmsClient == null). The public hooks must not
        // force-build one (a REFRESH on a never-scanned catalog is a cheap no-op on the metastore side), must not
        // throw on the null client, and must still clear the file cache.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        Files.write(dir.resolve("f"), new byte[10]);

        fileCache.listDataFiles("db", "t", dir.toUri().toString(), CONF);
        Assertions.assertEquals(1, fileCache.size());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db", "t"));
        Assertions.assertEquals(0, fileCache.size(), "REFRESH TABLE clears the file cache even with no client built");

        fileCache.listDataFiles("db", "t", dir.toUri().toString(), CONF);
        Assertions.assertEquals(1, fileCache.size());
        Assertions.assertDoesNotThrow(() -> connector.invalidateAll());
        Assertions.assertEquals(0, fileCache.size(), "REFRESH CATALOG clears the file cache even with no client built");
    }

    /**
     * Minimal {@link HmsClient} that counts {@code getTable} calls and returns a fresh table info per call (so a
     * cache hit — same instance — is distinguishable from a reload). Only the abstract read methods are stubbed;
     * the write/txn methods are interface defaults.
     */
    private static final class RecordingHmsClient implements HmsClient {
        int getTableCalls;

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            getTableCalls++;
            return HmsTableInfo.builder().dbName(dbName).tableName(tableName).build();
        }

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            return null;
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return false;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return Collections.emptyList();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            return Collections.emptyList();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
