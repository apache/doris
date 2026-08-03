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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.DorisConnectorException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.table.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link MaxComputePartitionCache}: the connector-owned partition-listing cache (a structural copy of the
 * hive connector's {@code HiveFileListingCache}), backed by the shared {@code fe-connector-cache} framework.
 *
 * <p>WHY (Rule 9): after the max_compute cutover the fe-core engine-side external meta cache stops routing to a
 * MaxCompute catalog, so without this connector-owned cache every {@code SHOW PARTITIONS} / partition-pruning /
 * partition-values call would re-list every partition from ODPS. These tests pin the behaviours that make the
 * re-homed cache correct: (1) a partition listing is cached keyed by {@code (db, table)} so it loads once; the
 * db / table dimensions never collide; (2) {@code invalidateTable}/{@code invalidateDb}/{@code invalidateAll}
 * drop exactly the intended scope (arming REFRESH TABLE / DATABASE / CATALOG); (3) the per-entry
 * {@code meta.cache.max_compute.partition.*} knob turns it off; (4) a load failure propagates and is NOT cached.
 * Two integration tests prove the metadata's partition-listing methods are served from the cache, so repeated /
 * cross-method listings of the same table share ONE ODPS round trip.</p>
 */
public class MaxComputePartitionCacheTest {

    // ==================== caching: hit / miss keyed by (db, table) ====================

    @Test
    public void partitionsAreCachedPerTable() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        List<Partition> a = cache.getPartitions("db", "t");
        List<Partition> b = cache.getPartitions("db", "t");
        // WHY: a hit must serve the cached listing without re-listing ODPS.
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, lister.totalCalls);
    }

    @Test
    public void keyScopedByDbAndTable() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        // WHY: (db, table) must both be part of the key so invalidateTable can scope one table — and so two
        // tables that share a name across dbs never serve each other's listing.
        cache.getPartitions("db1", "t");
        cache.getPartitions("db2", "t");
        cache.getPartitions("db1", "t2");
        Assertions.assertEquals(3, lister.totalCalls);
    }

    // ==================== invalidation (arms REFRESH TABLE / DATABASE / CATALOG) ====================

    @Test
    public void invalidateTableDropsOnlyThatTable() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        cache.getPartitions("db", "t1");
        cache.getPartitions("db", "t2");
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateTable("db", "t1");

        // WHY: t1 must re-list after its invalidation...
        cache.getPartitions("db", "t1");
        Assertions.assertEquals(2, (int) lister.callsPerTable.get("db/t1"));
        // ...while t2's entry (a different table) must survive — invalidateTable is scoped by (db, table).
        cache.getPartitions("db", "t2");
        Assertions.assertEquals(1, (int) lister.callsPerTable.get("db/t2"));
    }

    @Test
    public void invalidateAllDropsEverything() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        cache.getPartitions("db", "t1");
        cache.getPartitions("db", "t2");
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateAll();

        // WHY: every entry must reload after invalidateAll (REFRESH CATALOG).
        cache.getPartitions("db", "t1");
        cache.getPartitions("db", "t2");
        Assertions.assertEquals(4, lister.totalCalls);
    }

    @Test
    public void invalidateDbDropsOneDb() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        cache.getPartitions("db1", "t");
        cache.getPartitions("db2", "t");
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateDb("db1");

        // WHY: only db1's entries reload (REFRESH DATABASE db1)...
        cache.getPartitions("db1", "t");
        Assertions.assertEquals(2, (int) lister.callsPerTable.get("db1/t"));
        // ...db2's entry survives.
        cache.getPartitions("db2", "t");
        Assertions.assertEquals(1, (int) lister.callsPerTable.get("db2/t"));
    }

    // ==================== per-entry property knob ====================

    @Test
    public void disablingViaPropsBypassesTheCache() {
        CountingPartitionLister lister = new CountingPartitionLister();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(
                Collections.singletonMap("meta.cache.max_compute.partition.enable", "false"), lister);

        cache.getPartitions("db", "t");
        cache.getPartitions("db", "t");
        // WHY: meta.cache.max_compute.partition.enable=false must bypass caching entirely — every call re-lists.
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== failures are propagated, never cached ====================

    @Test
    public void loadFailureIsNotCached() {
        CountingPartitionLister lister = new CountingPartitionLister();
        lister.error = new DorisConnectorException("boom");
        MaxComputePartitionCache cache = new MaxComputePartitionCache(Collections.emptyMap(), lister);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> cache.getPartitions("db", "t"));
        Assertions.assertEquals("boom", e.getMessage());
        Assertions.assertEquals(1, lister.totalCalls);
        // WHY (Rule 9): a failed load must leave NO cache entry, else a momentary ODPS blip would poison the
        // listing for the whole TTL (a "catch -> return emptyList" loader mutation would cache an empty listing).
        Assertions.assertEquals(0L, cache.size());

        // WHY: after recovery the next clean call re-lists and succeeds.
        lister.error = null;
        cache.getPartitions("db", "t");
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== integration: the metadata's partition methods are cache-backed ====================

    @Test
    public void twoListPartitionsShareOneRoundTrip() {
        CountingStructureHelper helper = new CountingStructureHelper();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(
                Collections.emptyMap(), (db, t) -> helper.getPartitions(null, db, t));
        MaxComputeConnectorMetadata md = new MaxComputeConnectorMetadata(
                null, helper, "proj", "ep", "quota", Collections.emptyMap(), cache);
        MaxComputeTableHandle handle = new MaxComputeTableHandle("db", "t", null, null);

        md.listPartitions(null, handle, Optional.empty());
        md.listPartitions(null, handle, Optional.empty());

        // WHY: the second listPartitions is served from the cache — one ODPS round trip, not two.
        Assertions.assertEquals(1, helper.getPartitionsCalls);
    }

    @Test
    public void crossMethodShareOneRoundTrip() {
        CountingStructureHelper helper = new CountingStructureHelper();
        MaxComputePartitionCache cache = new MaxComputePartitionCache(
                Collections.emptyMap(), (db, t) -> helper.getPartitions(null, db, t));
        MaxComputeConnectorMetadata md = new MaxComputeConnectorMetadata(
                null, helper, "proj", "ep", "quota", Collections.emptyMap(), cache);
        MaxComputeTableHandle handle = new MaxComputeTableHandle("db", "t", null, null);

        md.listPartitions(null, handle, Optional.empty());
        md.listPartitionNames(null, handle);

        // WHY: listPartitions and listPartitionNames of the SAME table share ONE cached listing (the §5th-cache
        // coupling): a partition read warms the other method too.
        Assertions.assertEquals(1, helper.getPartitionsCalls);
    }

    /**
     * A {@link MaxComputePartitionCache.PartitionLister} double: counts calls (total + per {@code db/table}) and
     * returns a FRESH empty list per call, so reference identity distinguishes a cache hit from a reload (a real
     * {@code Partition} needs a live ODPS client, which the connector test module has no Mockito to fake). Throws
     * {@link #error} when set, to exercise the failure-not-cached path.
     */
    private static final class CountingPartitionLister implements MaxComputePartitionCache.PartitionLister {
        final Map<String, Integer> callsPerTable = new HashMap<>();
        int totalCalls;
        RuntimeException error;

        @Override
        public List<Partition> list(String dbName, String tableName) {
            totalCalls++;
            callsPerTable.merge(dbName + "/" + tableName, 1, Integer::sum);
            if (error != null) {
                throw error;
            }
            return new ArrayList<>();
        }
    }

    /**
     * Recording {@link McStructureHelper}: its {@code getPartitions} returns an empty list and increments a
     * counter (= the SDK round trip), so an integration test can assert the metadata was served from the cache.
     * Every other method returns a harmless default (none is invoked on the partition-listing path).
     */
    private static final class CountingStructureHelper implements McStructureHelper {
        int getPartitionsCalls;

        @Override
        public List<Partition> getPartitions(Odps mcClient, String dbName, String tableName) {
            getPartitionsCalls++;
            return Collections.emptyList();
        }

        // ---- unused on the partition-listing path: harmless defaults ----

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public List<String> listDatabaseNames(Odps mcClient, String defaultProject) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(Odps mcClient, String dbName, String tableName) {
            return false;
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) {
            return false;
        }

        @Override
        public TableIdentifier getTableIdentifier(String dbName, String tableName) {
            return null;
        }

        @Override
        public Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName) {
            return Collections.emptyIterator();
        }

        @Override
        public Table getOdpsTable(Odps mcClient, String dbName, String tableName) {
            return null;
        }

        @Override
        public Tables.TableCreator createTableCreator(Odps mcClient, String dbName,
                String tableName, TableSchema schema) {
            return null;
        }

        @Override
        public void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists)
                throws OdpsException {
        }

        @Override
        public void createDb(Odps mcClient, String dbName, boolean ifNotExists) {
        }

        @Override
        public void dropDb(Odps mcClient, String dbName, boolean ifExists) {
        }
    }
}
