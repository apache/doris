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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Locks the HMS-caching layer added to the hudi connector (the least-cached SPI connector), which mirrors
 * {@code HiveConnector} so repeated queries against the same hudi-on-HMS table stop re-hitting the metastore.
 * Two behaviors carry correctness weight and are pinned here (Rule 9):
 * <ul>
 *   <li><b>Fresh vs cached split.</b> The user-facing enumeration paths ({@code SHOW PARTITIONS} =
 *       {@code listPartitionNames}, {@code partition_values()} TVF = {@code listPartitionValues}) MUST list
 *       FRESH (bypass the cache), or an externally hive-synced partition stays invisible until the 24h TTL —
 *       a freshness regression the raw pre-cache client never had. The query-pruning / MTMV path
 *       ({@code listPartitions}) MUST use the cache (that is the whole point of wrapping).</li>
 *   <li><b>REFRESH flush.</b> {@code invalidateTable}/{@code invalidateDb}/{@code invalidateAll} MUST flush the
 *       {@link CachingHmsClient}, or REFRESH cannot clear the sibling's own cache (the gateway forwards REFRESH
 *       to the sibling, so the flush must land on THIS connector's client).</li>
 * </ul>
 */
public class HudiConnectorHmsCacheTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");
    private static final List<String> ONE_PARTITION = Collections.singletonList("year=2024/month=01");

    // ── wrap ─────────────────────────────────────────────────────────────────────────────────────────────

    @Test
    public void wrapWithCacheReturnsCachingHmsClient() {
        // MUTATION: returning the raw client (dropping the wrap) -> not a CachingHmsClient -> red. This is the
        // guard for HudiConnector.createClient wrapping the pooled ThriftHmsClient before handing it out.
        HmsClient wrapped = connector().wrapWithCache(new FakeHmsClient(ONE_PARTITION));
        Assertions.assertTrue(wrapped instanceof CachingHmsClient,
                "the hudi HMS client must be decorated with CachingHmsClient");
    }

    // ── fresh vs cached split (hive-sync partition source) ─────────────────────────────────────────────────

    @Test
    public void showPartitionsPathListsFresh() {
        // listPartitionNames backs SHOW PARTITIONS + the partitions() TVF -> must bypass the cache.
        // MUTATION: routing it through the cached listPartitionNames -> freshCalls==0 -> red.
        FakeHmsClient hms = new FakeHmsClient(ONE_PARTITION);
        HudiConnectorMetadata md = hiveSyncMetadata(hms);
        md.listPartitionNames(null, partitioned());
        Assertions.assertEquals(1, hms.freshCalls, "SHOW PARTITIONS must list FRESH (bypass cache)");
        Assertions.assertEquals(0, hms.cachedCalls, "SHOW PARTITIONS must NOT read the cached listing");
    }

    @Test
    public void partitionValuesTvfListsFresh() {
        // partition_values() TVF is user-facing enumeration too -> fresh, like listPartitionNames.
        FakeHmsClient hms = new FakeHmsClient(ONE_PARTITION);
        HudiConnectorMetadata md = hiveSyncMetadata(hms);
        md.listPartitionValues(null, partitioned(), YEAR_MONTH);
        Assertions.assertEquals(1, hms.freshCalls, "partition_values() must list FRESH (bypass cache)");
        Assertions.assertEquals(0, hms.cachedCalls, "partition_values() must NOT read the cached listing");
    }

    @Test
    public void queryPruningPathUsesCache() {
        // listPartitions backs query pruning / MTMV -> must use the cache (the wrap's intended win).
        // MUTATION: routing it through listPartitionNamesFresh -> cachedCalls==0 -> red.
        FakeHmsClient hms = new FakeHmsClient(ONE_PARTITION);
        HudiConnectorMetadata md = hiveSyncMetadata(hms);
        md.listPartitions(null, partitioned(), java.util.Optional.empty());
        Assertions.assertEquals(1, hms.cachedCalls, "query pruning must read the CACHED listing");
        Assertions.assertEquals(0, hms.freshCalls, "query pruning must NOT force a fresh listing");
    }

    // ── REFRESH flush wiring ───────────────────────────────────────────────────────────────────────────────

    @Test
    public void invalidateTableFlushesCache() {
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());
        // Populate the (db,t) partition-name cache: two reads = ONE delegate hit (cached).
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls, "second read must be served from cache");

        // REFRESH TABLE -> the connector must flush this table from the cache (MUTATION: an empty override -> the
        // next read stays cached -> cachedCalls==1 -> red).
        connector().invalidateTable(cache, "db", "t");
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, delegate.cachedCalls, "after REFRESH TABLE the next read must miss the cache");
    }

    @Test
    public void invalidateDbFlushesCache() {
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls);

        connector().invalidateDb(cache, "db");
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, delegate.cachedCalls, "after REFRESH DATABASE the next read must miss the cache");
    }

    @Test
    public void invalidateAllFlushesCache() {
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls);

        connector().invalidateAll(cache);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, delegate.cachedCalls, "after REFRESH CATALOG the next read must miss the cache");
    }

    @Test
    public void invalidateOnUnbuiltClientIsNoOp() {
        // REFRESH on a never-queried catalog must not force-build a client (nothing to flush). The public
        // overrides read the null hmsClient field; they must not throw.
        Assertions.assertDoesNotThrow(() -> {
            connector().invalidateTable("db", "t");
            connector().invalidateDb("db");
            connector().invalidateAll();
        });
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────────

    private static HudiConnector connector() {
        return new HudiConnector(Collections.emptyMap(), new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        });
    }

    private static HudiTableHandle partitioned() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(YEAR_MONTH).build();
    }

    private static HudiConnectorMetadata hiveSyncMetadata(HmsClient hms) {
        // hive-sync so collectPartitions lists partition names from HMS (where the fresh/cached split lives);
        // the stub executor returns the canned instant latestInstant would read off the timeline.
        return new HudiConnectorMetadata(hms,
                Collections.singletonMap("use_hive_sync_partition", "true"), stub(7L));
    }

    /** Executor that ignores the action and returns a canned value (stubs out the live metaClient). */
    private static HudiMetaClientExecutor stub(Object cannedReturn) {
        return new HudiMetaClientExecutor() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T execute(Callable<T> action) {
                return (T) cannedReturn;
            }
        };
    }

    /**
     * {@link HmsClient} double that counts the CACHED {@link #listPartitionNames} vs the FRESH
     * {@link #listPartitionNamesFresh} separately, so a test can pin which freshness contract each entry point
     * selects. Everything else fails loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        int cachedCalls;
        int freshCalls;
        private final List<String> names;

        FakeHmsClient(List<String> names) {
            this.names = names;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            cachedCalls++;
            return names;
        }

        @Override
        public List<String> listPartitionNamesFresh(String dbName, String tableName, int maxParts) {
            freshCalls++;
            return names;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
