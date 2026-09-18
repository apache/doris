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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheGovernance;
import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Locks the HMS-caching layer added to the hudi connector (the least-cached SPI connector), which mirrors
 * {@code HiveConnector} so repeated queries against the same hudi-on-HMS table stop re-hitting the metastore.
 * Two behaviors carry correctness weight and are pinned here (Rule 9):
 * <ul>
 *   <li><b>Fresh vs cached split.</b> {@code SHOW PARTITIONS} ({@code listPartitionNames}) MUST list FRESH
 *       (bypass the cache), or an externally hive-synced partition stays invisible until the 24h TTL — a
 *       freshness regression the raw pre-cache client never had. The query-pruning / MTMV path
 *       ({@code listPartitions}) MUST use the cache (that is the whole point of wrapping).
 *       <p>Note which path the {@code partition_values()} table function takes, because the earlier comment
 *       here had it wrong: the engine reaches the connector through {@code listPartitions}
 *       ({@code PluginDrivenExternalTable.getNameToPartitionValues}), i.e. the CACHED path, so its output can
 *       trail an external partition addition by one cache TTL. That is pre-existing behavior, not something
 *       this cache introduced, and deciding whether the table function should list fresh is a separate
 *       change — the mapping is recorded here so it does not get re-derived wrongly.</p></li>
 *   <li><b>REFRESH flush.</b> {@code invalidateTable}/{@code invalidateDb}/{@code invalidateAll} MUST flush the
 *       {@link CachingHmsClient}, or REFRESH cannot clear the sibling's own cache (the gateway forwards REFRESH
 *       to the sibling, so the flush must land on THIS connector's client).</li>
 * </ul>
 */
public class HudiConnectorHmsCacheTest {

    private static final List<String> YEAR_MONTH = Arrays.asList("year", "month");
    private static final List<String> ONE_PARTITION = Collections.singletonList("year=2024/month=01");
    private final List<HudiConnector> connectors = new ArrayList<>();

    @AfterEach
    void closeConnectors() throws Exception {
        for (HudiConnector connector : connectors) {
            connector.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"meta.cache.max-weight", "meta.cache.hive.partition_names.max-weight"})
    void weightLimitAccountsHmsCacheAndRejectsOversizedValues(String property) throws Exception {
        long catalogId = Long.MIN_VALUE + 71L;
        long previousGlobalWeight = MetaCacheGovernance.globalEstimatedWeight();
        Map<String, String> properties = new HashMap<>(HudiTestProperties.minimalMap());
        properties.put(property, "4KB");
        HudiConnector connector = connector(properties, catalogId);
        List<CatalogMetaCache> owners = MetaCacheGovernance.catalogCaches(catalogId);
        Assertions.assertEquals(1, owners.size());
        CatalogMetaCache owner = owners.get(0);
        Assertions.assertEquals("hudi", owner.engine());

        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        HmsClient cache = connector.wrapWithCache(delegate);
        Assertions.assertEquals(4, owner.entries().size());
        MetaCache<?, ?> entry = owner.entries().get("hive-partition-names");
        Assertions.assertTrue(entry.isWeightBounded());
        Assertions.assertEquals(4096L, entry.metrics().getMaxWeight());
        Assertions.assertEquals("meta.cache.max-weight".equals(property),
                owner.entries().get("hive-table").isWeightBounded());

        Assertions.assertEquals(ONE_PARTITION, cache.listPartitionNames("db", "t", -1));
        Assertions.assertEquals(ONE_PARTITION, cache.listPartitionNames("db", "t", -1));
        Assertions.assertEquals(1, delegate.cachedCalls);
        long retainedWeight = entry.metrics().getEstimatedWeight();
        Assertions.assertTrue(retainedWeight > 0L && retainedWeight <= 4096L);
        Assertions.assertEquals(previousGlobalWeight + retainedWeight,
                MetaCacheGovernance.globalEstimatedWeight());

        delegate.names = Collections.singletonList("partition=" + "x".repeat(8192));
        Assertions.assertEquals(delegate.names, cache.listPartitionNames("db", "large", -1));
        Assertions.assertEquals(delegate.names, cache.listPartitionNames("db", "large", -1));
        Assertions.assertEquals(3, delegate.cachedCalls, "oversized values must be returned but not cached");
        Assertions.assertEquals(2L, entry.metrics().getWeightRejectCount());
        Assertions.assertEquals("entry_too_large", entry.metrics().getLastWeightRejectReason());
        Assertions.assertEquals(retainedWeight, entry.metrics().getEstimatedWeight());

        connector.close();
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalogId).isEmpty());
        Assertions.assertEquals(0L, entry.metrics().getEstimatedWeight());
        Assertions.assertEquals(previousGlobalWeight, MetaCacheGovernance.globalEstimatedWeight());
    }

    @Test
    void noWeightLimitPreservesCountBasedCaching() {
        long catalogId = Long.MIN_VALUE + 72L;
        HudiConnector connector = connector(HudiTestProperties.minimalMap(), catalogId);
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        HmsClient cache = connector.wrapWithCache(delegate);
        CatalogMetaCache owner = MetaCacheGovernance.catalogCaches(catalogId).get(0);
        MetaCache<?, ?> entry = owner.entries().get("hive-partition-names");
        Assertions.assertFalse(entry.isWeightBounded());
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls);
        Assertions.assertEquals(0L, entry.metrics().getEstimatedWeight());
    }

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
        HudiConnector connector = connector();
        CachingHmsClient cache = (CachingHmsClient) connector.wrapWithCache(delegate);
        // Populate the (db,t) partition-name cache: two reads = ONE delegate hit (cached).
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls, "second read must be served from cache");

        // REFRESH TABLE -> the connector must flush this table from the cache (MUTATION: an empty override -> the
        // next read stays cached -> cachedCalls==1 -> red).
        connector.invalidateTable("db", "t");
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, delegate.cachedCalls, "after REFRESH TABLE the next read must miss the cache");
    }

    @Test
    public void invalidateDbFlushesCache() {
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        HudiConnector connector = connector();
        CachingHmsClient cache = (CachingHmsClient) connector.wrapWithCache(delegate);
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls);

        connector.invalidateDb("db");
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, delegate.cachedCalls, "after REFRESH DATABASE the next read must miss the cache");
    }

    @Test
    public void invalidateAllFlushesCache() {
        FakeHmsClient delegate = new FakeHmsClient(ONE_PARTITION);
        HudiConnector connector = connector();
        CachingHmsClient cache = (CachingHmsClient) connector.wrapWithCache(delegate);
        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(1, delegate.cachedCalls);

        connector.invalidateAll();
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

    private HudiConnector connector() {
        return connector(HudiTestProperties.minimalMap(), 1L);
    }

    private HudiConnector connector(Map<String, String> properties, long catalogId) {
        HudiConnector connector = new HudiConnector(properties, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return catalogId;
            }
        });
        connectors.add(connector);
        return connector;
    }

    private static HudiTableHandle partitioned() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE")
                .partitionKeyNames(YEAR_MONTH).build();
    }

    private static HudiConnectorMetadata hiveSyncMetadata(HmsClient hms) {
        // hive-sync so collectPartitions lists partition names from HMS (where the fresh/cached split lives);
        // the stub executor returns the canned instant latestInstant would read off the timeline.
        return new HudiConnectorMetadata(hms,
                HudiTestProperties.with(HudiCatalogProperties.USE_HIVE_SYNC_PARTITION, "true"), stub(7L));
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
        private List<String> names;

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
