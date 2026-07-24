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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * PERF-06 (S6) tests for the cross-query DERIVED partition-view cache ("cache A", the generic
 * {@link ConnectorPartitionViewCache}) wired into {@link HiveConnectorMetadata#listPartitions}. Hive does NOT
 * override {@code getMvccPartitionView} for a real hive handle (it returns the SPI default
 * {@code Optional.empty()} — fe-core's generic MTMV model already falls back to {@code listPartitions}), so —
 * like paimon's single typed field — there is exactly ONE enumeration hook to wrap.
 *
 * <p>Uses a {@link CountingHmsClient} double (no Mockito, no docker): the derivation seam
 * {@link HiveConnectorMetadata#listPartitions} wraps is {@code collectPartitionNames} -&gt;
 * {@code hmsClient.listPartitionNames}, so a cache HIT skips the whole loader (including the per-name
 * {@link HiveWriteUtils#toPartitionValues} parse + {@link ConnectorPartitionInfo} construction) and the call
 * count on {@code listPartitionNames} is the enumeration counter — the same seam
 * {@link HiveConnectorMetadataPartitionListTest} already asserts seam-touch on.
 *
 * <p>Hive is snapshot-less (its {@code beginQuerySnapshot} always pins {@code -1}) and its
 * {@link HiveTableHandle} carries no schema version, so cache A's key is always
 * {@code (db, table, -1, -1)} for a given table — unlike iceberg/paimon there is no "distinct snapshot"
 * dimension to re-key on; only the explicit invalidation hooks (+ TTL) can force a re-derive.
 */
public class HiveConnectorMetadataPartitionViewCacheTest {

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");
    private static final List<String> PARTITIONS = Arrays.asList(
            "year=2023/month=12",
            "year=2024/month=01");

    private static HiveConnectorMetadata metadataWithCache(CountingHmsClient client,
            ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache) {
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext(),
                () -> {
                    throw new UnsupportedOperationException();
                },
                () -> {
                    throw new UnsupportedOperationException();
                },
                handle -> {
                    throw new UnsupportedOperationException();
                },
                new HiveFileListingCache(Collections.emptyMap()), cache);
    }

    private static ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> partitionViewCache() {
        return new ConnectorPartitionViewCache<>("hive", Collections.emptyMap());
    }

    private static HiveTableHandle handle() {
        return new HiveTableHandle.Builder("db1", "t1", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    private static List<String> names(List<ConnectorPartitionInfo> infos) {
        return infos.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList());
    }

    @Test
    public void listPartitionsCachesDerivedListAcrossQueries() {
        // WHY: cache A must memoize the BUILT List<ConnectorPartitionInfo> keyed by (db, table, -1, -1), so a
        // repeated query on the same table skips the derived rebuild AND the hmsClient.listPartitionNames
        // round-trip. MUTATION: not consulting the cache (compute directly every call) -> the seam runs twice
        // -> red.
        CountingHmsClient client = new CountingHmsClient(PARTITIONS);
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        HiveConnectorMetadata md = metadataWithCache(client, cache);
        HiveTableHandle h = handle();

        List<ConnectorPartitionInfo> first = md.listPartitions(null, h, Optional.empty());
        List<ConnectorPartitionInfo> second = md.listPartitions(null, h, Optional.empty());

        Assertions.assertEquals(PARTITIONS, names(first));
        Assertions.assertEquals(names(first), names(second), "the cached list is returned verbatim");
        Assertions.assertEquals(1, client.listPartitionNamesCalls,
                "a cache hit must not re-enumerate (listPartitionNames once)");
    }

    @Test
    public void listPartitionsInvalidateTableForcesReEnumeration() {
        // WHY: REFRESH TABLE (HiveConnector.invalidateTable -> cache.invalidateTable) must drop the cached list
        // so the next query re-enumerates live. MUTATION: invalidateTable not wired -> the second call hits the
        // stale entry -> listPartitionNamesCalls stays 1 -> red.
        CountingHmsClient client = new CountingHmsClient(PARTITIONS);
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        HiveConnectorMetadata md = metadataWithCache(client, cache);
        HiveTableHandle h = handle();

        md.listPartitions(null, h, Optional.empty());
        cache.invalidateTable("db1", "t1");
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, client.listPartitionNamesCalls, "invalidateTable must force a re-enumeration");
    }

    @Test
    public void listPartitionsInvalidateAllForcesReEnumeration() {
        // WHY: REFRESH CATALOG (HiveConnector.invalidateAll -> cache.invalidateAll) must drop the cached list.
        // MUTATION: invalidateAll not wired -> the second call hits -> listPartitionNamesCalls stays 1 -> red.
        CountingHmsClient client = new CountingHmsClient(PARTITIONS);
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        HiveConnectorMetadata md = metadataWithCache(client, cache);
        HiveTableHandle h = handle();

        md.listPartitions(null, h, Optional.empty());
        cache.invalidateAll();
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, client.listPartitionNamesCalls, "invalidateAll must force a re-enumeration");
    }

    @Test
    public void listPartitionsWithFilterBypassesCache() {
        // WHY: a present filter must BYPASS the cache and compute directly -- and must NOT populate it either, so
        // a later empty-filter (pruning) call still misses. Hive already ignores the filter value entirely
        // (returns the full set regardless), but the cache-A key carries no filter dimension, so caching a
        // filtered call's result would be indistinguishable from caching an unfiltered one; bypassing keeps the
        // two paths independent, mirroring the iceberg/paimon pattern. MUTATION: caching regardless of filter ->
        // the second filtered call hits (count stays 1) -> red; or a bypassed call populating the cache -> the
        // following empty-filter call hits instead of missing -> red.
        CountingHmsClient client = new CountingHmsClient(PARTITIONS);
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        HiveConnectorMetadata md = metadataWithCache(client, cache);
        HiveTableHandle h = handle();

        ConnectorExpression filter = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("year", ConnectorType.of("STRING")),
                new ConnectorLiteral(ConnectorType.of("STRING"), "2024"));
        md.listPartitions(null, h, Optional.of(filter));
        md.listPartitions(null, h, Optional.of(filter));
        Assertions.assertEquals(2, client.listPartitionNamesCalls,
                "a present filter must bypass the cache (compute every call)");
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(3, client.listPartitionNamesCalls,
                "the empty-filter call must miss (filtered calls never populate)");
    }

    @Test
    public void listPartitionsNullCacheEnumeratesEveryCall() {
        // The convenience/test-ctor analogue: a null cache means compute directly every call -> the seam runs on
        // every query (no cross-query sharing). MUTATION: defaulting to a shared cache when null was intended ->
        // listPartitionNamesCalls stays 1 -> red.
        CountingHmsClient client = new CountingHmsClient(PARTITIONS);
        HiveConnectorMetadata md = metadataWithCache(client, null);
        HiveTableHandle h = handle();

        md.listPartitions(null, h, Optional.empty());
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, client.listPartitionNamesCalls, "a null (disabled) cache must re-enumerate every call");
    }

    /**
     * Minimal {@link HmsClient} double: {@code listPartitionNames} returns a fixed list and counts calls;
     * {@code getPartitions} fails loud (the per-partition round-trip {@link HiveConnectorMetadata#listPartitions}
     * must never make — mirrors {@link HiveConnectorMetadataPartitionListTest}'s FakeHmsClient).
     */
    private static final class CountingHmsClient implements HmsClient {
        private final List<String> partitionNames;
        int listPartitionNamesCalls;

        CountingHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalls++;
            return partitionNames;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            throw new AssertionError("get_partitions_by_names must not be called by partition listing");
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
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
