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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.cache.ConnectorMetadataCache;

import org.apache.paimon.partition.Partition;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * PERF-06 tests for the cross-query DERIVED partition-view cache ("cache A", the generic
 * {@link ConnectorMetadataCache}) wired into all three partition-enumeration hooks
 * ({@link PaimonConnectorMetadata#listPartitions}, {@code listPartitionNames}, {@code listPartitionValues})
 * via the shared {@code cachedPartitions} collector. Paimon does NOT override {@code getMvccPartitionView}
 * (the generic MTMV model falls back to its default listPartitions/LIST/timestamp path), so — unlike
 * iceberg's two typed fields — there is a single typed cache field, now shared by all three hooks.
 *
 * <p>Uses the real {@link RecordingPaimonCatalogOps} + {@link FakePaimonTable} harness (no Mockito, no docker):
 * {@link PaimonConnectorMetadata#collectPartitions}'s first (and only) remote call is
 * {@code catalogOps.listPartitions(Identifier)}, logged as {@code "listPartitions:db1.t1"}, so a cache HIT skips
 * the whole loader and that log count is the enumeration counter — the SAME proxy
 * {@link PaimonConnectorMetadataPartitionTest} already relies on ({@code ops.log.contains("listPartitions:...")}).
 *
 * <p>Every test uses a DISABLED {@link PaimonLatestSnapshotCache} ({@code ttl-second <= 0}, always-live) so the
 * cache-A key's {@code snapshotId} component is driven directly and deterministically by
 * {@code ops.latestSnapshotId}, isolating cache A (the only caching layer under test) — mirroring how the
 * iceberg PERF-06 tests pass the raw partition cache as {@code null} throughout.
 */
public class PaimonConnectorMetadataPartitionViewCacheTest {

    private static PaimonConnectorMetadata metadataWithCache(RecordingPaimonCatalogOps ops,
            ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext(),
                new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE),
                new PaimonLatestSnapshotCache(0L, 1), cache);
    }

    private static ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCache() {
        return new ConnectorMetadataCache<>("paimon", "partition_view", Collections.emptyMap());
    }

    private static RowType regionRowType() {
        return RowType.builder()
                .field("id", DataTypes.INT())
                .field("region", DataTypes.STRING())
                .build();
    }

    private static PaimonTableHandle handle(FakePaimonTable table) {
        PaimonTableHandle h = new PaimonTableHandle(
                "db1", "t1", Collections.singletonList("region"), Collections.emptyList());
        h.setPaimonTable(table);
        return h;
    }

    private static FakePaimonTable regionTable() {
        FakePaimonTable table = new FakePaimonTable(
                "t1", regionRowType(), Collections.singletonList("region"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        return table;
    }

    private static Partition partition(String regionValue) {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("region", regionValue);
        return new Partition(spec, 1L, 1L, 1, 1L, true);
    }

    private static long loadCount(RecordingPaimonCatalogOps ops) {
        return ops.log.stream().filter(s -> s.equals("listPartitions:db1.t1")).count();
    }

    private static List<String> names(List<ConnectorPartitionInfo> infos) {
        return infos.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList());
    }

    @Test
    public void listPartitionsCachesDerivedListAcrossQueries() {
        // WHY: cache A must memoize the BUILT List<ConnectorPartitionInfo> keyed by (db, table, snapshotId,
        // schemaId), so a repeated query on the same (unchanged) latest snapshot skips the derived rebuild AND
        // the remote catalogOps.listPartitions round-trip. MUTATION: not consulting the cache (compute directly
        // every call) -> the seam runs twice -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Arrays.asList(partition("cn"), partition("us"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        List<ConnectorPartitionInfo> first = md.listPartitions(null, h, Optional.empty());
        List<ConnectorPartitionInfo> second = md.listPartitions(null, h, Optional.empty());

        Assertions.assertEquals(Arrays.asList("region=cn", "region=us"), names(first));
        Assertions.assertEquals(names(first), names(second), "the cached list is returned verbatim");
        Assertions.assertEquals(1, loadCount(ops), "a cache hit must not re-enumerate (listPartitions once)");
    }

    @Test
    public void listPartitionsDifferentSnapshotReEnumerates() {
        // WHY: the underlying remote enumeration always reflects the CURRENT catalog state (it is
        // base-identifier-only, never pinned to a historical snapshot -- see partitionViewCacheKey's javadoc),
        // so the key must track "current" via latestSnapshotCache: a data change (new latest snapshot id)
        // must mint a new key and re-enumerate. MUTATION: keying on (db,table) only, ignoring snapshotId ->
        // the stale S1 list would be served after the snapshot changed -> loadCount/contents below red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Collections.singletonList(partition("cn"));
        List<ConnectorPartitionInfo> atS1 = md.listPartitions(null, h, Optional.empty());

        ops.latestSnapshotId = OptionalLong.of(200L);
        ops.partitions = Arrays.asList(partition("cn"), partition("us"));
        List<ConnectorPartitionInfo> atS2 = md.listPartitions(null, h, Optional.empty());

        Assertions.assertEquals(Collections.singletonList("region=cn"), names(atS1));
        Assertions.assertEquals(Arrays.asList("region=cn", "region=us"), names(atS2));
        Assertions.assertEquals(2, loadCount(ops), "distinct snapshot keys must each enumerate");
    }

    @Test
    public void listPartitionsInvalidateTableForcesReEnumeration() {
        // WHY: REFRESH TABLE (PaimonConnector.invalidateTable -> cache.invalidateTable) must drop the cached
        // list so the next query re-enumerates live. MUTATION: invalidateTable not wired -> the second call
        // hits the stale entry -> loadCount stays 1 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Collections.singletonList(partition("cn"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        md.listPartitions(null, h, Optional.empty());
        cache.invalidateTable("db1", "t1");
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, loadCount(ops), "invalidateTable must force a re-enumeration");
    }

    @Test
    public void listPartitionsInvalidateAllForcesReEnumeration() {
        // WHY: REFRESH CATALOG (PaimonConnector.invalidateAll -> cache.invalidateAll) must drop the cached
        // list. MUTATION: invalidateAll not wired -> the second call hits -> loadCount stays 1 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Collections.singletonList(partition("cn"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        md.listPartitions(null, h, Optional.empty());
        cache.invalidateAll();
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, loadCount(ops), "invalidateAll must force a re-enumeration");
    }

    @Test
    public void listPartitionsWithFilterBypassesCache() {
        // WHY: a present filter must BYPASS the cache and compute directly -- and must NOT populate it either,
        // so a later empty-filter (pruning) call still misses. legacy ignores the filter value entirely
        // (returns the full set regardless), but the cache-A key carries no filter dimension, so caching a
        // filtered call's result would be indistinguishable from caching an unfiltered one; bypassing keeps the
        // two paths independent, mirroring the iceberg pattern. MUTATION: caching regardless of filter -> the
        // second filtered call hits (loadCount stays 1) -> red; or a bypassed call populating the cache -> the
        // following empty-filter call hits instead of missing -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Collections.singletonList(partition("cn"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        ConnectorExpression filter = Collections::emptyList; // any non-empty filter
        md.listPartitions(null, h, Optional.of(filter));
        md.listPartitions(null, h, Optional.of(filter));
        Assertions.assertEquals(2, loadCount(ops), "a present filter must bypass the cache (compute every call)");
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(3, loadCount(ops), "the empty-filter call must miss (filtered calls never populate)");
    }

    @Test
    public void listPartitionsNullCacheEnumeratesEveryCall() {
        // The convenience/test-ctor analogue: a null cache means compute directly every call -> the seam runs
        // on every query (no cross-query sharing). MUTATION: defaulting to a shared cache when null was
        // intended -> loadCount 1 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Collections.singletonList(partition("cn"));
        PaimonConnectorMetadata md = metadataWithCache(ops, null);
        PaimonTableHandle h = handle(table);

        md.listPartitions(null, h, Optional.empty());
        md.listPartitions(null, h, Optional.empty());
        Assertions.assertEquals(2, loadCount(ops), "a null (disabled) cache must re-enumerate every call");
    }

    @Test
    public void unpartitionedHandleBypassesCacheWithoutTouchingSnapshotSeam() {
        // WHY: an unpartitioned handle must short-circuit to empty WITHOUT touching either seam
        // (latestSnapshotId or listPartitions) -- mirrors collectPartitions' own pre-existing contract
        // (PaimonConnectorMetadataPartitionTest#nonPartitionedHandleReturnsEmptyWithoutSeamCall). Building a
        // cache-A key would otherwise call latestSnapshotCache -> catalogOps.latestSnapshotId for a table that
        // is guaranteed to return an empty list, polluting the cache and the seam log for nothing. MUTATION:
        // building the key before the emptiness check -> "latestSnapshotId" appears in ops.log -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", RowType.builder().field("id", DataTypes.INT()).build(),
                Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        PaimonTableHandle h = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        h.setPaimonTable(table);
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);

        List<ConnectorPartitionInfo> result = md.listPartitions(null, h, Optional.empty());

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(ops.log.isEmpty(), "an unpartitioned handle must not touch any remote seam");
    }

    @Test
    public void listPartitionNamesCachesAcrossQueries() {
        // WHY (PA-1): listPartitionNames now routes through the SAME partitionViewCache as listPartitions
        // (SHOW PARTITIONS re-rendered the full list on every call before). Two calls on the same latest
        // snapshot must share one entry: the seam runs once and both calls return identical names.
        // MUTATION: listPartitionNames calling collectPartitions directly (the pre-fix code) -> loadCount 2 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Arrays.asList(partition("cn"), partition("us"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        List<String> first = md.listPartitionNames(null, h);
        List<String> second = md.listPartitionNames(null, h);

        Assertions.assertEquals(Arrays.asList("region=cn", "region=us"), first);
        Assertions.assertEquals(first, second, "the cached list drives identical names");
        Assertions.assertEquals(1, loadCount(ops), "listPartitionNames must hit the shared cache (enumerate once)");
    }

    @Test
    public void listPartitionValuesCachesAcrossQueries() {
        // WHY (PA-1): same as above for the partition_values() TVF path -- it re-rendered on every call before.
        // MUTATION: listPartitionValues calling collectPartitions directly -> loadCount 2 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Arrays.asList(partition("cn"), partition("us"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        List<String> cols = Collections.singletonList("region");
        List<List<String>> first = md.listPartitionValues(null, h, cols);
        List<List<String>> second = md.listPartitionValues(null, h, cols);

        Assertions.assertEquals(
                Arrays.asList(Collections.singletonList("cn"), Collections.singletonList("us")), first);
        Assertions.assertEquals(first, second, "the cached list drives identical values");
        Assertions.assertEquals(1, loadCount(ops), "listPartitionValues must hit the shared cache (enumerate once)");
    }

    @Test
    public void allThreeHooksShareOneCacheEntry() {
        // WHY (PA-1): the three enumeration hooks share ONE (db,table,snapshotId) entry -- listPartitions
        // populates it, listPartitionNames and listPartitionValues then derive from the SAME cached list
        // without re-enumerating, and the derived outputs stay byte-consistent with listPartitions' rendered
        // list. MUTATION: any hook bypassing the shared cachedPartitions -> loadCount > 1 -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = regionTable();
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(100L);
        ops.partitions = Arrays.asList(partition("cn"), partition("us"));
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);
        PaimonTableHandle h = handle(table);

        List<ConnectorPartitionInfo> full = md.listPartitions(null, h, Optional.empty());
        List<String> namesOut = md.listPartitionNames(null, h);
        List<List<String>> valuesOut = md.listPartitionValues(null, h, Collections.singletonList("region"));

        Assertions.assertEquals(1, loadCount(ops), "all three hooks must share one cache entry (enumerate once)");
        Assertions.assertEquals(names(full), namesOut, "listPartitionNames equals the names of listPartitions' list");
        Assertions.assertEquals(
                full.stream().map(p -> Collections.singletonList(p.getPartitionValues().get("region")))
                        .collect(Collectors.toList()),
                valuesOut, "listPartitionValues equals the values derived from listPartitions' list");
    }

    @Test
    public void unpartitionedNamesAndValuesBypassCacheWithoutTouchingSnapshotSeam() {
        // WHY (PA-1): the "no seam call for unpartitioned" contract must hold for the new routing of
        // listPartitionNames and listPartitionValues too -- cachedPartitions short-circuits before building a
        // key, so neither latestSnapshotId nor listPartitions is called. MUTATION: routing through the cache
        // before the emptiness check -> "latestSnapshotId" appears in ops.log -> red.
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", RowType.builder().field("id", DataTypes.INT()).build(),
                Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        PaimonTableHandle h = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        h.setPaimonTable(table);
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = partitionViewCache();
        PaimonConnectorMetadata md = metadataWithCache(ops, cache);

        Assertions.assertTrue(md.listPartitionNames(null, h).isEmpty());
        Assertions.assertTrue(md.listPartitionValues(null, h, Collections.singletonList("region")).isEmpty());
        Assertions.assertTrue(ops.log.isEmpty(), "unpartitioned names/values must not touch any remote seam");
    }
}
