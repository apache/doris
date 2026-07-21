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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * PERF-06 tests for the cross-query DERIVED partition-view cache ("cache A", the generic
 * {@link ConnectorPartitionViewCache}) wired into {@link IcebergConnectorMetadata#getMvccPartitionView} /
 * {@link IcebergConnectorMetadata#listPartitions}. Uses the real {@link InMemoryCatalog} +
 * {@link RecordingIcebergCatalogOps} harness (no Mockito, no docker): the cache sits ABOVE the per-query build,
 * whose first step is {@code resolveTableForRead -> catalogOps.loadTable} (logged as {@code loadTable:db1.t1}), so
 * a cache HIT skips the whole loader and the {@code loadTable} count is the enumeration counter — the SAME proxy
 * the sibling cache tests use ({@code IcebergConnectorMetadataMvccTest.beginQuerySnapshot*Cache*}). The raw
 * partition cache (PERF-02) is passed null throughout, so the {@code loadTable} count isolates cache A's effect.
 * The partition math/merge parity itself is covered by {@link IcebergPartitionUtilsTest}.
 */
public class IcebergConnectorMetadataPartitionViewCacheTest {

    private static final Schema PARTITIONED_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));

    /** A real db1.t1 partitioned by day(ts) carrying TWO snapshots (day=100, then +day=101). */
    private static final class TwoSnap {
        Table table;
        long s1;
        long s2;
        long schemaId;
    }

    private static TwoSnap twoSnapshotTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA).day("ts").build();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), PARTITIONED_SCHEMA, spec);
        table.newAppend().appendFile(dayFile(spec, "s3://b/db1/t1/f1.parquet", "ts_day=1970-04-11")).commit();
        TwoSnap f = new TwoSnap();
        f.s1 = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(dayFile(spec, "s3://b/db1/t1/f2.parquet", "ts_day=1970-04-12")).commit();
        f.s2 = table.currentSnapshot().snapshotId();
        f.schemaId = table.schema().schemaId();
        f.table = catalog.loadTable(TableIdentifier.of("db1", "t1"));
        Assertions.assertNotEquals(f.s1, f.s2, "two appends must create two distinct snapshots");
        return f;
    }

    private static org.apache.iceberg.DataFile dayFile(PartitionSpec spec, String path, String partitionPath) {
        return DataFiles.builder(spec)
                .withPath(path).withFileSizeInBytes(100).withRecordCount(1)
                .withPartitionPath(partitionPath).withFormat(FileFormat.PARQUET).build();
    }

    private static IcebergConnectorMetadata metadataWithMvccCache(RecordingIcebergCatalogOps ops,
            ConnectorPartitionViewCache<ConnectorMvccPartitionView> cache) {
        // 9-arg ctor: disabled latest-snapshot cache + null table/partition/comment caches so the loadTable count
        // reflects cache A alone; only the mvcc view cache under test is injected.
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext(),
                new IcebergLatestSnapshotCache(0L, 1), null, null, null, cache, null);
    }

    private static IcebergConnectorMetadata metadataWithListCache(RecordingIcebergCatalogOps ops,
            ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache) {
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext(),
                new IcebergLatestSnapshotCache(0L, 1), null, null, null, null, cache);
    }

    private static IcebergTableHandle handle() {
        return new IcebergTableHandle("db1", "t1");
    }

    private static long loadCount(RecordingIcebergCatalogOps ops) {
        return ops.log.stream().filter(s -> s.equals("loadTable:db1.t1")).count();
    }

    private static ConnectorPartitionViewCache<ConnectorMvccPartitionView> mvccCache() {
        return new ConnectorPartitionViewCache<>("iceberg", Collections.emptyMap());
    }

    private static ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> listCache() {
        return new ConnectorPartitionViewCache<>("iceberg", Collections.emptyMap());
    }

    private static List<String> mvccNames(Optional<ConnectorMvccPartitionView> view) {
        return view.get().getPartitions().stream()
                .map(ConnectorMvccPartition::getName).collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------
    // getMvccPartitionView
    // ---------------------------------------------------------------------

    @Test
    public void getMvccPartitionViewCachesDerivedViewAcrossQueries() {
        // WHY: cache A must memoize the BUILT MVCC view keyed by (db, table, snapshotId, schemaId), so a repeated
        // query on the same pin skips the derived rebuild AND the underlying loadTable/scan. MUTATION: not
        // consulting the cache (compute directly every call) -> loadTable runs twice -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        ConnectorPartitionViewCache<ConnectorMvccPartitionView> cache = mvccCache();
        IcebergConnectorMetadata md = metadataWithMvccCache(ops, cache);

        Optional<ConnectorMvccPartitionView> first = md.getMvccPartitionView(null, handle());
        Optional<ConnectorMvccPartitionView> second = md.getMvccPartitionView(null, handle());

        Assertions.assertEquals(java.util.Arrays.asList("ts_day=100", "ts_day=101"), mvccNames(first));
        Assertions.assertEquals(mvccNames(first), mvccNames(second), "the cached view is returned verbatim");
        Assertions.assertEquals(1, loadCount(ops), "a cache hit must not re-enumerate (loadTable once)");
    }

    @Test
    public void getMvccPartitionViewDifferentSnapshotReEnumerates() {
        // WHY: pinning a different snapshot id yields a different cache key (time-travel must not serve another
        // snapshot's view), so each pin re-enumerates. MUTATION: keying on (db,table) only -> the S1 view would be
        // served for the S2 pin -> names/loadCount below red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        IcebergConnectorMetadata md = metadataWithMvccCache(ops, mvccCache());

        IcebergTableHandle atS1 = handle().withSnapshot(f.s1, null, f.schemaId);
        IcebergTableHandle atS2 = handle().withSnapshot(f.s2, null, f.schemaId);
        List<String> namesS1 = mvccNames(md.getMvccPartitionView(null, atS1));
        List<String> namesS2 = mvccNames(md.getMvccPartitionView(null, atS2));

        Assertions.assertEquals(Collections.singletonList("ts_day=100"), namesS1);
        Assertions.assertEquals(java.util.Arrays.asList("ts_day=100", "ts_day=101"), namesS2);
        Assertions.assertEquals(2, loadCount(ops), "distinct snapshot keys must each enumerate");
    }

    @Test
    public void getMvccPartitionViewInvalidateTableForcesReEnumeration() {
        // WHY: REFRESH TABLE (Connector.invalidateTable -> cache.invalidateTable) must drop the cached view so the
        // next query re-enumerates live. MUTATION: invalidateTable not wired -> the second call hits the stale
        // entry -> loadCount stays 1 -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        ConnectorPartitionViewCache<ConnectorMvccPartitionView> cache = mvccCache();
        IcebergConnectorMetadata md = metadataWithMvccCache(ops, cache);

        md.getMvccPartitionView(null, handle());
        cache.invalidateTable("db1", "t1");
        md.getMvccPartitionView(null, handle());
        Assertions.assertEquals(2, loadCount(ops), "invalidateTable must force a re-enumeration");
    }

    @Test
    public void getMvccPartitionViewNullCacheEnumeratesEveryCall() {
        // The session=user analogue at the metadata level: a null cache (IcebergConnector passes null under
        // iceberg.rest.session=user) means compute directly every call -> loadTable on every query (no cross-query
        // sharing). MUTATION: defaulting to a shared cache when null was intended -> loadCount 1 -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        IcebergConnectorMetadata md = metadataWithMvccCache(ops, null);
        md.getMvccPartitionView(null, handle());
        md.getMvccPartitionView(null, handle());
        Assertions.assertEquals(2, loadCount(ops), "a null (disabled) cache must re-enumerate every call");
    }

    // ---------------------------------------------------------------------
    // listPartitions
    // ---------------------------------------------------------------------

    @Test
    public void listPartitionsCachesDerivedListAcrossQueries() {
        // WHY: the empty-filter pruning path must memoize the built partition-info list. MUTATION: not consulting
        // the cache -> loadTable twice -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = listCache();
        IcebergConnectorMetadata md = metadataWithListCache(ops, cache);

        List<ConnectorPartitionInfo> first = md.listPartitions(null, handle(), Optional.empty());
        List<ConnectorPartitionInfo> second = md.listPartitions(null, handle(), Optional.empty());

        List<String> names = first.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList());
        Assertions.assertEquals(java.util.Arrays.asList("ts_day=100", "ts_day=101"), names);
        Assertions.assertEquals(names,
                second.stream().map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList()));
        Assertions.assertEquals(1, loadCount(ops), "a cache hit must not re-enumerate (loadTable once)");
    }

    @Test
    public void listPartitionsWithFilterBypassesCache() {
        // WHY: only the empty-filter pruning path is cached; a non-empty filter must BYPASS the cache and compute
        // directly (the pruning path always passes Optional.empty()). MUTATION: caching regardless of filter ->
        // the second filtered call hits -> loadCount 1 + cache.size 1 -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = listCache();
        IcebergConnectorMetadata md = metadataWithListCache(ops, cache);

        ConnectorExpression filter = Collections::emptyList; // any non-empty filter
        md.listPartitions(null, handle(), Optional.of(filter));
        md.listPartitions(null, handle(), Optional.of(filter));
        Assertions.assertEquals(2, loadCount(ops), "a present filter must bypass the cache (compute every call)");
        // A bypassed call must also NOT populate the cache: a following empty-filter (pruning) call must MISS
        // (loadCount 3), not be served from a filtered-populated entry.
        md.listPartitions(null, handle(), Optional.empty());
        Assertions.assertEquals(3, loadCount(ops), "the empty-filter call must miss (filtered calls never populate)");
    }

    @Test
    public void listPartitionsInvalidateAllForcesReEnumeration() {
        // WHY: REFRESH CATALOG (Connector.invalidateAll -> cache.invalidateAll) must drop the cached list.
        // MUTATION: invalidateAll not wired -> the second call hits -> loadCount stays 1 -> red.
        TwoSnap f = twoSnapshotTable();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = f.table;
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = listCache();
        IcebergConnectorMetadata md = metadataWithListCache(ops, cache);

        md.listPartitions(null, handle(), Optional.empty());
        cache.invalidateAll();
        md.listPartitions(null, handle(), Optional.empty());
        Assertions.assertEquals(2, loadCount(ops), "invalidateAll must force a re-enumeration");
    }
}
