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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class PluginDrivenPartitionViewTest {
    private final CatalogMetaCache owner = CatalogMetaCache.unmanaged();
    private final MetaCache<PluginDrivenPartitionView.Key, PluginDrivenPartitionView> cache = owner.create(
            MetaCacheDefinition.<PluginDrivenPartitionView.Key, PluginDrivenPartitionView>builder(
                    "partition_view", CacheSpec.of(true, 600, 100), key -> ScopePath.table("db", "tbl")).build());

    private PluginDrivenMvccExternalTable table() {
        return new PluginDrivenMvccExternalTable() {
            @Override
            PluginDrivenPartitionView cachedPartitionView(PluginDrivenPartitionView.Key key,
                    Function<PluginDrivenPartitionView.Key, PluginDrivenPartitionView> loader) {
                return cache.get(key, loader);
            }
        };
    }

    @AfterEach
    void close() {
        owner.close();
    }

    private ConnectorPartitionInfo partition(String value, long modified, boolean isNull,
            List<ConnectorType> types) {
        return new ConnectorPartitionInfo("p=" + value, Collections.singletonMap("p", value),
                Collections.emptyMap(), -1, -1, modified, -1,
                Collections.singletonList(value), Collections.singletonList(isNull), types);
    }

    private ConnectorPartitionInfo partition(String value, long modified) {
        return partition(value, modified, false, Collections.emptyList());
    }

    @Test
    void mixedSpecDisplayPartitionsDoNotBuildAnOrderedIndex() {
        ConnectorPartitionInfo one = new ConnectorPartitionInfo("a=1", Collections.emptyMap(),
                Collections.emptyMap(), -1, -1, 1, -1, List.of("1"), List.of(false),
                List.of(ConnectorType.of("INT")));
        ConnectorPartitionInfo two = new ConnectorPartitionInfo("a=1/b=1", Collections.emptyMap(),
                Collections.emptyMap(), -1, -1, 1, -1, List.of("1", "1"), List.of(false, false),
                List.of(ConnectorType.of("INT"), ConnectorType.of("INT")));
        PluginDrivenPartitionView view = table().partitionView(List.of(one, two), List.of(Type.INT, Type.INT));
        Assertions.assertEquals(2, view.getItems().size());
        Assertions.assertEquals(2, view.getFreshness().size());
        Assertions.assertTrue(view.getSortedRanges().isEmpty());
        Assertions.assertTrue(table().partitionView(List.of(one), List.of(Type.INT)).getSortedRanges().isPresent());
    }

    @Test
    void disabledBinarySearchDoesNotBuildOrReuseAnOrderedIndex() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            PluginDrivenMvccExternalTable table = table();
            List<ConnectorPartitionInfo> parts = List.of(partition("1", 10));
            context.getSessionVariable().enableBinarySearchFilteringPartitions = false;
            PluginDrivenPartitionView withoutIndex = table.partitionView(parts, List.of(Type.INT));
            Assertions.assertTrue(withoutIndex.getSortedRanges().isEmpty());
            Assertions.assertSame(withoutIndex, table.partitionView(parts, List.of(Type.INT)));

            context.getSessionVariable().enableBinarySearchFilteringPartitions = true;
            PluginDrivenPartitionView withIndex = table.partitionView(parts, List.of(Type.INT));
            Assertions.assertNotSame(withoutIndex, withIndex);
            Assertions.assertTrue(withIndex.getSortedRanges().isPresent());
            Assertions.assertSame(withIndex, table.partitionView(parts, List.of(Type.INT)));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void rangeViewAccountingDoesNotTraverseExpressionSuppliers() {
        PartitionKey lower = new PartitionKey();
        lower.pushColumn(new IntLiteral(1), PrimitiveType.BIGINT);
        PartitionKey upper = new PartitionKey();
        upper.pushColumn(new IntLiteral(2), PrimitiveType.BIGINT);
        PluginDrivenPartitionView view = new PluginDrivenPartitionView(
                Map.of("p", new RangePartitionItem(Range.closedOpen(lower, upper))),
                Map.of("p", 1L));
        Assertions.assertTrue(view.getSortedRanges().isPresent());
        Assertions.assertTrue(PluginDrivenPartitionView.estimateSize(
                PluginDrivenPartitionView.Key.versioned(1, "db", "tbl", "v1", List.of(Type.BIGINT), true),
                view).isComplete());
    }

    @Test
    void weightedCacheAdmitsViewsBeyondTheReflectiveTraversalBudget() {
        List<ConnectorPartitionInfo> parts = new ArrayList<>();
        for (int i = 0; i < 12000; i++) {
            parts.add(partition(Integer.toString(i), i));
        }
        PluginDrivenPartitionView view = table().partitionView(parts, List.of(Type.INT));
        PluginDrivenPartitionView.Key key = PluginDrivenPartitionView.Key.list(1, "db", "tbl", parts,
                List.of(Type.INT));
        org.apache.doris.connector.cache.MetaCacheSizeEstimate estimate =
                PluginDrivenPartitionView.estimateSize(key, view);
        Assertions.assertTrue(estimate.isComplete());
        Assertions.assertTrue(estimate.getBytes() > 12000 * 64L);
        MetaCache<PluginDrivenPartitionView.Key, PluginDrivenPartitionView> weighted = owner.create(
                MetaCacheDefinition.<PluginDrivenPartitionView.Key, PluginDrivenPartitionView>builder(
                        "weighted_partition_view", CacheSpec.ofWeight(true, 600, 100, estimate.getBytes() * 2),
                        k -> ScopePath.table("db", "tbl"))
                        .sizeEstimator(PluginDrivenPartitionView::estimateSize).build());
        Assertions.assertSame(view, weighted.get(key, k -> view));
        Assertions.assertSame(view, weighted.get(key, k -> {
            throw new AssertionError("large partition view must remain cached");
        }));
        owner.invalidateTable("db", "tbl");
        Assertions.assertNull(weighted.getIfPresent(key));
    }

    @Test
    void versionedKeysSeparateGenerationsAndConversionParameters() {
        PluginDrivenPartitionView.Key first = PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-1", List.of(Type.INT), false);
        Assertions.assertEquals(first, PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-1", List.of(Type.INT), false));
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-2", List.of(Type.INT), false));
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-1", List.of(Type.BIGINT), false));
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-1", List.of(Type.INT), true));
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.versioned(
                2, "db", "tbl", "snapshot-1", List.of(Type.INT), false));
        PluginDrivenPartitionView a = cache.get(first,
                key -> new PluginDrivenPartitionView(Collections.emptyMap(), Collections.emptyMap()));
        Assertions.assertSame(a, cache.get(PluginDrivenPartitionView.Key.versioned(
                1, "db", "tbl", "snapshot-1", List.of(Type.INT), false), key -> {
                    throw new AssertionError("same version must not load partitions");
                }));
        owner.invalidateTable("db", "tbl");
        Assertions.assertNotSame(a, cache.get(first,
                key -> new PluginDrivenPartitionView(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    void reuseRecordsButNotChangedListing() {
        PluginDrivenMvccExternalTable table = table();
        List<ConnectorPartitionInfo> parts = new ArrayList<>();
        parts.add(partition("1", 10));
        PluginDrivenPartitionView first = table.partitionView(parts, List.of(Type.INT));
        Assertions.assertSame(first, table.partitionView(new ArrayList<>(parts), List.of(Type.INT)));
        parts.add(partition("2", 20));
        PluginDrivenPartitionView added = table.partitionView(parts, List.of(Type.INT));
        Assertions.assertEquals(1, first.getItems().size());
        Assertions.assertEquals(2, added.getItems().size());
        parts.remove(0);
        PluginDrivenPartitionView removed = table.partitionView(parts, List.of(Type.INT));
        Assertions.assertEquals(Collections.singleton("p=2"), removed.getItems().keySet());
        // An uncached connector can return new objects for unchanged conversion inputs.
        parts.set(0, partition("2", 20));
        Assertions.assertSame(removed, table.partitionView(parts, List.of(Type.INT)));
    }

    @Test
    void schemaFreshnessNullAndTransformChangesMiss() {
        PluginDrivenMvccExternalTable table = table();
        List<ConnectorPartitionInfo> parts = List.of(partition("1", 10));
        PluginDrivenPartitionView integer = table.partitionView(parts, List.of(Type.INT));
        PluginDrivenPartitionView string = table.partitionView(parts, List.of(Type.STRING));
        Assertions.assertNotSame(integer, string);
        Assertions.assertEquals(Type.STRING, ((ListPartitionItem) string.getItems().get("p=1"))
                .getItems().get(0).getKeys().get(0).getType());
        PluginDrivenPartitionView modified = table.partitionView(List.of(partition("1", 30)), List.of(Type.INT));
        Assertions.assertEquals(30L, modified.getFreshness().get("p=1"));
        Assertions.assertEquals(10L, integer.getFreshness().get("p=1"));
        PluginDrivenPartitionView nullValue = table.partitionView(
                List.of(partition("1", 30, true, Collections.emptyList())), List.of(Type.INT));
        Assertions.assertTrue(((ListPartitionItem) nullValue.getItems().get("p=1"))
                .getItems().get(0).getKeys().get(0).isNullLiteral());
        PluginDrivenPartitionView transformed = table.partitionView(
                List.of(partition("1", 30, false, List.of(ConnectorType.of("INT")))), List.of(Type.STRING));
        Assertions.assertEquals(Type.INT, ((ListPartitionItem) transformed.getItems().get("p=1"))
                .getItems().get(0).getKeys().get(0).getType());
    }

    @Test
    void timestampWithoutZoneDoesNotReuseAnotherSessionsInterpretation() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            PluginDrivenMvccExternalTable table = table();
            String value = "2024-01-01 08:00:00";
            List<ConnectorPartitionInfo> parts = List.of(partition(value, 10));
            List<Type> types = List.of(ScalarType.createTimeStampTzType(6));
            Assertions.assertEquals(types, PluginDrivenPartitionView.Key.list(
                    1, "db", "tbl", parts, types).frozenTypes());
            context.getSessionVariable().setTimeZone("UTC");
            PluginDrivenPartitionView utc = table.partitionView(parts, types);
            context.getSessionVariable().setTimeZone("Asia/Shanghai");
            PluginDrivenPartitionView shanghai = table.partitionView(parts, types);
            Assertions.assertNotSame(utc, shanghai);
            Assertions.assertNotEquals(utc.getItems().get("p=" + value), shanghai.getItems().get("p=" + value));
            Assertions.assertSame(shanghai, table.partitionView(parts, types));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void mutableSchemaTypeCannotChangeCachedSignature() {
        PluginDrivenMvccExternalTable table = table();
        List<ConnectorPartitionInfo> parts = List.of(partition("a", 10));
        ScalarType type = ScalarType.createVarcharType(10);
        PluginDrivenPartitionView first = table.partitionView(parts, List.of(type));
        List<Type> binary = List.of(ScalarType.createVarbinaryType(23));
        Assertions.assertEquals(binary, PluginDrivenPartitionView.Key.list(
                1, "db", "tbl", parts, binary).frozenTypes());
        type.setLength(20);
        Assertions.assertNotSame(first, table.partitionView(parts, List.of(type)));
    }

    @Test
    void refreshAndHistoricalPinsRemainIndependent() {
        PluginDrivenMvccExternalTable table = table();
        List<ConnectorPartitionInfo> oldParts = List.of(partition("1", 10));
        PluginDrivenPartitionView old = table.partitionView(oldParts, List.of(Type.INT));
        PluginDrivenMvccSnapshot oldPin = new PluginDrivenMvccSnapshot(
                ConnectorMvccSnapshot.builder().snapshotId(1).build(), old, null);
        owner.invalidateTable("db", "tbl");
        Assertions.assertNotSame(old, table.partitionView(oldParts, List.of(Type.INT)));
        PluginDrivenPartitionView latest = table.partitionView(List.of(partition("2", 20)), List.of(Type.INT));
        PluginDrivenMvccSnapshot latestPin = new PluginDrivenMvccSnapshot(
                ConnectorMvccSnapshot.builder().snapshotId(2).build(), latest, null);
        Assertions.assertEquals(Collections.singleton("p=1"), oldPin.getNameToPartitionItem().keySet());
        Assertions.assertEquals(Collections.singleton("p=2"), latestPin.getNameToPartitionItem().keySet());
        Assertions.assertEquals(old.getItems(), table.partitionView(oldParts, List.of(Type.INT)).getItems());
        Assertions.assertSame(old.getSortedRanges().get(), oldPin.getPartitionView().getSortedRanges().get());
        Assertions.assertSame(old.getItems().get("p=1"), old.getSortedRanges().get().sortedPartitions.get(0).partitionItem);
        Assertions.assertSame(old.getItems(), oldPin.getNameToPartitionItem());
        Assertions.assertSame(old.getFreshness(), oldPin.getNameToLastModifiedMillis());
        SelectedPartitions selected = new SelectedPartitions(1, oldPin.getNameToPartitionItem(), false);
        Assertions.assertSame(old.getItems(), selected.selectedPartitions);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> oldPin.getNameToPartitionItem().clear());
    }

    @Test
    void concurrentGenerationsNeverReturnAnotherQueriesPartitions() {
        PluginDrivenMvccExternalTable table = table();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            String value = Integer.toString(i);
            futures.add(CompletableFuture.runAsync(() -> {
                List<ConnectorPartitionInfo> parts = List.of(partition(value, 10));
                for (int j = 0; j < 100; j++) {
                    PluginDrivenPartitionView view = table.partitionView(parts, List.of(Type.INT));
                    Assertions.assertEquals(Collections.singleton("p=" + value), view.getItems().keySet());
                    if (j % 10 == 0) {
                        owner.invalidateTable("db", "tbl");
                    }
                }
            }));
        }
        futures.forEach(CompletableFuture::join);
    }

    @Test
    void refreshDuringLoadCannotPublishOldView() throws Exception {
        PluginDrivenMvccExternalTable table = table();
        List<ConnectorPartitionInfo> parts = List.of(partition("1", 10));
        PluginDrivenPartitionView old = table.partitionView(parts, List.of(Type.INT));
        PluginDrivenPartitionView.Key key = PluginDrivenPartitionView.Key.list(0, null, null, parts, List.of(Type.INT));
        owner.invalidateTable("db", "tbl");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<PluginDrivenPartitionView> loading = CompletableFuture.supplyAsync(() -> cache.get(key, k -> {
            entered.countDown();
            try {
                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return old;
        }));
        try {
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            owner.invalidateTable("db", "tbl");
        } finally {
            release.countDown();
        }
        Assertions.assertSame(old, loading.get(10, TimeUnit.SECONDS));
        Assertions.assertNotSame(old, cache.get(key, k -> new PluginDrivenPartitionView(
                old.getItems(), old.getFreshness())));
        Assertions.assertEquals("p=1", old.getSortedRanges().get().sortedPartitions.get(0).id);
    }

    @Test
    void disabledCacheStillPublishesConsistentMapAndIndex() {
        MetaCache<PluginDrivenPartitionView.Key, PluginDrivenPartitionView> disabled = owner.create(
                MetaCacheDefinition.<PluginDrivenPartitionView.Key, PluginDrivenPartitionView>builder(
                        "disabled", CacheSpec.of(false, 600, 100), key -> ScopePath.table("db", "tbl")).build());
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable() {
            @Override
            PluginDrivenPartitionView cachedPartitionView(PluginDrivenPartitionView.Key key,
                    Function<PluginDrivenPartitionView.Key, PluginDrivenPartitionView> loader) {
                return disabled.get(key, loader);
            }
        };
        List<ConnectorPartitionInfo> parts = List.of(partition("1", 10));
        PluginDrivenPartitionView first = table.partitionView(parts, List.of(Type.INT));
        PluginDrivenPartitionView second = table.partitionView(parts, List.of(Type.INT));
        Assertions.assertNotSame(first, second);
        Assertions.assertSame(second.getItems().get("p=1"), second.getSortedRanges().get().sortedPartitions.get(0).partitionItem);
    }

    @Test
    void rangeKeysFreezeBoundsAndIncludeSchemaAndTableIdentity() {
        List<String> lower = new ArrayList<>(List.of("1"));
        ConnectorMvccPartition partition = new ConnectorMvccPartition("p", lower, List.of("2"), 10);
        PluginDrivenPartitionView.Key first = PluginDrivenPartitionView.Key.range(
                1, "db", "tbl", List.of(partition), List.of(Type.INT));
        Assertions.assertEquals(first, PluginDrivenPartitionView.Key.range(
                1, "db", "tbl", List.of(partition), List.of(Type.INT)));
        lower.set(0, "0");
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.range(
                1, "db", "tbl", List.of(partition), List.of(Type.INT)));
        Assertions.assertEquals(List.of("1"), first.rangePartitions().get(0).getLowerBound());
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.range(
                2, "db", "tbl", first.rangePartitions(), List.of(Type.INT)));
        Assertions.assertNotEquals(first, PluginDrivenPartitionView.Key.range(
                1, "db", "tbl", first.rangePartitions(), List.of(Type.STRING)));
    }

    @Test
    void realManagerInvalidatesTheMapAndIndexTogetherAtEveryScope() {
        long catalogId = 987654321;
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("partition_view_test");
        Mockito.when(catalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.doReturn(catalog).when(catalogs).getCatalog(catalogId);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(new NereidsSortedPartitionsCacheManager());
        ExternalMetaCacheMgr manager = new ExternalMetaCacheMgr(true);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable(
                    1L, "tbl", "tbl", catalog, database);
            List<ConnectorPartitionInfo> parts = List.of(partition("1", 10));
            PluginDrivenPartitionView first = table.partitionView(parts, List.of(Type.INT));
            Assertions.assertSame(first, table.partitionView(parts, List.of(Type.INT)));
            manager.invalidateTable(catalogId, 1L, "db", 1L, "tbl");
            PluginDrivenPartitionView second = table.partitionView(parts, List.of(Type.INT));
            Assertions.assertNotSame(first, second);
            Assertions.assertNotSame(first.getSortedRanges().get(), second.getSortedRanges().get());
            manager.invalidateDb(catalogId, 1L, "db");
            PluginDrivenPartitionView third = table.partitionView(parts, List.of(Type.INT));
            Assertions.assertNotSame(second, third);
            manager.invalidateCatalog(catalogId);
            Assertions.assertNotSame(third, table.partitionView(parts, List.of(Type.INT)));
            Assertions.assertEquals("p=1", first.getSortedRanges().get().sortedPartitions.get(0).id);
            manager.removeCatalog(catalogId);
        } finally {
            manager.commonRefreshExecutor().shutdownNow();
            manager.getFileListingExecutor().shutdownNow();
            manager.getScheduleExecutor().shutdownNow();
        }
    }

}
