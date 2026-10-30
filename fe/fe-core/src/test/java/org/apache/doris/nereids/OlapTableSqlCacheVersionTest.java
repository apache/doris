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

package org.apache.doris.nereids;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class OlapTableSqlCacheVersionTest {
    private OlapTable table() {
        OlapTable table = PlanConstructor.newOlapTable(1, "sql_cache_partition_versions", 0);
        table.addPartition(partition(table, 10, 2));
        table.addPartition(partition(table, 20, 1));
        table.updateVisibleVersionAndTime(2, 100);
        return table;
    }

    private Partition partition(OlapTable table, long id, long version) {
        Partition partition = new Partition(id, "p" + id,
                new MaterializedIndex(-1, IndexState.NORMAL), table.getDefaultDistributionInfo());
        partition.updateVisibleVersionAndTime(version, 100);
        return partition;
    }

    private OlapTableSqlCacheVersion snapshot(OlapTable table) throws Exception {
        OlapTableSqlCacheVersion snapshot = new OlapTableSqlCacheVersion(table, table.getVisibleVersion());
        snapshot.addScan(table, ImmutableMap.of(10L, 2L));
        Assertions.assertTrue(snapshot.isUsable());
        return snapshot;
    }

    @Test
    void testUnrelatedPartitionWritePreservesKeyAndCache() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        table.getPartition(20).updateVisibleVersionAndTime(2, 200);
        table.updateVisibleVersionAndTime(3, 200);
        Assertions.assertTrue(cached.matches(table, 3));
        Assertions.assertEquals(cached.getCacheKey(), snapshot(table).getCacheKey());
    }

    @Test
    void testScannedPartitionWriteChangesKey() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        table.getPartition(10).updateVisibleVersionAndTime(3, 200);
        table.updateVisibleVersionAndTime(3, 200);
        Assertions.assertFalse(cached.matches(table, 3));
        OlapTableSqlCacheVersion current = new OlapTableSqlCacheVersion(table, 3);
        current.addScan(table, ImmutableMap.of(10L, 3L));
        Assertions.assertNotEquals(cached.getCacheKey(), current.getCacheKey());
    }

    @Test
    void testPrunedEmptyPartitionFirstWriteInvalidates() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        cached.addEmptyPartitions(table, ImmutableSet.of(20L));
        table.getPartition(20).updateVisibleVersionAndTime(2, 200);
        table.updateVisibleVersionAndTime(3, 200);
        Assertions.assertFalse(cached.matches(table, 3));
    }

    @Test
    void testMetadataChangesInvalidate() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        table.addPartition(partition(table, 30, 1));
        Assertions.assertFalse(cached.matches(table, 2));
        cached = snapshot(table);
        table.getIndexMetaByIndexId(table.getBaseIndexId()).setSchemaVersion(1);
        Assertions.assertFalse(cached.matches(table, 2));
    }

    @Test
    void testConcurrentCommitDuringPlanningKeepsTableVersionFallback() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = new OlapTableSqlCacheVersion(table, 2);
        table.updateVisibleVersionAndTime(3, 200);
        cached.addScan(table, ImmutableMap.of(10L, 2L));
        Assertions.assertFalse(cached.isUsable());
        Assertions.assertFalse(new OlapTableSqlCacheVersion(table, 3).isUsable());
    }

    @Test
    void testAllSelfJoinScansContributeDependencies() throws Exception {
        OlapTable table = table();
        table.getPartition(20).updateVisibleVersionAndTime(2, 100);
        OlapTableSqlCacheVersion cached = snapshot(table);
        cached.addScan(table, ImmutableMap.of(20L, 2L));
        table.getPartition(20).updateVisibleVersionAndTime(3, 200);
        table.updateVisibleVersionAndTime(3, 200);
        Assertions.assertFalse(cached.matches(table, 3));
    }

    @Test
    void testMetadataAggregateRetainsWholeTableDependency() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        cached.useTableVersion();
        Assertions.assertFalse(cached.isUsable());
    }

    @Test
    void testEmptyPartitionRefreshDoesNotChangeKey() throws Exception {
        OlapTable table = table();
        OlapTableSqlCacheVersion cached = snapshot(table);
        cached.addEmptyPartitions(table, ImmutableSet.of(20L));
        table.getPartition(20).updateVisibleVersionAndTime(1, 200);
        table.updateVisibleVersionAndTime(3, 200);
        Assertions.assertTrue(cached.matches(table, 3));
        OlapTableSqlCacheVersion refreshed = snapshot(table);
        refreshed.addEmptyPartitions(table, ImmutableSet.of(20L));
        Assertions.assertEquals(cached.getCacheKey(), refreshed.getCacheKey());
    }

    private OlapTable cacheableTable() {
        OlapTable table = Mockito.spy(table());
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.doReturn(true).when(table).isPartitionedTable();
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("cache_db");
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.isInternalCatalog()).thenReturn(true);
        return table;
    }

    @Test
    void testVariableAwareCacheKeysRetainTableVersionInBothBindingOrders() throws Exception {
        OlapTable table = cacheableTable();
        Variable variable = new Variable("partition_id", VariableType.USER, new IntegerLiteral(1));

        SqlCacheContext tableFirst = new SqlCacheContext(UserIdentity.ROOT);
        tableFirst.addUsedTable(table);
        tableFirst.addOlapScan(table, ImmutableMap.of(10L, 2L));
        SqlCacheContext.TableVersion version = tableFirst.getUsedTables().values().iterator().next();
        Assertions.assertTrue(version.olapVersion.isUsable());
        tableFirst.addUsedVariable(variable);
        Assertions.assertFalse(version.olapVersion.isUsable());
        Assertions.assertEquals("2", version.getCacheKey());

        SqlCacheContext variableFirst = new SqlCacheContext(UserIdentity.ROOT);
        variableFirst.addUsedVariable(variable);
        variableFirst.addUsedTable(table);
        variableFirst.addOlapScan(table, ImmutableMap.of(10L, 2L));
        Assertions.assertEquals("2", variableFirst.getUsedTables().values().iterator().next().getCacheKey());
    }

    @Test
    void testMaterializedViewScanRetainsSourceVersionInBothScanOrders() {
        OlapTable table = cacheableTable();
        MTMV mv = Mockito.mock(MTMV.class);
        for (boolean mvFirst : new boolean[] {true, false}) {
            SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
            context.addUsedTable(table);
            if (mvFirst) {
                context.addOlapScan(mv, ImmutableMap.of());
            }
            context.addOlapScan(table, ImmutableMap.of(10L, 2L));
            if (!mvFirst) {
                context.addOlapScan(mv, ImmutableMap.of());
            }
            SqlCacheContext.TableVersion version = context.getUsedTables().values().iterator().next();
            Assertions.assertFalse(version.olapVersion.isUsable());
            Assertions.assertEquals("2", version.getCacheKey());
        }
    }

    @Test
    void testCloudRevalidationBypassesStalePartitionVersionCache() throws Exception {
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> cloud = Mockito.mockStatic(CloudPartition.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            OlapTable table = Mockito.spy(table());
            Mockito.doReturn(2L).when(table).getVisibleVersion();
            CloudPartition partition = new CloudPartition(10, "p10",
                    new MaterializedIndex(-1, IndexState.NORMAL), table.getDefaultDistributionInfo(), 1, table.getId());
            partition.setCachedVisibleVersion(2, 100);
            table.addPartition(partition);
            OlapTableSqlCacheVersion cached = snapshot(table);
            cloud.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(ImmutableList.of(partition)))
                    .thenReturn(ImmutableList.of(2L), ImmutableList.of(3L));
            Assertions.assertTrue(cached.matches(table, 3));
            Assertions.assertFalse(cached.matches(table, 4));
            cloud.verify(() -> CloudPartition.getSnapshotVisibleVersionFromMs(ImmutableList.of(partition)),
                    Mockito.times(2));
        }
    }
}
