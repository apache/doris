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

package org.apache.doris.qe.cache;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.SqlCacheContext.ScanTable;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class CacheManagerTest {

    @BeforeClass
    public static void setUpClass() {
        MetricRepo.init();
    }

    @Test
    public void testBuildCacheTableForOlapScanNodeBypassesCacheWhenPartitionDropped() throws Exception {
        OlapScanNode node = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Partition partition1 = Mockito.mock(Partition.class);

        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(1L, 2L, 3L);

        Mockito.when(node.getOlapTable()).thenReturn(olapTable);
        Mockito.when(node.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        Mockito.when(olapTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("testDb");
        Mockito.when(olapTable.getName()).thenReturn("test_tbl");
        Mockito.when(olapTable.getPartition(1L)).thenReturn(partition1);
        Mockito.when(olapTable.getPartition(2L)).thenReturn(null);
        Mockito.when(partition1.getVisibleVersionTime()).thenReturn(1000L);
        Mockito.when(partition1.getId()).thenReturn(1L);
        Mockito.when(partition1.getCachedVisibleVersion()).thenReturn(10L);

        // A partition dropped between planning and cache building makes the selected set
        // inconsistent, so building the cache table must fail and the cache is bypassed.
        // Assert on the controlled message (not just RuntimeException) so a regression to the
        // old bare NullPointerException path is caught rather than silently satisfying the test.
        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> analyzer.buildCacheTableForOlapScanNode(node));
        Assert.assertFalse(ex instanceof NullPointerException);
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("Partition 2"));
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("dropped"));

        // partition3, ordered after the dropped partition2, must never be visited.
        Mockito.verify(olapTable, Mockito.never()).getPartition(3L);
    }

    @Test
    public void testBuildCacheTableForOlapScanNodeBypassesCacheWhenCloudBatchLookupNpes() throws Exception {
        // Simulates the cloud-mode race where the same dropped partition also resolves to null
        // inside getVersionInBatchForCloudMode's own partition lookup, which throws an NPE
        // (not RpcException) before the explicit null-check below is ever reached.
        OlapScanNode node = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Partition partition1 = Mockito.mock(Partition.class);

        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(1L, 2L);

        Mockito.when(node.getOlapTable()).thenReturn(olapTable);
        Mockito.when(node.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        Mockito.when(olapTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("testDb");
        Mockito.when(olapTable.getName()).thenReturn("test_tbl_cloud");
        Mockito.when(olapTable.getPartition(1L)).thenReturn(partition1);
        Mockito.when(olapTable.getPartition(2L)).thenReturn(null);
        Mockito.doThrow(new NullPointerException("simulated cloud batch lookup NPE on dropped partition"))
                .when(olapTable).getVersionInBatchForCloudMode(Mockito.anyCollection());

        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> analyzer.buildCacheTableForOlapScanNode(node));
        Assert.assertFalse(ex instanceof NullPointerException);
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("Partition 2"));
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("dropped"));
    }

    @Test
    public void testCheckCacheModeForNereidsFallsBackToNoneWhenPartitionDropped() throws Exception {
        // End-to-end: a dropped partition must make the whole cache-mode check resolve to
        // CacheMode.None (via buildCacheTableList's catch-and-empty-list), not throw out of
        // checkCacheModeForNereids.
        OlapScanNode node = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Partition partition1 = Mockito.mock(Partition.class);
        LogicalPlanAdapter parsedStmt = Mockito.mock(LogicalPlanAdapter.class);

        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(1L, 2L);
        Mockito.when(node.getOlapTable()).thenReturn(olapTable);
        Mockito.when(node.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        Mockito.when(olapTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("testDb");
        Mockito.when(olapTable.getName()).thenReturn("test_tbl_e2e");
        Mockito.when(olapTable.getPartition(1L)).thenReturn(partition1);
        Mockito.when(olapTable.getPartition(2L)).thenReturn(null);

        List<ScanNode> scanNodes = Lists.newArrayList(node);
        ConnectContext context = new ConnectContext();
        Config.cache_enable_sql_mode = true;
        context.getSessionVariable().setEnableSqlCache(true);
        CacheAnalyzer analyzer = new CacheAnalyzer(context, parsedStmt, scanNodes);

        analyzer.checkCacheModeForNereids(0);

        Assert.assertEquals(CacheAnalyzer.CacheMode.None, analyzer.getCacheMode());
    }

    @Test
    public void testBuildCacheTableForOlapScanNodeWhenVersionBatchFailed() throws Exception {
        OlapScanNode node = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Partition partition10 = Mockito.mock(Partition.class);
        Partition partition20 = Mockito.mock(Partition.class);

        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(10L, 20L);

        Mockito.when(node.getOlapTable()).thenReturn(olapTable);
        Mockito.when(node.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        Mockito.when(olapTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("testDb");
        Mockito.when(olapTable.getName()).thenReturn("test_tbl2");
        Mockito.when(olapTable.getPartition(10L)).thenReturn(partition10);
        Mockito.when(olapTable.getPartition(20L)).thenReturn(partition20);
        Mockito.when(partition10.getVisibleVersionTime()).thenReturn(2000L);
        Mockito.when(partition10.getId()).thenReturn(10L);
        Mockito.when(partition10.getCachedVisibleVersion()).thenReturn(100L);
        Mockito.when(partition20.getVisibleVersionTime()).thenReturn(4000L);
        Mockito.when(partition20.getId()).thenReturn(20L);
        Mockito.when(partition20.getCachedVisibleVersion()).thenReturn(200L);

        // getVersionInBatchForCloudMode throws RpcException; the method must catch it and continue.
        Mockito.doThrow(new RpcException("127.0.0.1", "mock rpc failed"))
                .when(olapTable).getVersionInBatchForCloudMode(Mockito.anyCollection());

        CacheAnalyzer.CacheTable cacheTable = analyzer.buildCacheTableForOlapScanNode(node);
        Assert.assertEquals(2L, cacheTable.partitionNum);
        Assert.assertSame(olapTable, cacheTable.table);
        Assert.assertEquals(20L, cacheTable.latestPartitionId);
        Assert.assertEquals(4000L, cacheTable.latestPartitionTime);
        Assert.assertEquals(200L, cacheTable.latestPartitionVersion);

        List<Pair<ScanTable, TableIf>> scanTables = analyzer.getScanTables();
        Assert.assertEquals(1, scanTables.size());
        Pair<ScanTable, TableIf> pair = scanTables.get(0);
        Assert.assertSame(olapTable, pair.second);
        Assert.assertEquals("internal.testDb.test_tbl2", pair.first.getFullTableName().toString());
        Assert.assertEquals(selectedPartitionIds, pair.first.getScanPartitions());
    }

    @Test
    public void testBuildCacheTableForOlapScanNodeWithOlderAndEqualVersionTime() throws Exception {
        OlapScanNode node = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Partition partition100 = Mockito.mock(Partition.class);
        Partition partition200 = Mockito.mock(Partition.class);
        Partition partition300 = Mockito.mock(Partition.class);

        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(100L, 200L, 300L);

        Mockito.when(node.getOlapTable()).thenReturn(olapTable);
        Mockito.when(node.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        Mockito.when(olapTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("testDb");
        Mockito.when(olapTable.getName()).thenReturn("test_tbl3");
        Mockito.when(olapTable.getPartition(100L)).thenReturn(partition100);
        Mockito.when(olapTable.getPartition(200L)).thenReturn(partition200);
        Mockito.when(olapTable.getPartition(300L)).thenReturn(partition300);
        Mockito.when(partition100.getVisibleVersionTime()).thenReturn(5000L);
        Mockito.when(partition100.getId()).thenReturn(100L);
        Mockito.when(partition100.getCachedVisibleVersion()).thenReturn(1000L);
        Mockito.when(partition200.getVisibleVersionTime()).thenReturn(4000L);
        Mockito.when(partition300.getVisibleVersionTime()).thenReturn(5000L);
        Mockito.when(partition300.getId()).thenReturn(300L);
        Mockito.when(partition300.getCachedVisibleVersion()).thenReturn(3000L);

        CacheAnalyzer.CacheTable cacheTable = analyzer.buildCacheTableForOlapScanNode(node);
        Assert.assertEquals(3L, cacheTable.partitionNum);
        Assert.assertSame(olapTable, cacheTable.table);
        // partition100 (5000L) is visited first, then partition300 (also 5000L) overrides because of >=.
        Assert.assertEquals(300L, cacheTable.latestPartitionId);
        Assert.assertEquals(5000L, cacheTable.latestPartitionTime);
        Assert.assertEquals(3000L, cacheTable.latestPartitionVersion);

        List<Pair<ScanTable, TableIf>> scanTables = analyzer.getScanTables();
        Assert.assertEquals(1, scanTables.size());
        Pair<ScanTable, TableIf> pair = scanTables.get(0);
        Assert.assertSame(olapTable, pair.second);
        Assert.assertEquals("internal.testDb.test_tbl3", pair.first.getFullTableName().toString());
        Assert.assertEquals(selectedPartitionIds, pair.first.getScanPartitions());
    }
}
