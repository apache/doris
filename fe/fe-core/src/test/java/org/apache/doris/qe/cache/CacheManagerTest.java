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
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.SqlCacheContext.ScanTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CacheManagerTest {

    @Test
    public void testBuildCacheTableForOlapScanNode(@Mocked OlapScanNode node,
            @Mocked OlapTable olapTable,
            @Mocked DatabaseIf database,
            @Mocked CatalogIf catalog,
            @Mocked Partition partition1,
            @Mocked Partition partition3) throws Exception {
        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(1L, 2L, 3L);

        new Expectations() {
            {
                node.getOlapTable();
                result = olapTable;

                node.getSelectedPartitionIds();
                result = selectedPartitionIds;
                minTimes = 3;

                olapTable.getDatabase();
                result = database;

                database.getCatalog();
                result = catalog;

                catalog.getName();
                result = "internal";

                database.getFullName();
                result = "testDb";

                olapTable.getName();
                result = "test_tbl";

                olapTable.getVersionInBatchForCloudMode(selectedPartitionIds);

                olapTable.getPartition(1L);
                result = partition1;

                olapTable.getPartition(2L);
                result = null;

                olapTable.getPartition(3L);
                result = partition3;

                partition1.getVisibleVersionTime();
                result = 1000L;
                partition1.getId();
                result = 1L;
                partition1.getCachedVisibleVersion();
                result = 10L;

                partition3.getVisibleVersionTime();
                result = 3000L;
                partition3.getId();
                result = 3L;
                partition3.getCachedVisibleVersion();
                result = 30L;
            }
        };

        CacheAnalyzer.CacheTable cacheTable = analyzer.buildCacheTableForOlapScanNode(node);
        Assert.assertEquals(3L, cacheTable.partitionNum);
        Assert.assertSame(olapTable, cacheTable.table);
        Assert.assertEquals(3L, cacheTable.latestPartitionId);
        Assert.assertEquals(3000L, cacheTable.latestPartitionTime);
        Assert.assertEquals(30L, cacheTable.latestPartitionVersion);

        List<Pair<ScanTable, TableIf>> scanTables = analyzer.getScanTables();
        Assert.assertEquals(1, scanTables.size());
        Pair<ScanTable, TableIf> pair = scanTables.get(0);
        Assert.assertSame(olapTable, pair.second);
        Assert.assertEquals("internal.testDb.test_tbl", pair.first.getFullTableName().toString());
        Assert.assertEquals(Lists.newArrayList(1L, 3L), pair.first.getScanPartitions());
    }

    @Test
    public void testBuildCacheTableForOlapScanNodeWhenVersionBatchFailed(@Mocked OlapScanNode node,
            @Mocked OlapTable olapTable,
            @Mocked DatabaseIf database,
            @Mocked CatalogIf catalog,
            @Mocked Partition partition10,
            @Mocked Partition partition20) throws Exception {
        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(10L, 20L);

        new Expectations() {
            {
                node.getOlapTable();
                result = olapTable;

                node.getSelectedPartitionIds();
                result = selectedPartitionIds;
                minTimes = 3;

                olapTable.getDatabase();
                result = database;

                database.getCatalog();
                result = catalog;

                catalog.getName();
                result = "internal";

                database.getFullName();
                result = "testDb";

                olapTable.getName();
                result = "test_tbl2";

                olapTable.getVersionInBatchForCloudMode((Collection<Long>) any);
                result = new RpcException("127.0.0.1", "mock rpc failed");

                olapTable.getPartition(10L);
                result = partition10;
                olapTable.getPartition(20L);
                result = partition20;

                partition10.getVisibleVersionTime();
                result = 2000L;
                partition10.getId();
                result = 10L;
                partition10.getCachedVisibleVersion();
                result = 100L;

                partition20.getVisibleVersionTime();
                result = 4000L;
                partition20.getId();
                result = 20L;
                partition20.getCachedVisibleVersion();
                result = 200L;
            }
        };

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
    public void testBuildCacheTableForOlapScanNodeWithOlderAndEqualVersionTime(@Mocked OlapScanNode node,
            @Mocked OlapTable olapTable,
            @Mocked DatabaseIf database,
            @Mocked CatalogIf catalog,
            @Mocked Partition partition100,
            @Mocked Partition partition200,
            @Mocked Partition partition300) throws Exception {
        CacheAnalyzer analyzer = new CacheAnalyzer(new ConnectContext(), null, Lists.newArrayList());
        ArrayList<Long> selectedPartitionIds = Lists.newArrayList(100L, 200L, 300L);

        new Expectations() {
            {
                node.getOlapTable();
                result = olapTable;

                node.getSelectedPartitionIds();
                result = selectedPartitionIds;
                minTimes = 3;

                olapTable.getDatabase();
                result = database;

                database.getCatalog();
                result = catalog;

                catalog.getName();
                result = "internal";

                database.getFullName();
                result = "testDb";

                olapTable.getName();
                result = "test_tbl3";

                olapTable.getVersionInBatchForCloudMode(selectedPartitionIds);

                olapTable.getPartition(100L);
                result = partition100;
                olapTable.getPartition(200L);
                result = partition200;
                olapTable.getPartition(300L);
                result = partition300;

                partition100.getVisibleVersionTime();
                result = 5000L;
                partition100.getId();
                result = 100L;
                partition100.getCachedVisibleVersion();
                result = 1000L;

                partition200.getVisibleVersionTime();
                result = 4000L;

                partition300.getVisibleVersionTime();
                result = 5000L;
                partition300.getId();
                result = 300L;
                partition300.getCachedVisibleVersion();
                result = 3000L;
            }
        };

        CacheAnalyzer.CacheTable cacheTable = analyzer.buildCacheTableForOlapScanNode(node);
        Assert.assertEquals(3L, cacheTable.partitionNum);
        Assert.assertSame(olapTable, cacheTable.table);
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
