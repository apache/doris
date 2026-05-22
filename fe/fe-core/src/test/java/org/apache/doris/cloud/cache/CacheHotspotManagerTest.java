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

package org.apache.doris.cloud.cache;

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Triple;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CacheHotspotManagerTest {
    private CacheHotspotManager cacheHotspotManager;
    private CloudSystemInfoService cloudSystemInfoService;
    private Partition partition;

    @Test
    public void testWarmUpNewClusterByTable() {
        cloudSystemInfoService = new CloudSystemInfoService();
        // Use mock with CALLS_REAL_METHODS to avoid package-private access issues
        // (CacheHotspotManager's helper methods are package-private)
        cacheHotspotManager = Mockito.mock(CacheHotspotManager.class, invocation -> {
            String methodName = invocation.getMethod().getName();
            switch (methodName) {
                case "getFileCacheCapacity":
                    return 100L;
                case "getPartitionsFromTriple": {
                    List<Partition> partitions = new ArrayList<>();
                    Partition spyPartition = Mockito.spy(new Partition(1, "p1", null, null));
                    Mockito.doReturn(10000000L).when(spyPartition).getDataSize(Mockito.anyBoolean());
                    List<MaterializedIndex> list = new ArrayList<>();
                    list.add(new MaterializedIndex());
                    Mockito.doReturn(list).when(spyPartition)
                            .getMaterializedIndices(Mockito.any(IndexExtState.class));
                    partitions.add(spyPartition);
                    return partitions;
                }
                case "getBackendsFromCluster": {
                    String dstClusterName = (String) invocation.getArgument(0);
                    List<Backend> backends = new ArrayList<>();
                    backends.add(new Backend(11, dstClusterName, 0));
                    return backends;
                }
                case "getTabletsFromIndexs": {
                    List<Tablet> list = new ArrayList<>();
                    list.add(new CloudTablet(1001L));
                    return list;
                }
                case "getTabletIdsFromBe": {
                    Set<Long> tabletIds = new HashSet<>();
                    tabletIds.add(1001L);
                    return tabletIds;
                }
                default:
                    return invocation.callRealMethod();
            }
        });

        // Setup mock data
        long jobId = 1L;
        String dstClusterName = "test_cluster";
        List<Triple<String, String, String>> tables = new ArrayList<>();
        tables.add(Triple.of("test_db", "test_table", ""));

        // force = true
        Map<Long, List<Tablet>> result = cacheHotspotManager.warmUpNewClusterByTable(
                jobId, dstClusterName, tables, true);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(11L).get(0).getId(), 1001L);

        // force = false
        RuntimeException exception = Assert.assertThrows(RuntimeException.class, () -> {
            cacheHotspotManager.warmUpNewClusterByTable(jobId, dstClusterName, tables, false);
        });
        Assert.assertEquals("The cluster " + dstClusterName + " cache size is not enough", exception.getMessage());
    }
}
