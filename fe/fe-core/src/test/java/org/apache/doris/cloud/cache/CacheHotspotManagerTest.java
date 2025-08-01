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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Triple;
import org.apache.doris.system.Backend;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

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
        partition = new Partition(0, null, null, null);
        new MockUp<Partition>() {

            @Mock
            public long getDataSize(boolean singleReplica) {
                return 10000000L;
            }

            @Mock
            public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState) {
                List<MaterializedIndex> list = new ArrayList<>();
                MaterializedIndex ind = new MaterializedIndex();
                list.add(ind);
                return list;
            }
        };

        cloudSystemInfoService = new CloudSystemInfoService();
        cacheHotspotManager = new CacheHotspotManager(cloudSystemInfoService);
        new MockUp<CacheHotspotManager>() {

            @Mock
            Long getFileCacheCapacity(String clusterName) throws RuntimeException {
                return 100L;
            }

            @Mock
            List<Partition> getPartitionsFromTriple(Triple<String, String, String> tableTriple) {
                List<Partition> partitions = new ArrayList<>();
                partition = new Partition(1, "p1", null, null);
                partitions.add(partition);
                return partitions;
            }

            @Mock
            List<Backend> getBackendsFromCluster(String dstClusterName) {
                List<Backend> backends = new ArrayList<>();
                Backend backend = new Backend(11, dstClusterName, 0);
                backends.add(backend);
                return backends;
            }

            @Mock
            public List<Tablet> getTabletsFromIndexs(List<MaterializedIndex> indexes) {
                List<Tablet> list = new ArrayList<>();
                Tablet tablet = new Tablet(1001L);
                list.add(tablet);
                return list;
            }

            @Mock
            Set<Long> getTabletIdsFromBe(long beId) {
                Set<Long> tabletIds = new HashSet<Long>();
                tabletIds.add(1001L);
                return tabletIds;
            }
        };

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
