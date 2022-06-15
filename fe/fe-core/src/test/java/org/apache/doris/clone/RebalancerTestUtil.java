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

package org.apache.doris.clone;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RebalancerTestUtil {

    // Add only one path, PathHash:id
    public static Backend createBackend(long id, long totalCap, long usedCap) {
        // ip:port won't be checked
        Backend be = new Backend(id, "192.168.0." + id, 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo = new DiskInfo("/path1");
        diskInfo.setPathHash(id);
        diskInfo.setTotalCapacityB(totalCap);
        diskInfo.setDataUsedCapacityB(usedCap);
        diskInfo.setAvailableCapacityB(totalCap - usedCap);
        disks.put(diskInfo.getRootPath(), diskInfo);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(true);
        be.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        return be;
    }

    // Create one tablet(and its replicas) for one partition. The replicas will created on backends which are numbered in beIds.
    // The tablet will be added to TabletInvertedIndex & OlapTable.
    // Only use the partition's baseIndex for simplicity
    public static void createTablet(TabletInvertedIndex invertedIndex, Database db, OlapTable olapTable, String partitionName, TStorageMedium medium,
                                    int tabletId, List<Long> beIds) {
        Partition partition = olapTable.getPartition(partitionName);
        MaterializedIndex baseIndex = partition.getBaseIndex();
        int schemaHash = olapTable.getSchemaHashByIndexId(baseIndex.getId());

        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(), partition.getId(), baseIndex.getId(),
                schemaHash, medium);
        Tablet tablet = new Tablet(tabletId);

        // add tablet to olapTable
        olapTable.getPartition("p0").getBaseIndex().addTablet(tablet, tabletMeta);
        createReplicasAndAddToIndex(invertedIndex, tabletMeta, tablet, beIds);
    }

    // Create replicas on backends which are numbered in beIds.
    // The tablet & replicas will be added to invertedIndex.
    public static void createReplicasAndAddToIndex(TabletInvertedIndex invertedIndex, TabletMeta tabletMeta, Tablet tablet, List<Long> beIds) {
        invertedIndex.addTablet(tablet.getId(), tabletMeta);

        IntStream.range(0, beIds.size()).forEach(i -> {
            Replica replica = new Replica(tablet.getId() + i, beIds.get(i), Replica.ReplicaState.NORMAL, 1, tabletMeta.getOldSchemaHash());
            // We've set pathHash to beId for simplicity
            replica.setPathHash(beIds.get(i));
            // isRestore set true, to avoid modifying Catalog.getCurrentInvertedIndex
            tablet.addReplica(replica, true);
            invertedIndex.addReplica(tablet.getId(), replica);
        });
    }
}
