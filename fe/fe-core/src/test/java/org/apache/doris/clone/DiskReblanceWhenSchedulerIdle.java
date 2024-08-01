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


import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class DiskReblanceWhenSchedulerIdle extends TestWithFeService {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_round_robin_create_tablet = true;
        Config.allow_replica_on_same_host = true;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
        Config.schedule_slot_num_per_hdd_path = 1;
        Config.disable_balance = true;
        Config.enable_debug_points = true;
    }

    @Override
    protected int backendNum() {
        return 2;
    }

    @Test
    public void testDiskReblanceWhenSchedulerIdle() throws Exception {
        // case start
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        Assertions.assertEquals(backendNum(), backends.size());
        for (Backend be : backends) {
            Assertions.assertEquals(0, invertedIndex.getTabletNumByBackendId(be.getId()));
        }

        long totalCapacity = 10L << 30;

        for (int i = 0; i < backends.size(); i++) {
            Map<String, DiskInfo> disks = Maps.newHashMap();
            for (int j = 0; j < 2; j++) {
                DiskInfo diskInfo = new DiskInfo("be_" + i + "_disk_" + j);
                diskInfo.setTotalCapacityB(totalCapacity);
                diskInfo.setDataUsedCapacityB(1L << 30);
                diskInfo.setAvailableCapacityB(9L << 30);
                diskInfo.setPathHash((1000L * (i + 1)) + 10 * j);
                disks.put(diskInfo.getRootPath(), diskInfo);
            }
            backends.get(i).setDisks(ImmutableMap.copyOf(disks));
        }
        Backend be0 = backends.get(0);

        createDatabase("test");
        createTable("CREATE TABLE test.tbl1 (k INT) DISTRIBUTED BY HASH(k) "
                + " BUCKETS 4 PROPERTIES ( \"replication_num\" = \"1\","
                + " \"storage_medium\" = \"HDD\")");

        Assertions.assertEquals(2, invertedIndex.getTabletNumByBackendId(backends.get(0).getId()));
        Assertions.assertEquals(2, invertedIndex.getTabletNumByBackendId(backends.get(1).getId()));


        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(tbl);
        Partition partition = tbl.getPartitions().iterator().next();
        List<Tablet> tablets = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets();

        DiskInfo diskInfo0 = be0.getDisks().values().asList().get(0);
        DiskInfo diskInfo1 = be0.getDisks().values().asList().get(1);

        tablets.forEach(tablet -> {
            Lists.newArrayList(tablet.getReplicas()).forEach(
                    replica -> {
                    if (replica.getBackendId() == backends.get(1).getId()) {
                        replica.setDataSize(totalCapacity / 4);
                        replica.setRowCount(1);
                        tablet.deleteReplica(replica);
                        replica.setBackendId(backends.get(0).getId());
                        replica.setPathHash(diskInfo0.getPathHash());
                        tablet.addReplica(replica);
                    } else {
                        replica.setPathHash(diskInfo0.getPathHash());
                    }
                }
            );
        });

        diskInfo0.setAvailableCapacityB(0L);
        diskInfo1.setAvailableCapacityB(totalCapacity);
        DebugPointUtil.addDebugPointWithValue("FE.HIGH_LOAD_BE_ID", backends.get(1).getId());

        Table<Tag, TStorageMedium, Long> lastPickTimeTable = Env.getCurrentEnv().getTabletScheduler().getRebalancer().getLastPickTimeTable();
        lastPickTimeTable.put(Tag.DEFAULT_BACKEND_TAG, TStorageMedium.HDD, 0L);
        Config.disable_balance = false;


        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Map<Long, Integer> gotDiskTabletNums = Maps.newHashMap();
        tablets.forEach(tablet -> tablet.getReplicas().forEach(replica -> {
            gotDiskTabletNums.put(replica.getPathHash(), 1 + gotDiskTabletNums.getOrDefault(replica.getPathHash(), 0));
        }));


        Map<Long, Integer> expectTabletNums = Maps.newHashMap();
        expectTabletNums.put(diskInfo0.getPathHash(), 2);
        expectTabletNums.put(diskInfo1.getPathHash(), 2);

        Assertions.assertEquals(expectTabletNums, gotDiskTabletNums);
    }
}
