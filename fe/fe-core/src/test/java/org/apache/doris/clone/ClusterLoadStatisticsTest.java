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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ClusterLoadStatisticsTest {

    private Backend be1;
    private Backend be2;
    private Backend be3;
    private Backend be4;

    private Env env;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;

    @Before
    public void setUp() {
        // be1
        // 50%, 95%, 2%
        be1 = new Backend(10001, "192.168.0.1", 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1_000_000);
        diskInfo1.setAvailableCapacityB(500_000);
        diskInfo1.setDataUsedCapacityB(480_000);
        diskInfo1.setPathHash(1001);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        DiskInfo diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2_000_000);
        diskInfo2.setAvailableCapacityB(100_000);
        diskInfo2.setDataUsedCapacityB(80_000);
        diskInfo2.setPathHash(1002);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        DiskInfo diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500_000);
        diskInfo3.setAvailableCapacityB(490_000);
        diskInfo3.setDataUsedCapacityB(10_000);
        diskInfo3.setPathHash(1003);
        disks.put(diskInfo3.getRootPath(), diskInfo3);

        be1.setDisks(ImmutableMap.copyOf(disks));
        be1.setAlive(true);

        // be2
        be2 = new Backend(10002, "192.168.0.2", 9052);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(2_000_000);
        diskInfo1.setAvailableCapacityB(1_900_000);
        diskInfo1.setDataUsedCapacityB(480_000);
        diskInfo1.setPathHash(2001);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(20_000_000);
        diskInfo2.setAvailableCapacityB(1_000_000);
        diskInfo2.setDataUsedCapacityB(80_000);
        diskInfo2.setPathHash(2002);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        be2.setDisks(ImmutableMap.copyOf(disks));
        be2.setAlive(true);

        // be3
        be3 = new Backend(10003, "192.168.0.3", 9053);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(4_000_000);
        diskInfo1.setAvailableCapacityB(100_000);
        diskInfo1.setDataUsedCapacityB(80_000);
        diskInfo1.setPathHash(3001);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2_000_000);
        diskInfo2.setAvailableCapacityB(100_000);
        diskInfo2.setDataUsedCapacityB(80_000);
        diskInfo2.setPathHash(3002);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500_000);
        diskInfo3.setAvailableCapacityB(490_000);
        diskInfo3.setDataUsedCapacityB(10_000);
        diskInfo3.setPathHash(3003);
        disks.put(diskInfo3.getRootPath(), diskInfo3);

        be3.setDisks(ImmutableMap.copyOf(disks));
        be3.setAlive(true);

        // compute role node
        be4 = new Backend(10004, "192.168.0.4", 9053);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(4_000_000);
        diskInfo1.setAvailableCapacityB(100_000);
        diskInfo1.setDataUsedCapacityB(80_000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        be4.setDisks(ImmutableMap.copyOf(disks));
        be4.setAlive(true);
        Map<String, String> tagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap.put(Tag.TYPE_ROLE, Tag.VALUE_COMPUTATION);
        be4.setTagMap(tagMap);

        systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(be1);
        systemInfoService.addBackend(be2);
        systemInfoService.addBackend(be3);
        systemInfoService.addBackend(be4);

        // tablet
        invertedIndex = new TabletInvertedIndex();

        invertedIndex.addTablet(50000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(50000, new Replica(50001, be1.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(60000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(60000, new Replica(60002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(60000, new Replica(60003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(70000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(70000, new Replica(70002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(70000, new Replica(70003, be3.getId(), 0, ReplicaState.NORMAL));
    }

    @Test
    public void test() {
        LoadStatisticForTag loadStatistic = new LoadStatisticForTag(
                Tag.DEFAULT_BACKEND_TAG, systemInfoService, invertedIndex);
        loadStatistic.init();
        List<List<String>> infos = loadStatistic.getStatistic(TStorageMedium.HDD);
        Assert.assertEquals(3, infos.size());
        BackendLoadStatistic beStat1 = loadStatistic.getBackendLoadStatistic(be1.getId());
        Assert.assertNotNull(beStat1);
        RootPathLoadStatistic path2 = beStat1.getPathStatisticByPathHash(1002);
        RootPathLoadStatistic path3 = beStat1.getPathStatisticByPathHash(1003);
        Assert.assertEquals(Classification.HIGH, path2.getLocalClazz());
        Assert.assertEquals(Classification.HIGH, path2.getGlobalClazz());
        Assert.assertEquals(Classification.LOW, path3.getLocalClazz());
        Assert.assertEquals(Classification.LOW, path3.getGlobalClazz());
    }

}
