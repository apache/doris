package com.baidu.palo.clone;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.DiskInfo;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;

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

    private Catalog catalog;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;

    @Before
    public void setUp() {
        // be1
        be1 = new Backend(10001, "192.168.0.1", 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        DiskInfo diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2000000);
        diskInfo2.setAvailableCapacityB(100000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        DiskInfo diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500000);
        diskInfo3.setAvailableCapacityB(490000);
        diskInfo3.setDataUsedCapacityB(10000);
        disks.put(diskInfo3.getRootPath(), diskInfo3);
        
        be1.setDisks(ImmutableMap.copyOf(disks));
        be1.setAlive(true);
        be1.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        

        // be2
        be2 = new Backend(10002, "192.168.0.2", 9052);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(2000000);
        diskInfo1.setAvailableCapacityB(1900000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(20000000);
        diskInfo2.setAvailableCapacityB(1000000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        be2.setDisks(ImmutableMap.copyOf(disks));
        be2.setAlive(true);
        be2.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);

        // be3
        be3 = new Backend(10003, "192.168.0.3", 9053);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(4000000);
        diskInfo1.setAvailableCapacityB(100000);
        diskInfo1.setDataUsedCapacityB(80000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2000000);
        diskInfo2.setAvailableCapacityB(100000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500000);
        diskInfo3.setAvailableCapacityB(490000);
        diskInfo3.setDataUsedCapacityB(10000);
        disks.put(diskInfo3.getRootPath(), diskInfo3);

        be3.setDisks(ImmutableMap.copyOf(disks));
        be3.setAlive(true);
        be3.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);

        systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(be1);
        systemInfoService.addBackend(be2);
        systemInfoService.addBackend(be3);
        
        // tablet
        invertedIndex = new TabletInvertedIndex();

        invertedIndex.addTablet(50000, new TabletMeta(1, 2, 3, 4, 5));
        invertedIndex.addReplica(50000, new Replica(50001, be1.getId(), ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50002, be2.getId(), ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50003, be3.getId(), ReplicaState.NORMAL));

        invertedIndex.addTablet(60000, new TabletMeta(1, 2, 3, 4, 5));
        invertedIndex.addReplica(60000, new Replica(60002, be2.getId(), ReplicaState.NORMAL));
        invertedIndex.addReplica(60000, new Replica(60003, be3.getId(), ReplicaState.NORMAL));

        invertedIndex.addTablet(70000, new TabletMeta(1, 2, 3, 4, 5));
        invertedIndex.addReplica(70000, new Replica(70002, be2.getId(), ReplicaState.NORMAL));
        invertedIndex.addReplica(70000, new Replica(70003, be3.getId(), ReplicaState.NORMAL));
    }

    @Test
    public void test() {
        ClusterLoadStatistic loadStatistic = new ClusterLoadStatistic(catalog, systemInfoService, invertedIndex);
        loadStatistic.init(SystemInfoService.DEFAULT_CLUSTER);
        List<List<String>> infos = loadStatistic.getCLusterStatistic();
        System.out.println(infos);
        Assert.assertEquals(3, infos.size());
    }

}
