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

package org.apache.doris.system;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SystemInfoServiceTest {
    private SystemInfoService infoService;

    @Before
    public void setUp() {
        infoService = new SystemInfoService();
    }

    private void addBackend(long beId, String host, int hbPort) {
        Backend backend = new Backend(beId, host, hbPort);
        infoService.addBackend(backend);
    }

    @Test
    public void testGetHostAndPort() {
        String ipv4 = "192.168.1.2:9050";
        String ipv6 = "[fe80::5054:ff:fec9:dee0]:9050";
        String ipv6Error = "fe80::5054:ff:fec9:dee0:9050";
        try {
            HostInfo hostAndPort = SystemInfoService.getHostAndPort(ipv4);
            Assert.assertEquals("192.168.1.2", hostAndPort.getHost());
            Assert.assertEquals(9050, hostAndPort.getPort());
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            HostInfo hostAndPort = SystemInfoService.getHostAndPort(ipv6);
            Assert.assertEquals("fe80::5054:ff:fec9:dee0", hostAndPort.getHost());
            Assert.assertEquals(9050, hostAndPort.getPort());
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            SystemInfoService.getHostAndPort(ipv6Error);
            Assert.fail();
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBackendHbResponseSerialization() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        System.out.println(Env.getCurrentEnvJournalVersion());

        BackendHbResponse writeResponse = new BackendHbResponse(1L, 1234, 1234, 1234, 1234, 1234, "test",
                Tag.VALUE_COMPUTATION, false, 1234);

        // Write objects to file
        File file1 = new File("./BackendHbResponseSerialization");
        try {
            file1.createNewFile();

            try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file1))) {
                writeResponse.write(dos);
                dos.flush();
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }

            // Read objects from file
            try (DataInputStream dis = new DataInputStream(new FileInputStream(file1))) {
                BackendHbResponse readResponse = (BackendHbResponse) HeartbeatResponse.read(dis);
                // Before meta version 121, nodeRole will not be read, so readResponse is not equal to writeResponse
                Assert.assertTrue(readResponse.toString().equals(writeResponse.toString()));
                Assert.assertTrue(Tag.VALUE_COMPUTATION.equals(readResponse.getNodeRole()));
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }

        } finally {
            file1.delete();
        }
    }

    @Test
    public void testSelectBackendIdsByPolicy() throws Exception {
        // 1. no backend
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy, 1).size());
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy, 4).size());

        // 2. add one backend but not alive
        addBackend(10001, "192.168.1.1", 9050);
        Backend be1 = infoService.getBackend(10001);
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy, 1).size());
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy, 0).size());
        // policy with no condition
        BeSelectionPolicy policy2 = new BeSelectionPolicy.Builder().build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy2, 1).size());

        // 3. add more backends
        addBackend(10002, "192.168.1.2", 9050);
        Backend be2 = infoService.getBackend(10002);
        be2.setAlive(true);
        addBackend(10003, "192.168.1.3", 9050);
        Backend be3 = infoService.getBackend(10003);
        be3.setAlive(true);
        addBackend(10004, "192.168.1.4", 9050);
        Backend be4 = infoService.getBackend(10004);
        be4.setAlive(true);
        addBackend(10005, "192.168.1.5", 9050);
        Backend be5 = infoService.getBackend(10005);

        // b1 and be5 is dead, be2,3,4 is alive
        BeSelectionPolicy policy3 = new BeSelectionPolicy.Builder().needScheduleAvailable().build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy3, 1).size());
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy3, 1).contains(10001L));
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy3, 1).contains(10005L));
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy3, 2).size());
        Assert.assertEquals(3, infoService.selectBackendIdsByPolicy(policy3, 3).size());
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy3, 3).contains(10002L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy3, 3).contains(10003L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy3, 3).contains(10004L));
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy3, 4).size());

        // 4. set be status
        be2.setLoadDisabled(true);
        be3.setQueryDisabled(true);
        be4.setDecommissioned(true);
        // now, only b3,b4 is loadable, only be2,b4 is queryable, only be2,3 is schedulable
        BeSelectionPolicy policy4 = new BeSelectionPolicy.Builder().needScheduleAvailable().build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy4, 1).size());
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy4, 1).contains(10001L));
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy4, 1).contains(10004L));
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy4, 1).contains(10005L));
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy4, 2).size());
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy4, 2).contains(10002L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy4, 2).contains(10003L));
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy4, 3).size());

        BeSelectionPolicy policy5 = new BeSelectionPolicy.Builder().needLoadAvailable().build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy5, 1).size());
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy5, 1).contains(10001L));
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy5, 1).contains(10002L));
        Assert.assertFalse(infoService.selectBackendIdsByPolicy(policy5, 1).contains(10005L));
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy5, 2).size());
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy5, 2).contains(10003L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy5, 2).contains(10004L));

        // 5. set tags
        // reset all be
        be1.setAlive(true);
        be2.setLoadDisabled(false);
        be3.setQueryDisabled(false);
        be5.setAlive(true);
        be3.setAlive(true);
        be4.setAlive(true);
        be4.setDecommissioned(false);
        be5.setAlive(true);
        BeSelectionPolicy policy6 = new BeSelectionPolicy.Builder().needQueryAvailable().build();
        Assert.assertEquals(5, infoService.selectBackendIdsByPolicy(policy6, 5).size());

        Tag taga = Tag.create(Tag.TYPE_LOCATION, "taga");
        Tag tagb = Tag.create(Tag.TYPE_LOCATION, "tagb");
        be1.setTagMap(taga.toMap());
        be2.setTagMap(taga.toMap());
        be3.setTagMap(tagb.toMap());
        be4.setTagMap(tagb.toMap());
        be5.setTagMap(tagb.toMap());

        BeSelectionPolicy policy7 = new BeSelectionPolicy.Builder().needQueryAvailable().addTags(Sets.newHashSet(taga))
                .build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy7, 1).size());
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy7, 2).size());
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy7, 2).contains(10001L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy7, 2).contains(10002L));
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy7, 3).size());

        BeSelectionPolicy policy8 = new BeSelectionPolicy.Builder().needQueryAvailable().addTags(Sets.newHashSet(tagb))
                .build();
        Assert.assertEquals(3, infoService.selectBackendIdsByPolicy(policy8, 3).size());
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy8, 3).contains(10003L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy8, 3).contains(10004L));
        Assert.assertTrue(infoService.selectBackendIdsByPolicy(policy8, 3).contains(10005L));

        BeSelectionPolicy policy9 = new BeSelectionPolicy.Builder().needQueryAvailable()
                .addTags(Sets.newHashSet(taga, tagb)).build();
        Assert.assertEquals(5, infoService.selectBackendIdsByPolicy(policy9, 5).size());

        // 6. check storage medium
        addDisk(be1, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 1 * 1024 * 1024L);
        addDisk(be2, "path2", TStorageMedium.SSD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        addDisk(be3, "path3", TStorageMedium.SSD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        addDisk(be4, "path4", TStorageMedium.SSD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        addDisk(be5, "path5", TStorageMedium.SSD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);

        BeSelectionPolicy policy10 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga, tagb))
                .setStorageMedium(TStorageMedium.SSD).build();
        Assert.assertEquals(4, infoService.selectBackendIdsByPolicy(policy10, 4).size());
        Assert.assertEquals(3, infoService.selectBackendIdsByPolicy(policy10, 3).size());
        // check return as many as possible
        Assert.assertEquals(4, infoService.selectBackendIdsByPolicy(policy10, -1).size());
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy10, 5).size());

        BeSelectionPolicy policy11 =
                new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(tagb)).setStorageMedium(TStorageMedium.HDD)
                        .build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy11, 1).size());

        // 7. check disk usage
        BeSelectionPolicy policy12 =
                new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga)).setStorageMedium(TStorageMedium.HDD)
                        .build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy12, 1).size());
        BeSelectionPolicy policy13 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).needCheckDiskUsage().build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy13, 1).size());

        // 8. check same host
        addBackend(10006, "192.168.1.1", 9051);
        Backend be6 = infoService.getBackend(10006);
        be6.setTagMap(taga.toMap());
        be6.setAlive(true);
        addDisk(be1, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 100 * 1024 * 1024L);
        addDisk(be6, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 100 * 1024 * 1024L);
        BeSelectionPolicy policy14 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy14, 2).size());
        BeSelectionPolicy policy15 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).allowOnSameHost().build();
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy15, 2).size());
    }

    @Test
    public void testComputeNodeBackendSelect() throws Exception {
        // only one compute node
        Tag taga = Tag.create(Tag.TYPE_LOCATION, "taga");
        addBackend(20001, "192.168.2.1", 9051);
        Backend be1 = infoService.getBackend(20001);
        setComputeNode(be1, taga);
        be1.setAlive(true);
        addDisk(be1, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 100 * 1024 * 1024L);
        BeSelectionPolicy policy01 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy01, 1).size());

        BeSelectionPolicy policy02 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).preferComputeNode(true).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy02, 1).size());

        BeSelectionPolicy policy03 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).preferComputeNode(true).assignExpectBeNum(0).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy03, 1).size());

        BeSelectionPolicy policy04 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).preferComputeNode(true).assignExpectBeNum(1).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy04, 1).size());

        // one compute node and two mix node
        addBackend(20002, "192.168.2.2", 9051);
        Backend be2 = infoService.getBackend(20002);
        be2.setTagMap(taga.toMap());
        be2.setAlive(true);
        addDisk(be2, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 100 * 1024 * 1024L);

        addBackend(20003, "192.168.2.3", 9051);
        Backend be3 = infoService.getBackend(20003);
        be3.setTagMap(taga.toMap());
        be3.setAlive(true);
        addDisk(be3, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 100 * 1024 * 1024L);

        BeSelectionPolicy policy05 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).build();
        Assert.assertEquals(0, infoService.selectBackendIdsByPolicy(policy05, 3).size());

        BeSelectionPolicy policy06 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).preferComputeNode(true).assignExpectBeNum(2).build();
        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy06, 2).size());

        BeSelectionPolicy policy07 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .setStorageMedium(TStorageMedium.HDD).preferComputeNode(true).assignExpectBeNum(3).build();
        Assert.assertEquals(3, infoService.selectBackendIdsByPolicy(policy07, 3).size());
    }

    @Test
    public void testPreferLocationsSelect() throws Exception {
        Tag taga = Tag.create(Tag.TYPE_LOCATION, "taga");

        // add more backends
        addBackend(10002, "192.168.1.2", 9050);
        Backend be2 = infoService.getBackend(10002);
        be2.setAlive(true);
        addBackend(10003, "192.168.1.3", 9050);
        Backend be3 = infoService.getBackend(10003);
        be3.setAlive(true);
        addBackend(10004, "192.168.1.4", 9050);
        Backend be4 = infoService.getBackend(10004);
        be4.setAlive(true);
        addBackend(10005, "192.168.1.5", 9050);
        Backend be5 = infoService.getBackend(10005);
        be5.setAlive(true);

        setComputeNode(be5, taga);

        List<String> preferLocations = new ArrayList<>();
        preferLocations.add("192.168.1.2");
        BeSelectionPolicy policy1 = new BeSelectionPolicy.Builder().addPreLocations(preferLocations).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy1, 1).size());
        preferLocations.add("192.168.1.3");
        BeSelectionPolicy policy2 = new BeSelectionPolicy.Builder().addPreLocations(preferLocations).build();

        Assert.assertEquals(2, infoService.selectBackendIdsByPolicy(policy2, 2).size());

        // only one preferLocations
        preferLocations.clear();
        preferLocations.add("192.168.1.4");
        BeSelectionPolicy policy3 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .addPreLocations(preferLocations).preferComputeNode(true).assignExpectBeNum(3).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy3, 1).size());

        preferLocations.add("192.168.1.5");
        BeSelectionPolicy policy4 = new BeSelectionPolicy.Builder().addTags(Sets.newHashSet(taga))
                .addPreLocations(preferLocations).preferComputeNode(true).assignExpectBeNum(1).build();
        Assert.assertEquals(1, infoService.selectBackendIdsByPolicy(policy4, 1).size());
    }

    @Test
    public void testSelectBackendIdsForReplicaCreation() throws Exception {
        addBackend(10001, "192.168.1.1", 9050);
        Backend be1 = infoService.getBackend(10001);
        addDisk(be1, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be1.setAlive(true);
        addBackend(10002, "192.168.1.2", 9050);
        Backend be2 = infoService.getBackend(10002);
        addDisk(be2, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be2.setAlive(true);
        addBackend(10003, "192.168.1.3", 9050);
        Backend be3 = infoService.getBackend(10003);
        addDisk(be3, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be3.setAlive(true);
        addBackend(10004, "192.168.1.4", 9050);
        Backend be4 = infoService.getBackend(10004);
        addDisk(be4, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be4.setAlive(true);
        addBackend(10005, "192.168.1.5", 9050);
        Backend be5 = infoService.getBackend(10005);
        addDisk(be5, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be5.setAlive(true);
        // no effect with compute node
        addBackend(10006, "192.168.1.6", 9050);
        Backend be6 = infoService.getBackend(10006);
        addDisk(be6, "path1", TStorageMedium.HDD, 200 * 1024 * 1024L, 150 * 1024 * 1024L);
        be6.setAlive(true);
        setComputeNode(be6, Tag.DEFAULT_BACKEND_TAG);

        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        // also check if the random selection logic can evenly distribute the replica.
        Map<Long, Integer> beCounterMap = Maps.newHashMap();
        for (int i = 0; i < 10000; ++i) {
            Map<Tag, List<Long>> res = infoService.selectBackendIdsForReplicaCreation(replicaAlloc,
                    TStorageMedium.HDD, false, false);
            Assert.assertEquals(3, res.get(Tag.DEFAULT_BACKEND_TAG).size());
            for (Long beId : res.get(Tag.DEFAULT_BACKEND_TAG)) {
                beCounterMap.put(beId, beCounterMap.getOrDefault(beId, 0) + 1);
            }
        }
        List<Integer> list = Lists.newArrayList(beCounterMap.values());
        Collections.sort(list);
        int diff = list.get(list.size() - 1) - list.get(0);
        // The max replica num and min replica num's diff is less than 5%.
        Assert.assertTrue((diff * 1.0 / list.get(0)) < 0.05);
    }

    private void addDisk(Backend be, String path, TStorageMedium medium, long totalB, long availB) {
        DiskInfo diskInfo1 = new DiskInfo(path);
        diskInfo1.setTotalCapacityB(totalB);
        diskInfo1.setAvailableCapacityB(availB);
        diskInfo1.setStorageMedium(medium);
        Map<String, DiskInfo> map = Maps.newHashMap();
        map.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(map));
    }

    private void setComputeNode(Backend be, Tag tag) {
        Map<String, String> tagMap = tag.toMap();
        tagMap.put(Tag.TYPE_ROLE, Tag.VALUE_COMPUTATION);
        be.setTagMap(tagMap);
    }

}
