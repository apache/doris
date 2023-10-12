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

package org.apache.doris.catalog;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.common.FeConstants;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TDiskType;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BackendTest {
    private Backend backend;
    private long backendId = 9999;
    private String host = "myhost";
    private int heartbeatPort = 21234;
    private int bePort = 21235;
    private int httpPort = 21237;
    private int beRpcPort = 21238;

    private Env env;

    private FakeEnv fakeEnv;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() {
        env = AccessTestUtil.fetchAdminCatalog();

        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();

        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
        FakeEnv.setSystemInfo(AccessTestUtil.fetchSystemInfoService());

        backend = new Backend(backendId, host, heartbeatPort);
        backend.updateOnce(bePort, httpPort, beRpcPort);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(backendId, backend.getId());
        Assert.assertEquals(host, backend.getHost());
        Assert.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        Assert.assertEquals(bePort, backend.getBePort());

        // set new port
        int newBePort = 31235;
        int newHttpPort = 31237;
        backend.updateOnce(newBePort, newHttpPort, beRpcPort);
        Assert.assertEquals(newBePort, backend.getBePort());

        // check alive
        Assert.assertTrue(backend.isAlive());
    }

    @Test
    public void diskInfoTest() {
        Map<String, TDisk> diskInfos = new HashMap<String, TDisk>();

        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        disk1.setDirType(TDiskType.STORAGE);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        disk2.setDirType(TDiskType.STORAGE);
        TDisk disk3 = new TDisk("/data3/", 3000, 600, false);
        disk3.setDirType(TDiskType.STORAGE);

        diskInfos.put(disk1.getRootPath(), disk1);
        diskInfos.put(disk2.getRootPath(), disk2);
        diskInfos.put(disk3.getRootPath(), disk3);

        // first update
        backend.updateDisks(diskInfos);
        Assert.assertEquals(disk1.getDiskTotalCapacity() + disk2.getDiskTotalCapacity(),
                backend.getTotalCapacityB());
        Assert.assertEquals(1, backend.getAvailableCapacityB());

        // second update
        diskInfos.remove(disk1.getRootPath());
        backend.updateDisks(diskInfos);
        Assert.assertEquals(disk2.getDiskTotalCapacity(), backend.getTotalCapacityB());
        Assert.assertEquals(disk2.getDiskAvailableCapacity() + 1, backend.getAvailableCapacityB());
        Assert.assertFalse(backend.hasSpecifiedStorageMedium(TStorageMedium.SSD));
        Assert.assertTrue(backend.hasSpecifiedStorageMedium(TStorageMedium.HDD));
    }

    @Test
    public void testSerialization() throws Exception {
        // Write 100 objects to file
        Path path = Paths.get("./backendTest" + UUID.randomUUID());
        Files.createFile(path);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        List<Backend> list1 = new LinkedList<Backend>();
        List<Backend> list2 = new LinkedList<Backend>();

        DiskInfo diskInfo1 = new DiskInfo("/disk1");
        DiskInfo diskInfo2 = new DiskInfo("/disk2");
        ImmutableMap<String, DiskInfo> diskRefs = ImmutableMap.of(
                "disk1", diskInfo1,
                "disk2", diskInfo2);
        for (int count = 0; count < 100; ++count) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            list1.add(backend);
        }
        for (int count = 100; count < 200; count++) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            backend.setDisks(diskRefs);
            backend.setLastStreamLoadTime(count);
            list1.add(backend);
        }
        for (Backend backend : list1) {
            backend.write(dos);
        }
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        for (int count = 0; count < 200; ++count) {
            Backend backend = Backend.read(dis);
            list2.add(backend);
            Assert.assertEquals(count, backend.getId());
            Assert.assertEquals("10.120.22.32" + count, backend.getHost());
        }

        // check isAlive
        Backend backend100 = list2.get(100);
        Assert.assertTrue(backend100.isAlive());
        // check disksRef
        ImmutableMap<String, DiskInfo> backend100DiskRef = backend100.getDisks();
        Assert.assertEquals(2, backend100DiskRef.size());
        Assert.assertTrue(backend100DiskRef.containsKey("disk1"));
        Assert.assertTrue(backend100DiskRef.containsKey("disk2"));
        DiskInfo backend100DiskInfo1 = backend100DiskRef.get("disk1");
        Assert.assertEquals("/disk1", backend100DiskInfo1.getRootPath());
        DiskInfo backend100DiskInfo2 = backend100DiskRef.get("disk2");
        Assert.assertEquals("/disk2", backend100DiskInfo2.getRootPath());
        // check backend status
        Backend.BackendStatus backend100BackendStatus = backend100.getBackendStatus();
        Assert.assertEquals(100, backend100BackendStatus.lastStreamLoadTime);

        for (int count = 0; count < 200; count++) {
            Assert.assertEquals(list1.get(count), list2.get(count));
        }
        Assert.assertNotEquals(list1.get(1), list1.get(2));
        Assert.assertNotEquals(list1.get(1), this);
        Assert.assertEquals(list1.get(1), list1.get(1));

        Backend back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        Backend back2 = new Backend(2, "a", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertNotEquals(back1, back2);

        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "b", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertNotEquals(back1, back2);

        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "a", 2);
        back2.updateOnce(1, 1, 1);
        Map<String, String> tagMap = Maps.newHashMap();
        tagMap.put(Tag.TYPE_LOCATION, "l1");
        tagMap.put("compute", "c1");
        back2.setTagMap(tagMap);
        Assert.assertNotEquals(back1, back2);

        Assert.assertTrue(back1.toString().contains("tags: {location=default}"));
        Assert.assertEquals("{\"compute\" : \"c1\", \"location\" : \"l1\"}", back2.getTagMapString());

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }

}
