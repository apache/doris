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
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class, MetricRepo.class })
public class BackendTest {
    private Backend backend;
    private long backendId = 9999;
    private String host = "myhost";
    private int heartbeatPort = 21234;
    private int bePort = 21235;
    private int httpPort = 21237;
    private int beRpcPort = 21238;

    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);

        backend = new Backend(backendId, host, heartbeatPort);
        backend.updateOnce(bePort, httpPort, beRpcPort);

        PowerMock.mockStatic(MetricRepo.class);
        MetricRepo.generateCapacityMetrics();
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(MetricRepo.class);
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
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        TDisk disk3 = new TDisk("/data3/", 3000, 600, false);

        diskInfos.put(disk1.getRoot_path(), disk1);
        diskInfos.put(disk2.getRoot_path(), disk2);
        diskInfos.put(disk3.getRoot_path(), disk3);

        // first update
        backend.updateDisks(diskInfos);
        Assert.assertEquals(disk1.getDisk_total_capacity() + disk2.getDisk_total_capacity(),
                            backend.getTotalCapacityB());
        Assert.assertEquals(1, backend.getAvailableCapacityB());

        // second update
        diskInfos.remove(disk1.getRoot_path());
        backend.updateDisks(diskInfos);
        Assert.assertEquals(disk2.getDisk_total_capacity(), backend.getTotalCapacityB());
        Assert.assertEquals(disk2.getDisk_available_capacity() + 1, backend.getAvailableCapacityB());
    }

    @Test
    public void testSerialization() throws Exception {
        // Write 100 objects to file 
        File file = new File("./backendTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        List<Backend> list1 = new LinkedList<Backend>();
        List<Backend> list2 = new LinkedList<Backend>();
        
        for (int count = 0; count < 100; ++count) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            list1.add(backend);
        }
        for (int count = 100; count < 200; count++) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            list1.add(backend);
        }
        for (Backend backend : list1) {
            backend.write(dos);
        }
        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        for (int count = 0; count < 100; ++count) {
            Backend backend = new Backend();
            backend.readFields(dis);
            list2.add(backend);
            Assert.assertEquals(count, backend.getId());
            Assert.assertEquals("10.120.22.32" + count, backend.getHost());
        }
        
        for (int count = 100; count < 200; ++count) {
            Backend backend = Backend.read(dis);
            list2.add(backend);
            Assert.assertEquals(count, backend.getId());
            Assert.assertEquals("10.120.22.32" + count, backend.getHost());
        }
        
        for (int count = 0; count < 200; count++) {
            Assert.assertTrue(list1.get(count).equals(list2.get(count)));
        }
        Assert.assertFalse(list1.get(1).equals(list1.get(2)));
        Assert.assertFalse(list1.get(1).equals(this));
        Assert.assertTrue(list1.get(1).equals(list1.get(1)));
        
        Backend back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        Backend back2 = new Backend(2, "a", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));
        
        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "b", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));
        
        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "a", 2);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));
        
        Assert.assertEquals("Backend [id=1, host=a, heartbeatPort=1, alive=true]", back1.toString());
        
        // 3. delete files
        dis.close();
        file.delete();
    }
    
}
