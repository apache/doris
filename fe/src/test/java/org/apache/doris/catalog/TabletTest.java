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

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.FeConstants;

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

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class })
public class TabletTest {

    private Tablet tablet;
    private Replica replica1;
    private Replica replica2;
    private Replica replica3;

    private TabletInvertedIndex invertedIndex;

    @Before 
    public void makeTablet() {
        invertedIndex = new TabletInvertedIndex();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        EasyMock.expect(Catalog.isCheckpointThread()).andReturn(false).anyTimes();
        PowerMock.replay(Catalog.class);

        tablet = new Tablet(1);
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1);
        invertedIndex.addTablet(1, tabletMeta);
        replica1 = new Replica(1L, 1L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        replica2 = new Replica(2L, 2L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        replica3 = new Replica(3L, 3L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(replica1, tablet.getReplicaById(replica1.getId()));
        Assert.assertEquals(replica2, tablet.getReplicaById(replica2.getId()));
        Assert.assertEquals(replica3, tablet.getReplicaById(replica3.getId()));

        Assert.assertEquals(3, tablet.getReplicas().size());
        Assert.assertEquals(replica1, tablet.getReplicaByBackendId(replica1.getBackendId()));
        Assert.assertEquals(replica2, tablet.getReplicaByBackendId(replica2.getBackendId()));
        Assert.assertEquals(replica3, tablet.getReplicaByBackendId(replica3.getBackendId()));
        
        
        long newTabletId = 20000;
        tablet.setTabletId(newTabletId);
        Assert.assertEquals("tabletId=" + newTabletId, tablet.toString());
    }

    @Test
    public void deleteReplicaTest() {
        // delete replica1
        Assert.assertTrue(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assert.assertNull(tablet.getReplicaById(replica1.getId()));

        // err: re-delete replica1
        Assert.assertFalse(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assert.assertFalse(tablet.deleteReplica(replica1));
        Assert.assertNull(tablet.getReplicaById(replica1.getId()));

        // delete replica2
        Assert.assertTrue(tablet.deleteReplica(replica2));
        Assert.assertEquals(1, tablet.getReplicas().size());

        // clear replicas
        tablet.clearReplica();
        Assert.assertEquals(0, tablet.getReplicas().size());
    }
        
    @Test
    public void testSerialization() throws Exception {
        File file = new File("./olapTabletTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        tablet.write(dos);
        dos.flush();
        dos.close();
        
        // 2. Read a object from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Tablet rTablet1 = Tablet.read(dis);
        Assert.assertEquals(1, rTablet1.getId());
        Assert.assertEquals(3, rTablet1.getReplicas().size());
        Assert.assertEquals(rTablet1.getReplicas().get(0).getVersion(), rTablet1.getReplicas().get(1).getVersion());
        Assert.assertEquals(rTablet1.getReplicas().get(0).getVersionHash(),
                            rTablet1.getReplicas().get(1).getVersionHash());
        
        Assert.assertTrue(rTablet1.equals(tablet));
        Assert.assertTrue(rTablet1.equals(rTablet1));
        Assert.assertFalse(rTablet1.equals(this));
        
        Tablet tablet2 = new Tablet(1);
        Replica replica1 = new Replica(1L, 1L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        Replica replica2 = new Replica(2L, 2L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        Replica replica3 = new Replica(3L, 3L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0);
        tablet2.addReplica(replica1);
        tablet2.addReplica(replica2);
        Assert.assertFalse(tablet2.equals(tablet));
        tablet2.addReplica(replica3);
        Assert.assertTrue(tablet2.equals(tablet));
        
        Tablet tablet3 = new Tablet(1);
        tablet3.addReplica(replica1);
        tablet3.addReplica(replica2);
        tablet3.addReplica(new Replica(4L, 4L, 100L, 0L, 200000L, 3000L, ReplicaState.NORMAL, 0, 0, 0, 0));
        Assert.assertFalse(tablet3.equals(tablet));
        
        dis.close();
        file.delete();
    }

}
