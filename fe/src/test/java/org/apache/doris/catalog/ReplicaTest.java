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

import static org.junit.Assert.assertEquals;

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.FeMetaVersion;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import mockit.Mocked;
import mockit.NonStrictExpectations;

public class ReplicaTest {
    
    // replica serialize and deserialize test will use catalog so that it should be mocked
    @Mocked
    Catalog catalog;
    
    private Replica replica;
    private long replicaId;
    private long backendId;
    private long version;
    private long versionHash;
    private long dataSize;
    private long rowCount;
    
    
    @Before
    public void setUp() {
        replicaId = 10000;
        backendId = 20000;
        version = 2;
        versionHash = 98765;
        dataSize = 9999;
        rowCount = 1024;
        replica = new Replica(replicaId, backendId, version, versionHash, 0, dataSize, rowCount, ReplicaState.NORMAL, 0,
                0, version, versionHash);
    }
    
    @Test
    public void getMethodTest() {
        Assert.assertEquals(replicaId, replica.getId());
        Assert.assertEquals(backendId, replica.getBackendId());
        Assert.assertEquals(version, replica.getVersion());
        Assert.assertEquals(versionHash, replica.getVersionHash());
        Assert.assertEquals(dataSize, replica.getDataSize());
        Assert.assertEquals(rowCount, replica.getRowCount());

        // update new version
        long newVersion = version + 1;
        long newVersionHash = 87654;
        long newDataSize = dataSize + 100;
        long newRowCount = rowCount + 10;
        replica.updateVersionInfo(newVersion, newVersionHash, newDataSize, newRowCount);
        Assert.assertEquals(newVersion, replica.getVersion());
        Assert.assertEquals(newVersionHash, replica.getVersionHash());
        Assert.assertEquals(newDataSize, replica.getDataSize());
        Assert.assertEquals(newRowCount, replica.getRowCount());

        // check version catch up
        Assert.assertFalse(replica.checkVersionCatchUp(5, 98765, false));
        Assert.assertFalse(replica.checkVersionCatchUp(newVersion, 76543, false));
        Assert.assertTrue(replica.checkVersionCatchUp(newVersion, newVersionHash, false));
    }

    @Test
    public void testSerialization() throws Exception {
        new NonStrictExpectations() {
            {
                Catalog.getCurrentCatalogJournalVersion();
                result = FeMetaVersion.VERSION_45;
            }
        };

        // 1. Write objects to file
        File file = new File("./olapReplicaTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        List<Replica> list1 = new ArrayList<Replica>();
        List<Replica> list2 = new ArrayList<Replica>();
        for (int count = 0; count < 10; ++count) {
            Replica olapReplica = new Replica(100L * count, 100L * count, 100L * count, 100L * count, 0,
                                              100L * count, 100 * count, ReplicaState.NORMAL, 0, 0, 100L * count, 100L * count);
            list1.add(olapReplica);
            olapReplica.write(dos);
        }
        
        Replica replica = new Replica(10L, 20L, 0, null);
        list1.add(replica);
        replica.write(dos);
        dos.flush();
        dos.close();
        
        // 2. Read a object from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        for (int count = 0; count < 10; ++count) {
            Replica olapReplica = new Replica();
            olapReplica.readFields(dis);
            Assert.assertEquals(100 * count, olapReplica.getId());
            Assert.assertEquals(100 * count, olapReplica.getBackendId());
            Assert.assertEquals(100 * count, olapReplica.getVersion());
            Assert.assertEquals(100 * count, olapReplica.getVersionHash());
            Assert.assertEquals(100 * count, olapReplica.getDataSize());
            Assert.assertEquals(100 * count, olapReplica.getRowCount());
            Assert.assertEquals(Replica.ReplicaState.NORMAL, olapReplica.getState());
            list2.add(olapReplica);
        }
        Replica olapReplica = new Replica();
        olapReplica.readFields(dis);
        list2.add(olapReplica);
        
        // 3. Check equal
        for (int i = 0; i < 11; i++) {
            Assert.assertTrue(list1.get(i).equals(list2.get(i)));
        }
        
        Assert.assertTrue(list1.get(1).equals(list1.get(1)));
        Assert.assertFalse(list1.get(1).equals(list1));
        
        dis.close();
        file.delete();
    }
    
    @Test
    public void testUpdateVersion1() {
        Replica originalReplica = new Replica(10000, 20000, 3, 1231, 0, 100, 78, ReplicaState.NORMAL, 0, 0, 3, 1231);
        // new version is little than original version, it is invalid the version will not update
        originalReplica.updateVersionInfo(2, 111, 100, 78);
        assertEquals(3, originalReplica.getVersion());
        assertEquals(1231, originalReplica.getVersionHash());
    }
    
    @Test
    public void testUpdateVersion2() {
        Replica originalReplica = new Replica(10000, 20000, 3, 1231, 0, 100, 78, ReplicaState.NORMAL, 0, 0, 0, 0);
        originalReplica.updateVersionInfo(3, 111, 100, 78);
        // if new version >= current version and last success version <= new version, then last success version should be updated
        assertEquals(3, originalReplica.getLastSuccessVersion());
        assertEquals(111, originalReplica.getLastSuccessVersionHash());
        assertEquals(3, originalReplica.getVersion());
        assertEquals(111, originalReplica.getVersionHash());
    }
    
    @Test
    public void testUpdateVersion3() {
        // version(3) ---> last failed version (8) ---> last success version(10)
        Replica originalReplica = new Replica(10000, 20000, 3, 111, 0, 100, 78, ReplicaState.NORMAL, 0, 0, 0, 0);
        originalReplica.updateLastFailedVersion(8, 100);
        assertEquals(3, originalReplica.getLastSuccessVersion());
        assertEquals(111, originalReplica.getLastSuccessVersionHash());
        assertEquals(3, originalReplica.getVersion());
        assertEquals(111, originalReplica.getVersionHash());
        assertEquals(8, originalReplica.getLastFailedVersion());
        assertEquals(100, originalReplica.getLastFailedVersionHash());
        
        // update last success version 10
        originalReplica.updateVersionInfo(originalReplica.getVersion(), 
                originalReplica.getVersionHash(), originalReplica.getLastFailedVersion(), 
                originalReplica.getLastFailedVersionHash(), 
                10, 1210);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(1210, originalReplica.getLastSuccessVersionHash());
        assertEquals(3, originalReplica.getVersion());
        assertEquals(111, originalReplica.getVersionHash());
        assertEquals(8, originalReplica.getLastFailedVersion());
        assertEquals(100, originalReplica.getLastFailedVersionHash());
        
        // update version to 8, the last success version and version should be 10
        originalReplica.updateVersionInfo(8, 100, 100, 78);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(1210, originalReplica.getLastSuccessVersionHash());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(1210, originalReplica.getVersionHash());
        assertEquals(-1, originalReplica.getLastFailedVersion());
        assertEquals(0, originalReplica.getLastFailedVersionHash());
        
        // update last failed version to 12
        originalReplica.updateLastFailedVersion(12, 1212);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(1210, originalReplica.getLastSuccessVersionHash());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(1210, originalReplica.getVersionHash());
        assertEquals(12, originalReplica.getLastFailedVersion());
        assertEquals(1212, originalReplica.getLastFailedVersionHash());
        
        // update last success version to 15
        originalReplica.updateVersionInfo(originalReplica.getVersion(), 
                originalReplica.getVersionHash(), originalReplica.getLastFailedVersion(), 
                originalReplica.getLastFailedVersionHash(), 
                15, 1215);
        assertEquals(15, originalReplica.getLastSuccessVersion());
        assertEquals(1215, originalReplica.getLastSuccessVersionHash());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(1210, originalReplica.getVersionHash());
        assertEquals(12, originalReplica.getLastFailedVersion());
        assertEquals(1212, originalReplica.getLastFailedVersionHash());
        
        // update last failed version to 18
        originalReplica.updateLastFailedVersion(18, 1218);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(1210, originalReplica.getLastSuccessVersionHash());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(1210, originalReplica.getVersionHash());
        assertEquals(18, originalReplica.getLastFailedVersion());
        assertEquals(1218, originalReplica.getLastFailedVersionHash());
        
        // update version to 17 then version and success version is 17
        originalReplica.updateVersionInfo(17, 1217, 100, 78);
        assertEquals(17, originalReplica.getLastSuccessVersion());
        assertEquals(1217, originalReplica.getLastSuccessVersionHash());
        assertEquals(17, originalReplica.getVersion());
        assertEquals(1217, originalReplica.getVersionHash());
        assertEquals(18, originalReplica.getLastFailedVersion());
        assertEquals(1218, originalReplica.getLastFailedVersionHash());
        
        // update version to 18, then version and last success version should be 18 and failed version should be -1
        originalReplica.updateVersionInfo(18, 1218, 100, 78);
        assertEquals(18, originalReplica.getLastSuccessVersion());
        assertEquals(1218, originalReplica.getLastSuccessVersionHash());
        assertEquals(18, originalReplica.getVersion());
        assertEquals(1218, originalReplica.getVersionHash());
        assertEquals(-1, originalReplica.getLastFailedVersion());
        assertEquals(0, originalReplica.getLastFailedVersionHash());
    }
}

