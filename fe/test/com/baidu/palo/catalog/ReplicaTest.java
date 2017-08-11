// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.catalog.Replica.ReplicaState;

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

public class ReplicaTest {
    
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
        replica = new Replica(replicaId, backendId, version, versionHash, dataSize, rowCount, ReplicaState.NORMAL);
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
        replica.updateInfo(newVersion, newVersionHash, newDataSize, newRowCount);
        Assert.assertEquals(newVersion, replica.getVersion());
        Assert.assertEquals(newVersionHash, replica.getVersionHash());
        Assert.assertEquals(newDataSize, replica.getDataSize());
        Assert.assertEquals(newRowCount, replica.getRowCount());

        // check version catch up
        Assert.assertFalse(replica.checkVersionCatchUp(5, 98765));
        Assert.assertFalse(replica.checkVersionCatchUp(newVersion, 76543));
        Assert.assertTrue(replica.checkVersionCatchUp(newVersion, newVersionHash));
    }
    
    @Test
    public void toStringTest() {
        StringBuffer strBuffer = new StringBuffer("replicaId=");
        strBuffer.append(replicaId);
        strBuffer.append(", BackendId=");
        strBuffer.append(backendId);
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", versionHash=");
        strBuffer.append(versionHash);
        strBuffer.append(", dataSize=");
        strBuffer.append(dataSize);
        strBuffer.append(", rowCount=");
        strBuffer.append(rowCount);

        Assert.assertEquals(strBuffer.toString(), replica.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./olapReplicaTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        List<Replica> list1 = new ArrayList<Replica>();
        List<Replica> list2 = new ArrayList<Replica>();
        for (int count = 0; count < 10; ++count) {
            Replica olapReplica = new Replica(100L * count, 100L * count, 100L * count, 100L * count,
                                              100L * count, 100 * count, ReplicaState.NORMAL);
            list1.add(olapReplica);
            olapReplica.write(dos);
        }
        
        Replica replica = new Replica(10L, 20L, null);
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
            Assert.assertTrue(list1.get(i).toString().equals(list2.get(i).toString()));
        }
        
        Assert.assertTrue(list1.get(1).equals(list1.get(1)));
        Assert.assertFalse(list1.get(1).equals(list1));
        
        dis.close();
        file.delete();
    }
}

