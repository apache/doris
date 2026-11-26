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
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ReplicaTest {

    // replica serialize and deserialize test will use catalog so that it should be mocked
    private Env env = Mockito.mock(Env.class);

    private Replica replica;
    private long replicaId;
    private long backendId;
    private long version;
    private long dataSize;
    private long rowCount;

    @BeforeEach
    public void setUp() {
        replicaId = 10000;
        backendId = 20000;
        version = 2;
        dataSize = 9999;
        rowCount = 1024;
        replica = new LocalReplica(replicaId, backendId, version, 0, dataSize, 0, rowCount, ReplicaState.NORMAL, 0, version);
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(replicaId, replica.getId());
        Assertions.assertEquals(backendId, replica.getBackendIdWithoutException());
        Assertions.assertEquals(version, replica.getVersion());
        Assertions.assertEquals(dataSize, replica.getDataSize());
        Assertions.assertEquals(rowCount, replica.getRowCount());

        // update new version
        long newVersion = version + 1;
        replica.updateVersion(newVersion);

        // check version catch up
        Assertions.assertFalse(replica.checkVersionCatchUp(5, false));
        Assertions.assertTrue(replica.checkVersionCatchUp(newVersion, false));
        Assertions.assertTrue(replica.checkVersionCatchUp(newVersion, false));
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./olapReplicaTest"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        List<Replica> list1 = new ArrayList<Replica>();
        List<Replica> list2 = new ArrayList<Replica>();
        for (int count = 0; count < 10; ++count) {
            Replica olapReplica = new LocalReplica(100L * count, 100L * count, 100L * count, 0,
                                              100L * count, 0,  100 * count, ReplicaState.NORMAL, 0, 100L * count);
            list1.add(olapReplica);
            Text.writeString(dos, GsonUtils.GSON.toJson(olapReplica));
        }

        Replica replica = new LocalReplica(10L, 20L, 0, null);
        list1.add(replica);
        Text.writeString(dos, GsonUtils.GSON.toJson(replica));
        dos.flush();
        dos.close();

        // 2. Read a object from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        for (int count = 0; count < 10; ++count) {
            Replica olapReplica = GsonUtils.GSON.fromJson(Text.readString(dis), Replica.class);
            Assertions.assertEquals(100 * count, olapReplica.getId());
            Assertions.assertEquals(100 * count, olapReplica.getBackendId());
            Assertions.assertEquals(100 * count, olapReplica.getVersion());
            Assertions.assertEquals(100 * count, olapReplica.getDataSize());
            Assertions.assertEquals(100 * count, olapReplica.getRowCount());
            Assertions.assertEquals(Replica.ReplicaState.NORMAL, olapReplica.getState());
            list2.add(olapReplica);
        }
        Replica olapReplica = GsonUtils.GSON.fromJson(Text.readString(dis), Replica.class);
        list2.add(olapReplica);

        // 3. Check equal
        for (int i = 0; i < 11; i++) {
            Assertions.assertEquals(list1.get(i), list2.get(i));
        }

        Assertions.assertEquals(list1.get(1), list1.get(1));
        Assertions.assertNotEquals(list1.get(1), list1);

        dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testUpdateVersion1() {
        Replica originalReplica = new LocalReplica(10000, 20000, 3, 0, 100, 0, 78, ReplicaState.NORMAL, 0, 3);
        // new version is little than original version, it is invalid the version will not update
        originalReplica.updateVersion(2);
        Assertions.assertEquals(3, originalReplica.getVersion());
    }

    @Test
    public void testUpdateVersion2() {
        Replica originalReplica = new LocalReplica(10000, 20000, 3, 0, 100, 0, 78, ReplicaState.NORMAL, 0, 0);
        originalReplica.updateVersion(3);
        // if new version >= current version and last success version <= new version, then last success version should be updated
        Assertions.assertEquals(3, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(3, originalReplica.getVersion());
    }

    @Test
    public void testUpdateVersion3() {
        // version(3) ---> last failed version (8) ---> last success version(10)
        Replica originalReplica = new LocalReplica(10000, 20000, 3, 111, 0, 0, 78, ReplicaState.NORMAL, 0, 0);
        originalReplica.updateLastFailedVersion(8);
        Assertions.assertEquals(3, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(3, originalReplica.getVersion());
        Assertions.assertEquals(8, originalReplica.getLastFailedVersion());

        // update last success version 10
        originalReplica.updateVersionWithFailed(originalReplica.getVersion(),
                originalReplica.getLastFailedVersion(),
                10);
        Assertions.assertEquals(10, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(3, originalReplica.getVersion());
        Assertions.assertEquals(8, originalReplica.getLastFailedVersion());

        // update version to 8, the last success version and version should be 10
        originalReplica.updateVersion(8);
        Assertions.assertEquals(10, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(10, originalReplica.getVersion());
        Assertions.assertEquals(-1, originalReplica.getLastFailedVersion());

        // update last failed version to 12
        originalReplica.updateLastFailedVersion(12);
        Assertions.assertEquals(10, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(10, originalReplica.getVersion());
        Assertions.assertEquals(12, originalReplica.getLastFailedVersion());

        // update last success version to 15
        originalReplica.updateVersionWithFailed(originalReplica.getVersion(),
                originalReplica.getLastFailedVersion(),
                15);
        Assertions.assertEquals(15, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(10, originalReplica.getVersion());
        Assertions.assertEquals(12, originalReplica.getLastFailedVersion());

        // update last failed version to 18
        originalReplica.updateLastFailedVersion(18);
        Assertions.assertEquals(10, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(10, originalReplica.getVersion());
        Assertions.assertEquals(18, originalReplica.getLastFailedVersion());

        // update version to 17 then version and success version is 17
        originalReplica.updateVersion(17);
        Assertions.assertEquals(17, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(17, originalReplica.getVersion());
        Assertions.assertEquals(18, originalReplica.getLastFailedVersion());

        // update version to 18, then version and last success version should be 18 and failed version should be -1
        originalReplica.updateVersion(18);
        Assertions.assertEquals(18, originalReplica.getLastSuccessVersion());
        Assertions.assertEquals(18, originalReplica.getVersion());
        Assertions.assertEquals(-1, originalReplica.getLastFailedVersion());
    }
}
