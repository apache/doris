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

package org.apache.doris.persist;

import org.apache.doris.policy.StoragePolicy;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class StoragePolicyPersistTest {
    @Test
    public void test() throws IOException {
        long cooldownTime = System.currentTimeMillis();
        StoragePolicy storagePolicy = new StoragePolicy(1, "test_policy", "resource1", cooldownTime, "-1", -1);

        // 1. Write objects to file
        File file = new File("./StoregaPolicyPersistTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        storagePolicy.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        StoragePolicy anotherStoragePolicy = StoragePolicy.read(dis);
        Assert.assertEquals(cooldownTime, anotherStoragePolicy.getCooldownTimestampMs());

        StoragePolicy clonePolicy = anotherStoragePolicy.clone();
        Assert.assertEquals(cooldownTime, clonePolicy.getCooldownTimestampMs());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
