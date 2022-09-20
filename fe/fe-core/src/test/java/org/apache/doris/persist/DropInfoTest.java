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

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DropInfoTest {
    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get("./BatchRemoveTransactionOperationTest"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        DropInfo info1 = new DropInfo();
        info1.write(dos);

        DropInfo info2 = new DropInfo(1, 2, -1, true);
        info2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        DropInfo rInfo1 = DropInfo.read(dis);
        Assert.assertEquals(rInfo1, info1);

        DropInfo rInfo2 = DropInfo.read(dis);
        Assert.assertEquals(rInfo2, info2);

        Assert.assertEquals(1, rInfo2.getDbId());
        Assert.assertEquals(2, rInfo2.getTableId());
        Assert.assertTrue(rInfo2.isForceDrop());

        Assert.assertEquals(rInfo2, rInfo2);
        Assert.assertNotEquals(rInfo2, this);
        Assert.assertNotEquals(info2, new DropInfo(0, 2, -1L, true));
        Assert.assertNotEquals(info2, new DropInfo(1, 0, -1L, true));
        Assert.assertNotEquals(info2, new DropInfo(1, 2, -1L, false));
        Assert.assertEquals(info2, new DropInfo(1, 2, -1L, true));

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
