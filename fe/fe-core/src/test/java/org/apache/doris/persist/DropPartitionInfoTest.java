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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class DropPartitionInfoTest {
    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./dropPartitionInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        DropPartitionInfo info1 = new DropPartitionInfo(1L, 2L, 3L, "test_partition", false, true, 0, 0L, 0L);
        info1.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        DropPartitionInfo rInfo1 = DropPartitionInfo.read(dis);

        Assertions.assertEquals(Long.valueOf(1L), rInfo1.getDbId());
        Assertions.assertEquals(Long.valueOf(2L), rInfo1.getTableId());
        Assertions.assertEquals(Long.valueOf(3L), rInfo1.getPartitionId());
        Assertions.assertEquals("test_partition", rInfo1.getPartitionName());
        Assertions.assertFalse(rInfo1.isTempPartition());
        Assertions.assertTrue(rInfo1.isForceDrop());

        Assertions.assertEquals(rInfo1, info1);
        Assertions.assertNotEquals(rInfo1, this);
        Assertions.assertNotEquals(info1, new DropPartitionInfo(-1L, 2L, 3L, "test_partition", false, true, 0, 0L, 0L));
        Assertions.assertNotEquals(info1, new DropPartitionInfo(1L, -2L, 3L, "test_partition", false, true, 0, 0L, 0L));
        Assertions.assertNotEquals(info1, new DropPartitionInfo(1L, 2L, 3L, "test_partition1", false, true, 0, 0L, 0L));
        Assertions.assertNotEquals(info1, new DropPartitionInfo(1L, 2L, 3L, "test_partition", true, true, 0, 0L, 0L));
        Assertions.assertNotEquals(info1, new DropPartitionInfo(1L, 2L, 3L, "test_partition", false, false, 0, 0L, 0L));
        Assertions.assertEquals(info1, new DropPartitionInfo(1L, 2L, 3L, "test_partition", false, true, 0, 0L, 0L));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
