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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class DropDbInfoTest {
    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./dropDbInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        DropDbInfo info1 = new DropDbInfo();
        info1.write(dos);

        DropDbInfo info2 = new DropDbInfo("test_db", true);
        info2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        DropDbInfo rInfo1 = DropDbInfo.read(dis);
        Assert.assertTrue(rInfo1.equals(info1));

        DropDbInfo rInfo2 = DropDbInfo.read(dis);
        Assert.assertTrue(rInfo2.equals(info2));

        Assert.assertEquals("test_db", rInfo2.getDbName());
        Assert.assertTrue(rInfo2.isForceDrop());

        Assert.assertTrue(rInfo2.equals(rInfo2));
        Assert.assertFalse(rInfo2.equals(this));
        Assert.assertFalse(info2.equals(new DropDbInfo("test_db1", true)));
        Assert.assertFalse(info2.equals(new DropDbInfo("test_db", false)));
        Assert.assertTrue(info2.equals(new DropDbInfo("test_db", true)));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
