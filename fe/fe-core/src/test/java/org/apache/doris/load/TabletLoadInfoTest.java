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

package org.apache.doris.load;

import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class TabletLoadInfoTest {
    private FakeCatalog fakeCatalog;
    @Test
    public void testSerialization() throws Exception {
        // mock catalog
        fakeCatalog = new FakeCatalog();
        FakeCatalog.setMetaVersion(FeConstants.meta_version);

        // test
        File file = new File("./tabletLoadInfoTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        TabletLoadInfo tabletLoadInfo0 = new TabletLoadInfo();
        tabletLoadInfo0.write(dos);
        
        TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("hdfs://host:port/dir", 1L);
        tabletLoadInfo.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        TabletLoadInfo rTabletLoadInfo0 = new TabletLoadInfo();
        rTabletLoadInfo0.readFields(dis);
        
        TabletLoadInfo tabletLoadInfo1 = new TabletLoadInfo();
        tabletLoadInfo1.readFields(dis);

        Assert.assertEquals("hdfs://host:port/dir", tabletLoadInfo1.getFilePath());
        Assert.assertEquals(1L, tabletLoadInfo1.getFileSize());
        
        Assert.assertTrue(tabletLoadInfo1.equals(tabletLoadInfo));
        Assert.assertTrue(rTabletLoadInfo0.equals(tabletLoadInfo0));
        Assert.assertFalse(rTabletLoadInfo0.equals(tabletLoadInfo1));
 
        dis.close();
        file.delete();
    }

}
