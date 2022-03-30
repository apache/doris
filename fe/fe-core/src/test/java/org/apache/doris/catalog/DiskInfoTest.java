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
import org.apache.doris.thrift.TStorageMedium;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiskInfoTest {

    private Catalog catalog;

    private FakeCatalog fakeCatalog;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();

        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();

        FakeCatalog.setCatalog(catalog);
        FakeCatalog.setMetaVersion(FeConstants.meta_version);
        FakeCatalog.setSystemInfo(AccessTestUtil.fetchSystemInfoService());
    }

    @Test
    public void testSerialization() throws IOException {
        // write disk info to file
        File file = new File("./diskInfoTest");
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        DiskInfo diskInfo1 = new DiskInfo("/disk1");
        // 1 GB
        long totalCapacityB = 1024 * 1024 * 1024L;
        diskInfo1.setTotalCapacityB(totalCapacityB);
        // 1 MB
        long dataUsedCapacityB = 1024 * 1024L;
        diskInfo1.setDataUsedCapacityB(dataUsedCapacityB);
        // without serialize
        diskInfo1.setStorageMedium(TStorageMedium.SSD);

        diskInfo1.write(dos);
        dos.flush();
        dos.close();

        // read disk info from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        DiskInfo result = DiskInfo.read(dis);

        // check
        Assert.assertEquals("/disk1", result.getRootPath());
        Assert.assertEquals(totalCapacityB, result.getTotalCapacityB());
        Assert.assertEquals(dataUsedCapacityB, result.getDataUsedCapacityB());
        Assert.assertTrue(result.getStorageMedium() == null);
    }
}
