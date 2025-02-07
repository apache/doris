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

import org.apache.doris.catalog.Database;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.meta.MetaContext;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.Files;

public class CreateDbInfoTest {
    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./createDbInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(file.toPath()));

        Database db = new Database(10000, "db1");
        CreateDbInfo info1 = new CreateDbInfo(InternalCatalog.INTERNAL_CATALOG_NAME, db.getName(), db);
        info1.write(dos);

        CreateDbInfo info2 = new CreateDbInfo("external_catalog", "external_db", null);
        info2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(file.toPath()));

        CreateDbInfo rInfo1 = CreateDbInfo.read(dis);
        Assert.assertEquals(info1.getCtlName(), rInfo1.getCtlName());
        Assert.assertEquals(info1.getDbName(), rInfo1.getDbName());
        Assert.assertEquals(info1.getInternalDb().getId(), rInfo1.getInternalDb().getId());

        CreateDbInfo rInfo2 = CreateDbInfo.read(dis);
        Assert.assertEquals(info2.getCtlName(), rInfo2.getCtlName());
        Assert.assertEquals(info2.getDbName(), rInfo2.getDbName());
        Assert.assertNull(rInfo2.getInternalDb());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
