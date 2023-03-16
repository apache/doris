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

package org.apache.doris.common.util;

import org.apache.doris.analysis.CreateFileStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SmallFileMgrTest {

    @Mocked
    Env env;
    @Mocked
    InternalCatalog catalog;
    @Mocked
    EditLog editLog;
    @Mocked
    Database db;

    @Ignore // Could not find a way to mock a private method
    @Test
    public void test(@Injectable CreateFileStmt stmt1, @Injectable CreateFileStmt stmt2) throws DdlException {
        new Expectations() {
            {
                db.getId();
                minTimes = 0;
                result = 1L;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = db;
                stmt1.getDbName();
                minTimes = 0;
                result = "db1";
                stmt1.getFileName();
                minTimes = 0;
                result = "file1";
                stmt1.getCatalogName();
                minTimes = 0;
                result = "kafka";
                stmt1.getDownloadUrl();
                minTimes = 0;
                result = "http://127.0.0.1:8001/file1";

                stmt2.getDbName();
                minTimes = 0;
                result = "db1";
                stmt2.getFileName();
                minTimes = 0;
                result = "file2";
                stmt2.getCatalogName();
                minTimes = 0;
                result = "kafka";
                stmt2.getDownloadUrl();
                minTimes = 0;
                result = "http://127.0.0.1:8001/file2";
            }
        };

        SmallFile smallFile = new SmallFile(1L, "kafka", "file1", 10001L, "ABCD", 12, "12345", true);
        final SmallFileMgr smallFileMgr = new SmallFileMgr();
        new Expectations(smallFileMgr) {
            {
                Deencapsulation.invoke(smallFileMgr, "downloadAndCheck", anyLong, anyString, anyString, anyString,
                        anyString, anyBoolean);
                result = smallFile;
            }
        };

        // 1. test create
        try {
            smallFileMgr.createFile(stmt1);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        Assert.assertTrue(smallFileMgr.containsFile(1L, "kafka", "file1"));
        SmallFile gotFile = smallFileMgr.getSmallFile(1L, "kafka", "file1", true);
        Assert.assertEquals(10001L, gotFile.id);
        gotFile = smallFileMgr.getSmallFile(10001L);
        Assert.assertEquals(10001L, gotFile.id);

        // 2. test file num limit
        Config.max_small_file_number = 1;
        boolean fail = false;
        try {
            smallFileMgr.createFile(stmt2);
        } catch (DdlException e) {
            fail = true;
            Assert.assertTrue(e.getMessage().contains("File number exceeds limit"));
        }
        Assert.assertTrue(fail);

        // 3. test remove
        try {
            smallFileMgr.removeFile(2L, "kafka", "file1", true);
        } catch (DdlException e) {
            // this is expected
        }
        gotFile = smallFileMgr.getSmallFile(10001L);
        Assert.assertEquals(10001L, gotFile.id);
        smallFileMgr.removeFile(1L, "kafka", "file1", true);
        gotFile = smallFileMgr.getSmallFile(10001L);
        Assert.assertNull(gotFile);

        // 4. test file limit again
        try {
            smallFileMgr.createFile(stmt1);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        gotFile = smallFileMgr.getSmallFile(10001L);
        Assert.assertEquals(10001L, gotFile.id);
    }

}
