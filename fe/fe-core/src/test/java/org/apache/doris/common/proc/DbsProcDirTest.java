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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import mockit.Expectations;
import mockit.Mocked;

public class DbsProcDirTest {
    private Database db1;
    private Database db2;
    @Mocked
    private Catalog catalog;

    // construct test case
    //  catalog
    //  | - db1
    //  | - db2

    @Before
    public void setUp() {
        db1 = new Database(10000L, "db1");
        db2 = new Database(10001L, "db2");
    }

    @After
    public void tearDown() {
        catalog = null;
    }

    @Test
    public void testRegister() {
        DbsProcDir dir;

        dir = new DbsProcDir(catalog);
        Assert.assertFalse(dir.register("db1", new BaseProcDir()));
    }

    @Test(expected = AnalysisException.class)
    public void testLookupNormal() throws AnalysisException {
        new Expectations(catalog) {
            {
                catalog.getDbNullable("db1");
                minTimes = 0;
                result = db1;

                catalog.getDbNullable("db2");
                minTimes = 0;
                result = db2;

                catalog.getDbNullable("db3");
                minTimes = 0;
                result = null;

                catalog.getDbNullable(db1.getId());
                minTimes = 0;
                result = db1;

                catalog.getDbNullable(db2.getId());
                minTimes = 0;
                result = db2;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcNodeInterface node;

        dir = new DbsProcDir(catalog);
        try {
            node = dir.lookup(String.valueOf(db1.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }


        dir = new DbsProcDir(catalog);
        try {
            node = dir.lookup(String.valueOf(db2.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new DbsProcDir(catalog);
        node = dir.lookup("10002");
        Assert.assertNull(node);
    }

    @Test
    public void testLookupInvalid() {
        DbsProcDir dir;
        ProcNodeInterface node;

        dir = new DbsProcDir(catalog);
        try {
            node = dir.lookup(null);
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            node = dir.lookup("");
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testFetchResultNormal() throws AnalysisException {
        new Expectations(catalog) {
            {
                catalog.getDbNames();
                minTimes = 0;
                result = Lists.newArrayList("db1", "db2");

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = db1;

                catalog.getDbNullable("db2");
                minTimes = 0;
                result = db2;

                catalog.getDbNullable("db3");
                minTimes = 0;
                result = null;

                catalog.getDbNullable(db1.getId());
                minTimes = 0;
                result = db1;

                catalog.getDbNullable(db2.getId());
                minTimes = 0;
                result = db2;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(catalog);
        result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Quota", "LastConsistencyCheckTime", "ReplicaQuota"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList(String.valueOf(db1.getId()), db1.getFullName(), "0", "1024.000 TB", FeConstants.null_string, "1073741824"));
        rows.add(Arrays.asList(String.valueOf(db2.getId()), db2.getFullName(), "0", "1024.000 TB", FeConstants.null_string, "1073741824"));
        Assert.assertEquals(rows, result.getRows());
    }

    @Test
    public void testFetchResultInvalid() throws AnalysisException {
        new Expectations(catalog) {
            {
                catalog.getDbNames();
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(null);
        try {
            result = dir.fetchResult();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }

        dir = new DbsProcDir(catalog);
        result = dir.fetchResult();
        Assert.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Quota", "LastConsistencyCheckTime", "ReplicaQuota"),
                            result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        Assert.assertEquals(rows, result.getRows());
    }
}
