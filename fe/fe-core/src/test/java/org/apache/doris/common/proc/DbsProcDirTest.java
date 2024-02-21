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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DbsProcDirTest {
    private Database db1;
    private Database db2;
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    @Mocked
    GlobalTransactionMgr transactionMgr;

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
        env = null;
    }

    @Test
    public void testRegister() {
        DbsProcDir dir;

        dir = new DbsProcDir(env, catalog);
        Assert.assertFalse(dir.register("db1", new BaseProcDir()));
    }

    @Test(expected = AnalysisException.class)
    public void testLookupNormal() throws AnalysisException {
        new Expectations(env, catalog) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

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

        dir = new DbsProcDir(env, catalog);
        try {
            node = dir.lookup(String.valueOf(db1.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new DbsProcDir(env, catalog);
        try {
            node = dir.lookup(String.valueOf(db2.getId()));
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof TablesProcDir);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new DbsProcDir(env, catalog);
        node = dir.lookup("10002");
        Assert.assertNull(node);
    }

    @Test
    public void testLookupInvalid() {
        DbsProcDir dir;

        dir = new DbsProcDir(env, catalog);
        try {
            dir.lookup(null);
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            dir.lookup("");
        } catch (AnalysisException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testFetchResultNormal() throws AnalysisException {
        new Expectations(env, catalog) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                env.getGlobalTransactionMgr();
                minTimes = 0;
                result = transactionMgr;

                transactionMgr.getRunningTxnNums(db1.getId());
                minTimes = 0;
                result = 10;

                transactionMgr.getRunningTxnNums(db2.getId());
                minTimes = 0;
                result = 20;

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

        dir = new DbsProcDir(env, catalog);
        result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Size", "Quota",
                "LastConsistencyCheckTime", "ReplicaCount", "ReplicaQuota", "RunningTransactionNum", "TransactionQuota",
                "LastUpdateTime"), result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList(String.valueOf(db1.getId()), db1.getFullName(), "0", "0.000 ", "1024.000 TB",
                FeConstants.null_string, "0", "1073741824", "10", "1000", FeConstants.null_string));
        rows.add(Arrays.asList(String.valueOf(db2.getId()), db2.getFullName(), "0", "0.000 ", "1024.000 TB",
                FeConstants.null_string, "0", "1073741824", "20", "1000", FeConstants.null_string));
        Assert.assertEquals(rows, result.getRows());
    }

    @Test
    public void testFetchResultInvalid() throws AnalysisException {
        new Expectations(env, catalog) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNames();
                minTimes = 0;
                result = null;
            }
        };

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(null, catalog);
        try {
            result = dir.fetchResult();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }

        dir = new DbsProcDir(env, catalog);
        result = dir.fetchResult();
        Assert.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Size", "Quota",
                "LastConsistencyCheckTime", "ReplicaCount", "ReplicaQuota", "RunningTransactionNum", "TransactionQuota",
                "LastUpdateTime"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        Assert.assertEquals(rows, result.getRows());
    }
}
