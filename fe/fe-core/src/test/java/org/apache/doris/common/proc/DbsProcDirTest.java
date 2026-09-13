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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class DbsProcDirTest {
    private Database db1;
    private Database db2;
    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);

    GlobalTransactionMgr transactionMgr = Mockito.mock(GlobalTransactionMgr.class);

    // construct test case
    //  catalog
    //  | - db1
    //  | - db2

    @BeforeEach
    public void setUp() {
        db1 = new Database(10000L, "db1");
        db2 = new Database(10001L, "db2");
    }

    @AfterEach
    public void tearDown() {
        env = null;
    }

    @Test
    public void testRegister() {
        DbsProcDir dir;

        dir = new DbsProcDir(env, catalog);
        Assertions.assertFalse(dir.register("db1", new BaseProcDir()));
    }

    @Test
    public void testLookupNormal() throws AnalysisException {
        Assertions.assertThrows(AnalysisException.class, () -> {
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("db1")).thenReturn(db1);
            Mockito.when(catalog.getDbNullable("db2")).thenReturn(db2);
            Mockito.when(catalog.getDbNullable("db3")).thenReturn(null);
            Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(null);
            Mockito.when(catalog.getDbNullable(db1.getId())).thenReturn(db1);
            Mockito.when(catalog.getDbNullable(db2.getId())).thenReturn(db2);

            DbsProcDir dir;
            ProcNodeInterface node;

            dir = new DbsProcDir(env, catalog);
            try {
                node = dir.lookup(String.valueOf(db1.getId()));
                Assertions.assertNotNull(node);
                Assertions.assertTrue(node instanceof TablesProcDir);
            } catch (AnalysisException e) {
                Assertions.fail();
            }

            dir = new DbsProcDir(env, catalog);
            try {
                node = dir.lookup(String.valueOf(db2.getId()));
                Assertions.assertNotNull(node);
                Assertions.assertTrue(node instanceof TablesProcDir);
            } catch (AnalysisException e) {
                Assertions.fail();
            }

            dir = new DbsProcDir(env, catalog);
            node = dir.lookup("10002");
            Assertions.assertNull(node);
        });
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
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getGlobalTransactionMgr()).thenReturn(transactionMgr);
        Mockito.when(transactionMgr.getRunningTxnNums(db1.getId())).thenReturn(10);
        Mockito.when(transactionMgr.getRunningTxnNums(db2.getId())).thenReturn(20);
        Mockito.when(catalog.getDbNames()).thenReturn(Lists.newArrayList("db1", "db2"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db1);
        Mockito.when(catalog.getDbNullable("db2")).thenReturn(db2);
        Mockito.when(catalog.getDbNullable("db3")).thenReturn(null);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(null);
        Mockito.when(catalog.getDbNullable(db1.getId())).thenReturn(db1);
        Mockito.when(catalog.getDbNullable(db2.getId())).thenReturn(db2);

        DbsProcDir dir;
        ProcResult result;

        dir = new DbsProcDir(env, catalog);
        result = dir.fetchResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);

        Assertions.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Size", "Quota",
                "LastConsistencyCheckTime", "ReplicaCount", "ReplicaQuota", "RunningTransactionNum", "TransactionQuota",
                "LastUpdateTime"), result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Arrays.asList(String.valueOf(db1.getId()), db1.getFullName(), "0", "0.000 ", "8388608.000 TB",
                FeConstants.null_string, "0", "1073741824", "10", String.valueOf(Config.max_running_txn_num_per_db), FeConstants.null_string));
        rows.add(Arrays.asList(String.valueOf(db2.getId()), db2.getFullName(), "0", "0.000 ", "8388608.000 TB",
                FeConstants.null_string, "0", "1073741824", "20", String.valueOf(Config.max_running_txn_num_per_db), FeConstants.null_string));
        Assertions.assertEquals(rows, result.getRows());
    }

    @Test
    public void testFetchResultInvalid() throws AnalysisException {
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNames()).thenReturn(null);

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
        Assertions.assertEquals(Lists.newArrayList("DbId", "DbName", "TableNum", "Size", "Quota",
                "LastConsistencyCheckTime", "ReplicaCount", "ReplicaQuota", "RunningTransactionNum", "TransactionQuota",
                "LastUpdateTime"),
                result.getColumnNames());
        List<List<String>> rows = Lists.newArrayList();
        Assertions.assertEquals(rows, result.getRows());
    }

    @Test
    public void testListTableNameFailed() throws AnalysisException {
        ExternalCatalog ctlg = Mockito.mock(ExternalCatalog.class);
        Mockito.when(ctlg.getDbNames()).thenReturn(Lists.newArrayList("db1"));

        ExternalDatabase mockDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(mockDb.getId()).thenReturn(3L);
        Mockito.when(mockDb.getTables()).thenThrow(new RuntimeException("list table failed"));
        Mockito.doReturn(mockDb).when(ctlg).getDbNullable("db1");

        DbsProcDir dbsProcDir = new DbsProcDir(env, ctlg);
        ProcResult procResult = dbsProcDir.fetchResult();
        List<List<String>> rows = procResult.getRows();
        Assertions.assertEquals(1, rows.size());
        List<String> strings = rows.get(0);
        Assertions.assertEquals("3", strings.get(0));  // id
        Assertions.assertEquals("db1", strings.get(1)); // name
        Assertions.assertEquals("-1", strings.get(2)); // tableNum
    }
}
