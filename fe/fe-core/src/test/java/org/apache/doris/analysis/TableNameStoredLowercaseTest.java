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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.UUID;

public class TableNameStoredLowercaseTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID() + "/";
    private static DorisAssert dorisAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Config.lower_case_table_names = 1;
        Config.enable_batch_delete_by_default = true;
        Config.enable_http_server_v2 = false;
        UtFrameUtils.createDorisCluster(runningDir);
        String table1 = "CREATE TABLE db1.TABLE1 (\n"
                + "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n"
                + "  `citycode` smallint(6) NULL COMMENT \"\",\n"
                + "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n"
                + "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`siteid`, `citycode`, `username`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")";
        String table2 = "create table db1.TABLE2(k1 int, k2 varchar(32), k3 varchar(32), k4 int, k5 largeint) "
                + "AGGREGATE KEY(k1, k2,k3,k4,k5) distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(table1)
                .withTable(table2);
    }

    @Test
    public void testGlobalVariable() {
        Assert.assertEquals(1, GlobalVariable.lowerCaseTableNames);
    }

    @Test
    public void testTableNameLowerCase() {
        Set<String> tableNames = Env.getCurrentInternalCatalog().getDbNullable("db1")
                .getTableNamesWithLock();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("table1"));
        Assert.assertTrue(tableNames.contains("table2"));
        Assert.assertFalse(tableNames.contains("TABLE1"));
    }

    @Test
    public void testQueryTableNameCaseInsensitive() throws Exception {
        String sql1 = "select Table1.siteid, Table2.k2 from table1 join table2 on TAble1.siteid = TAble2.k1"
                + " where TABle2.k5 > 1000 order by TABLe1.siteid";
        dorisAssert.query(sql1).explainQuery();

        String sql2 = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = "
                + "'ech_dw' ORDER BY routines.ROUTINE_SCHEMA";
        dorisAssert.query(sql2).explainQuery();
    }

    @Test
    public void testQueryTableAliasCaseInsensitive() throws Exception {
        String sql1 = "select T1.siteid, t2.k2 from table1 T1 join table2 T2 on t1.siteid = t2.k1"
                + " where T2.k5 > 1000 order by t1.siteid";
        dorisAssert.query(sql1).explainQuery();

        String sql2 = "select t.siteid, T.username from (select * from Table1) T";
        dorisAssert.query(sql2).explainQuery();
    }

    @Test
    public void testCreateSameTableFailed() {
        String table2 = "create table db1.TABle2(k1 int, k2 varchar(32), k3 varchar(32)) "
                + "AGGREGATE KEY(k1, k2, k3) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        try {
            dorisAssert.withTable(table2);
            Assert.fail("The table name is case insensitive, "
                    + "but the tables 'TABLE2' and 'table2' were successfully created");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        String view2 = "create view table2 as select * from TABLE2";
        try {
            dorisAssert.withView(view2);
            Assert.fail("The table name is case insensitive, "
                    + "but the table 'TABLE2' and view 'table2' were successfully created");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
