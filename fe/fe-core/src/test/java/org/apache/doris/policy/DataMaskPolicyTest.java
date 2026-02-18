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

package org.apache.doris.policy;

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for data mask policy.
 **/
public class DataMaskPolicyTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table table1\n"
                + "(k1 int, k2 varchar(100), k3 varchar(100)) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        // create user
        addUser("tom", true);
        addUser("jack", true);
        createRole("role1", true);
        createRole("role2", true);
        grantPriv("GRANT SELECT_PRIV ON *.*.* TO 'tom'@'%'");
        grantPriv("GRANT SELECT_PRIV ON *.*.* TO 'jack'@'%'");
    }

    @Test
    public void testParseDataMask() {
        NereidsParser parser = new NereidsParser();
        parser.parseSingle("create data mask policy test on internal.test.table1.k1 to jack using MASK_NULL");
        parser.parseSingle("show data mask policy");
        parser.parseSingle("show data mask policy for jack");
        parser.parseSingle("show data mask policy for role role1");
        parser.parseSingle("drop data mask policy test");
    }

    @Test
    public void testDataMaskPolicyForDDL() throws Exception {
        useUser("root");
        //case 1 : add one policy
        String sql = "create data mask policy test on internal.test.table1.k1 to jack using MASK_NULL";
        executeSql(sql);
        /*
          +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
          | PolicyName | CatalogName | DbName | TableName | ColumnName | DataMaskType | DataMaskDef           | User | Role |
          +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
          | test       | internal    | test   | table1    | k1         | MASK_NULL    | NUll                  | jack | NULL |
          +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
         */
        List<List<String>> resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        Assertions.assertEquals("jack", resultRows.get(0).get(7));
        //case 2: and another policy
        sql = "create data mask policy test1 on internal.test.table1.k2 to jack using MASK_HASH";
        executeSql(sql);
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(2, resultRows.size());
        for (List<String> resultRow : resultRows) {
            Assertions.assertEquals("jack", resultRow.get(7));
        }
        // case 3: and same as case 2 policy. should throw exception
        sql = "create data mask policy test2 on internal.test.table1.k2 to jack using MASK_HASH";
        String finalSql = sql;
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> executeSql(finalSql));
        Assertions.assertEquals("errCode = 2, detailMessage = The policy name already exists or a policy of the same type already exists", exception.getMessage());
        //case 3: drop a policy
        sql = "drop data mask policy test1";
        executeSql(sql);
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        //case 4: add policy for role
        sql = "create data mask policy test3 on internal.test.table1.k2 to role role1 using MASK_HASH";
        executeSql(sql);
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        Assertions.assertEquals("jack", resultRows.get(0).get(7));
        resultRows = showDataMaskPolicy(null, "role1").getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        Assertions.assertEquals("role1", resultRows.get(0).get(8));
        resultRows = showDataMaskPolicy(null, null).getResultRows();
        Assertions.assertEquals(2, resultRows.size());

        //case 5: add policy same as case 4. should throw exception
        sql = "create data mask policy test4 on internal.test.table1.k2 to role role1 using MASK_HASH";
        String finalSql1 = sql;
        exception = Assertions.assertThrows(IllegalStateException.class, () -> executeSql(finalSql1));
        Assertions.assertEquals("errCode = 2, detailMessage = The policy name already exists or a policy of the same type already exists", exception.getMessage());

        sql = "drop data mask policy test";
        executeSql(sql);

        sql = "drop data mask policy test3";
        executeSql(sql);

        resultRows = showDataMaskPolicy(null, null).getResultRows();
        Assertions.assertEquals(0, resultRows.size());

        //case 6 : add one policy with priority
        sql = "create data mask policy test on internal.test.table1.k1 to jack using MASK_NULL level 0";
        executeSql(sql);
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        Assertions.assertEquals("0", resultRows.get(0).get(9));

        //case 7 : add one policy with same priority should throw exception
        sql = "create data mask policy test1 on internal.test.table1.k1 to jack using MASK_NULL level 1";
        String finalSql2 = sql;
        exception = Assertions.assertThrows(IllegalStateException.class, () -> executeSql(finalSql2));
        Assertions.assertEquals("errCode = 2, detailMessage = The policy name already exists or a policy of the same type already exists", exception.getMessage());

        //case 8 : add one policy with different priority
        sql = "create data mask policy test1 on internal.test.table1.k1 to jack using MASK_HASH level 1";
        executeSql(sql);
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(2, resultRows.size());

        sql = "drop data mask policy test";
        executeSql(sql);

        sql = "drop data mask policy test1";
        executeSql(sql);

        resultRows = showDataMaskPolicy(null, null).getResultRows();
        Assertions.assertEquals(0, resultRows.size());

        //case 9 : add one policy with priority
        sql = "create data mask policy test on internal.test.table1.k1 to role role1 using MASK_NULL level 0";
        executeSql(sql);
        resultRows = showDataMaskPolicy(null, "role1").getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        Assertions.assertEquals("0", resultRows.get(0).get(9));

        //case 10 : add one policy with same priority should throw exception
        sql = "create data mask policy test1 on internal.test.table1.k1 to role role1 using MASK_NULL level 1";
        String finalSql3 = sql;
        exception = Assertions.assertThrows(IllegalStateException.class, () -> executeSql(finalSql3));
        Assertions.assertEquals("errCode = 2, detailMessage = The policy name already exists or a policy of the same type already exists", exception.getMessage());

        //case 11 : add one policy with different priority
        sql = "create data mask policy test1 on internal.test.table1.k1 to role role1 using MASK_HASH level 1";
        executeSql(sql);
        resultRows = showDataMaskPolicy(null, "role1").getResultRows();
        Assertions.assertEquals(2, resultRows.size());

        //case 12 : add one policy with not exists column
        sql = "create data mask policy test1 on test.table1.k4 to role role1 using MASK_HASH level 1";
        String finalSql4 = sql;
        exception = Assertions.assertThrows(IllegalStateException.class, () -> executeSql(finalSql4));
        Assertions.assertEquals("errCode = 2, detailMessage = column not exist: k4", exception.getMessage());

        sql = "drop data mask policy test";
        executeSql(sql);

        sql = "drop data mask policy test1";
        executeSql(sql);

        resultRows = showDataMaskPolicy(null, null).getResultRows();
        Assertions.assertEquals(0, resultRows.size());
    }

    @Test
    public void testDataMaskForQuery() throws Exception {
        useUser("root");
        String sql = "create data mask policy test on internal.test.table1.k1 to jack using MASK_HASH";
        executeSql(sql);
        sql = "create data mask policy test1 on internal.test.table1.k2 to jack using MASK_DEFAULT";
        executeSql(sql);
        sql = "create data mask policy test2 on internal.test.table1.k2 to jack using MASK_NONE level 1";
        executeSql(sql);
        useUser("jack");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("hex(sha2(CAST(k1[#0] AS varchar(65533)), 256))"));
        sql = "drop data mask policy test";
        useUser("root");
        executeSql(sql);
        sql = "drop data mask policy test1";
        executeSql(sql);
        sql = "drop data mask policy test2";
        executeSql(sql);
    }
}
