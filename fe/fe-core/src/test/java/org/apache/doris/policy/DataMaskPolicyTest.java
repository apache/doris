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
                + "(k1 int, k2 varchar(100)) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        // create user
        addUser("tom", true);
        addUser("jack", true);
        addRole("role1", true);
        addRole("role2", true);
        grant("GRANT SELECT_PRIV ON *.*.* TO 'tom'@'%'");
        grant("GRANT SELECT_PRIV ON *.*.* TO 'jack'@'%'");
    }

    @Test
    public void testParseDataMask() {
        NereidsParser parser = new NereidsParser();
        parser.parseSingle("create data mask policy test on internal.test.table1.k1 to jack using MASK_NULL");
        parser.parseSingle("show data mask policy");
        parser.parseSingle("drop data mask policy test");
    }

    @Test
    public void testDataMaskPolicyForDDL() throws Exception {
        dataMaskDDL(true);
        dataMaskDDL(false);
    }

    public void dataMaskDDL(boolean isNereids) throws Exception {
        if (!isNereids) {
            setSession("set enable_nereids_planner = false");
        } else {
            setSession("set enable_nereids_planner = true");
            setSession("set enable_fallback_to_original_planner = false");
        }
        useUser("root");
        //case 1 : add one policy
        String sql = "create data mask policy test on internal.test.table1.k1 to jack using MASK_NULL";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        /**
         * +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
         * | PolicyName | CatalogName | DbName | TableName | ColumnName | DataMaskType | DataMaskDef           | User | Role |
         * +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
         * | test       | internal    | test   | table1    | k1         | MASK_NULL    | NUll                  | jack | NULL |
         * +------------+-------------+--------+-----------+------------+--------------+-----------------------+------+------+
         */
        List<List<String>> resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(resultRows.size(), 1);
        Assertions.assertEquals(resultRows.get(0).get(7), "jack");
        //case 2: and another policy
        sql = "create data mask policy test1 on internal.test.table1.k2 to jack using MASK_HASH";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(resultRows.size(), 2);
        for (List<String> resultRow : resultRows) {
            Assertions.assertEquals(resultRow.get(7), "jack");
        }
        // case 3: and same as case 2 policy. should throw exception
        sql = "create data mask policy test2 on internal.test.table1.k2 to jack using MASK_HASH";
        //getSqlStmtExecutor(sql);
        Assertions.assertNull(getSqlStmtExecutor(sql));
        //case 3: drop a policy
        sql = "drop data mask policy test1";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(resultRows.size(), 1);
        //case 4: add policy for role
        sql = "create data mask policy test3 on internal.test.table1.k2 to role role1 using MASK_HASH";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        resultRows = showDataMaskPolicy("jack", null).getResultRows();
        Assertions.assertEquals(resultRows.size(), 1);
        Assertions.assertEquals(resultRows.get(0).get(7), "jack");
        resultRows = showDataMaskPolicy(null, "role1").getResultRows();
        Assertions.assertEquals(resultRows.size(), 1);
        Assertions.assertEquals(resultRows.get(0).get(8), "role1");
        resultRows = showDataMaskPolicy(null, null).getResultRows();
        Assertions.assertEquals(resultRows.size(), 2);

        //case 5: add policy same as case 4. should throw exception
        sql = "create data mask policy test4 on internal.test.table1.k2 to role role1 using MASK_HASH";
        Assertions.assertNull(getSqlStmtExecutor(sql));

        sql = "drop data mask policy test";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));

        sql = "drop data mask policy test3";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
    }

    @Test
    public void testDataMaskForQuery() throws Exception {
        setSession("set enable_nereids_planner = true");
        useUser("root");
        String sql = "create data mask policy test on internal.test.table1.k1 to jack using MASK_HASH";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        sql = "create data mask policy test1 on internal.test.table1.k2 to jack using MASK_DEFAULT";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        useUser("jack");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("hex(sha2(CAST(k1[#0] AS varchar(65533)), 256)), ''"));
        sql = "drop data mask policy test";
        useUser("root");
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
        sql = "drop data mask policy test1";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
    }
}
