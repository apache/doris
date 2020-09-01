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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class UniqueKeySemiJoinTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createTbl1Str = "CREATE TABLE `t0` (\n" +
                "  `c0` tinyint NOT NULL,\n" +
                "  `c1` tinyint NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`c0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c0`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        String createTbl2Str = "CREATE TABLE `t1` (\n" +
                "  `c0` tinyint NOT NULL,\n" +
                "  `c1` tinyint NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`c0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c0`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTbl1Str)
                   .withTable(createTbl2Str);
    }

    @Test
    public void testSemiJoin() throws Exception {
        String sql = " SELECT * FROM t1 LEFT SEMI JOIN t0 ON t1.c0 = t0.c0";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("t0.__DORIS_DELETE_SIGN__ is a hidden column to mark whether a row deleted when unique key existed, '__DORIS_DELETE_SIGN__ = 0' will appear in where clause, that will cause semi join syntax error, please use not in/existed as replacement.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        sql = " SELECT * FROM t1 RIGHT SEMI JOIN t0 ON t1.c0 = t0.c0";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("t0.__DORIS_DELETE_SIGN__ is a hidden column to mark whether a row deleted when unique key existed, '__DORIS_DELETE_SIGN__ = 0' will appear in where clause, that will cause semi join syntax error, please use not in/existed as replacement.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        sql = " SELECT * FROM t1 LEFT ANTI JOIN t0 ON t1.c0 = t0.c0";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("t0.__DORIS_DELETE_SIGN__ is a hidden column to mark whether a row deleted when unique key existed, '__DORIS_DELETE_SIGN__ = 0' will appear in where clause, that will cause semi join syntax error, please use not in/existed as replacement.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        sql = " SELECT * FROM t1 RIGHT ANTI JOIN t0 ON t1.c0 = t0.c0";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("t0.__DORIS_DELETE_SIGN__ is a hidden column to mark whether a row deleted when unique key existed, '__DORIS_DELETE_SIGN__ = 0' will appear in where clause, that will cause semi join syntax error, please use not in/existed as replacement.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        sql = " SELECT * FROM t1 LEFT JOIN t0 ON t1.c0 = t0.c0";
        dorisAssert.query(sql).explainQuery();

        sql = " SELECT * FROM t1 RIGHT JOIN t0 ON t1.c0 = t0.c0";
        dorisAssert.query(sql).explainQuery();

        sql = " SELECT * FROM t1 where c0 NOT IN ( select c0 from t0)";
        dorisAssert.query(sql).explainQuery();
    }
}
