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

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class StmtRewriterTest {

    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/StmtRewriterTest/"
            + UUID.randomUUID().toString() + "/";
    private static final String TABLE_NAME = "table1";
    private static final String DB_NAME = "db1";
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void beforeClass() throws Exception{
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME + " (empid int, name varchar, " +
                "deptno int, salary int, commission int) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    /**
     * The whole query plan is following:
     +-----------------------------------------+
     | Explain String                          |
     +-----------------------------------------+
     | PLAN FRAGMENT 0                         |
     |  OUTPUT EXPRS:<slot 2> | <slot 3>       |
     |   PARTITION: UNPARTITIONED              |
     |                                         |
     |   RESULT SINK                           |
     |                                         |
     |   10:EXCHANGE                           |
     |      tuple ids: 1 5                     |
     |                                         |
     | PLAN FRAGMENT 1                         |
     |  OUTPUT EXPRS:                          |
     |   PARTITION: HASH_PARTITIONED: <slot 2> |
     |                                         |
     |   STREAM DATA SINK                      |
     |     EXCHANGE ID: 10                     |
     |     UNPARTITIONED                       |
     |                                         |
     |   4:CROSS JOIN                          |
     |   |  cross join:                        |
     |   |  predicates: <slot 3> > <slot 8>    |
     |   |  tuple ids: 1 5                     |
     |   |                                     |
     |   |----9:EXCHANGE                       |
     |   |       tuple ids: 5                  |
     |   |                                     |
     |   6:AGGREGATE (merge finalize)          |
     |   |  output: sum(<slot 3>)              |
     |   |  group by: <slot 2>                 |
     |   |  tuple ids: 1                       |
     |   |                                     |
     |   5:EXCHANGE                            |
     |      tuple ids: 1                       |
     |                                         |
     | PLAN FRAGMENT 2                         |
     |  OUTPUT EXPRS:                          |
     |   PARTITION: UNPARTITIONED              |
     |                                         |
     |   STREAM DATA SINK                      |
     |     EXCHANGE ID: 09                     |
     |     UNPARTITIONED                       |
     |                                         |
     |   8:AGGREGATE (merge finalize)          |
     |   |  output: avg(<slot 7>)              |
     |   |  group by:                          |
     |   |  tuple ids: 5                       |
     |   |                                     |
     |   7:EXCHANGE                            |
     |      tuple ids: 4                       |
     |                                         |
     | PLAN FRAGMENT 3                         |
     |  OUTPUT EXPRS:                          |
     |   PARTITION: RANDOM                     |
     |                                         |
     |   STREAM DATA SINK                      |
     |     EXCHANGE ID: 07                     |
     |     UNPARTITIONED                       |
     |                                         |
     |   3:AGGREGATE (update serialize)        |
     |   |  output: avg(`salary`)              |
     |   |  group by:                          |
     |   |  tuple ids: 4                       |
     |   |                                     |
     |   2:OlapScanNode                        |
     |      TABLE: all_type_table              |
     |      PREAGGREGATION: ON                 |
     |      rollup: all_type_table             |
     |      tuple ids: 3                       |
     |                                         |
     | PLAN FRAGMENT 4                         |
     |  OUTPUT EXPRS:                          |
     |   PARTITION: RANDOM                     |
     |                                         |
     |   STREAM DATA SINK                      |
     |     EXCHANGE ID: 05                     |
     |     HASH_PARTITIONED: <slot 2>          |
     |                                         |
     |   1:AGGREGATE (update serialize)        |
     |   |  STREAMING                          |
     |   |  output: sum(`salary`)              |
     |   |  group by: `empid`                  |
     |   |  tuple ids: 1                       |
     |   |                                     |
     |   0:OlapScanNode                        |
     |      TABLE: all_type_table              |
     |      PREAGGREGATION: ON                 |
     |      rollup: all_type_table             |
     |      tuple ids: 0                       |
     +-----------------------------------------+
     *
     * @throws Exception
     */
    @Test
    public void testRewriteHavingClauseSubqueries() throws Exception {
        String subquery = "select avg(salary) from " + TABLE_NAME;
        String query = "select empid, sum(salary) from " + TABLE_NAME + " group by empid having sum(salary) > (" +
                subquery + ");";
        dorisAssert.query(query).explainContains("CROSS JOIN", "predicates: <slot 3> > <slot 8>");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }
}
