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

package org.apache.doris.nereids.mv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.qe.ConnectContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Covers MTMV rewrite for GROUPING SETS / grouping_id / CTE shapes that interact with
 * LogicalPlanDeepCopier and deferred predicates (see MaterializedViewDebugTest scenarios).
 */
public class GroupingSetsMtmvRewriteTest extends SqlTestBase {

    private static final String MV_NAME = "mv_table_a";

    @Override
    protected void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createTable(
                "CREATE TABLE IF NOT EXISTS table_a (\n"
                        + "  `g` varchar(20) NULL,\n"
                        + "  `value_1` bigint NULL,\n"
                        + "  `date_1` varchar(20) NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`g`)\n"
                        + "DISTRIBUTED BY HASH(`g`) BUCKETS 3\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n");
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx, boolean isMVPartitionValid,
                    Map<BaseTableInfo, Set<String>> queryUsedRelatedTablePartitionsMap) {
                return true;
            }
        };
        createMvByNereids(
                "CREATE MATERIALIZED VIEW " + MV_NAME + "\n"
                        + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                        + "DISTRIBUTED BY HASH(`g`) BUCKETS 3\n"
                        + "PROPERTIES ('replication_num'='1')\n"
                        + "AS SELECT date_1, g, sum(value_1) FROM table_a a GROUP BY date_1, g");
    }

    /**
     * Ensures EXPLAIN succeeds without MV rewrite pipeline errors (regression for invalid struct /
     * LogicalProperties / deferred predicate issues on these SQL shapes).
     */
    private void assertGroupingSetsExplainOk(String query) throws Exception {
        String plan = getSQLPlanOrErrorMsg("EXPLAIN " + query);
        Assertions.assertFalse(plan.contains("MaterializedViewRewriteFail"),
                () -> "unexpected MV rewrite failure in plan:\n" + plan);
    }

    @Test
    void testGroupingSetsWithGroupingId() throws Exception {
        assertGroupingSetsExplainOk(
                "SELECT date_1, grouping_id(date_1), g, sum(value_1) x\n"
                        + "FROM table_a a GROUP BY GROUPING SETS ((g),(date_1,g))");
    }

    @Test
    void testGroupingSetsOutputAliasOnDate() throws Exception {
        assertGroupingSetsExplainOk(
                "SELECT date_1 col1, grouping_id(date_1), g, sum(value_1) x\n"
                        + "FROM table_a a GROUP BY GROUPING SETS ((g),(date_1,g))");
    }

    @Test
    void testGroupingSetsShortAliasOnDate1() throws Exception {
        assertGroupingSetsExplainOk(
                "SELECT date_1 l, g, sum(value_1) x FROM table_a a "
                        + "GROUP BY GROUPING SETS ((g),(date_1,g))");
    }

    @Test
    void testCteWithGroupingSetsSelectStar() throws Exception {
        assertGroupingSetsExplainOk(
                "WITH temp AS ("
                        + "SELECT date_1, grouping_id(date_1) AS g_id, g, sum(value_1) x FROM table_a "
                        + "GROUP BY GROUPING SETS ((g),(date_1,g))"
                        + ") SELECT * FROM temp");
    }

    @Test
    void testCteWithGroupingSetsFilterOnGroupingId() throws Exception {
        assertGroupingSetsExplainOk(
                "WITH temp AS ("
                        + "SELECT date_1, grouping_id(date_1) AS g_id, g, sum(value_1) x FROM table_a "
                        + "GROUP BY GROUPING SETS ((g),(date_1,g))"
                        + ") SELECT * FROM temp WHERE g_id = 1");
    }
}
