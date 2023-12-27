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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils.RelatedTableInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Test for materialized view util
 */
public class MaterializedViewUtilsTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("mv_util_test");
        useDatabase("mv_util_test");

        createTable("CREATE TABLE IF NOT EXISTS lineitem (\n"
                + "  L_ORDERKEY    INTEGER NOT NULL,\n"
                + "  L_PARTKEY     INTEGER NOT NULL,\n"
                + "  L_SUPPKEY     INTEGER NOT NULL,\n"
                + "  L_LINENUMBER  INTEGER NOT NULL,\n"
                + "  L_QUANTITY    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_EXTENDEDPRICE  DECIMALV3(15,2) NOT NULL,\n"
                + "  L_DISCOUNT    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_TAX         DECIMALV3(15,2) NOT NULL,\n"
                + "  L_RETURNFLAG  CHAR(1) NOT NULL,\n"
                + "  L_LINESTATUS  CHAR(1) NOT NULL,\n"
                + "  L_SHIPDATE    DATE NOT NULL,\n"
                + "  L_COMMITDATE  DATE NOT NULL,\n"
                + "  L_RECEIPTDATE DATE NOT NULL,\n"
                + "  L_SHIPINSTRUCT CHAR(25) NOT NULL,\n"
                + "  L_SHIPMODE     CHAR(10) NOT NULL,\n"
                + "  L_COMMENT      VARCHAR(44) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)\n"
                + "PARTITION BY RANGE(L_SHIPDATE) (PARTITION `day_1` VALUES LESS THAN ('2017-02-01'))\n"
                + "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS orders  (\n"
                + "  O_ORDERKEY       INTEGER NOT NULL,\n"
                + "  O_CUSTKEY        INTEGER NOT NULL,\n"
                + "  O_ORDERSTATUS    CHAR(1) NOT NULL,\n"
                + "  O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,\n"
                + "  O_ORDERDATE      DATE NOT NULL,\n"
                + "  O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n"
                + "  O_CLERK          CHAR(15) NOT NULL, \n"
                + "  O_SHIPPRIORITY   INTEGER NOT NULL,\n"
                + "  O_COMMENT        VARCHAR(79) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)\n"
                + "PARTITION BY RANGE(O_ORDERDATE) (PARTITION `day_2` VALUES LESS THAN ('2017-03-01'))\n"
                + "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS partsupp (\n"
                + "  PS_PARTKEY     INTEGER NOT NULL,\n"
                + "  PS_SUPPKEY     INTEGER NOT NULL,\n"
                + "  PS_AVAILQTY    INTEGER NOT NULL,\n"
                + "  PS_SUPPLYCOST  DECIMALV3(15,2)  NOT NULL,\n"
                + "  PS_COMMENT     VARCHAR(199) NOT NULL \n"
                + ")\n"
                + "DUPLICATE KEY(PS_PARTKEY, PS_SUPPKEY)\n"
                + "DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");

        createTable("CREATE TABLE IF NOT EXISTS lineitem_null (\n"
                + "  L_ORDERKEY    INTEGER NOT NULL,\n"
                + "  L_PARTKEY     INTEGER NOT NULL,\n"
                + "  L_SUPPKEY     INTEGER NOT NULL,\n"
                + "  L_LINENUMBER  INTEGER NOT NULL,\n"
                + "  L_QUANTITY    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_EXTENDEDPRICE  DECIMALV3(15,2) NOT NULL,\n"
                + "  L_DISCOUNT    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_TAX         DECIMALV3(15,2) NOT NULL,\n"
                + "  L_RETURNFLAG  CHAR(1) NOT NULL,\n"
                + "  L_LINESTATUS  CHAR(1) NOT NULL,\n"
                + "  L_SHIPDATE    DATE NULL,\n"
                + "  L_COMMITDATE  DATE NULL,\n"
                + "  L_RECEIPTDATE DATE NULL,\n"
                + "  L_SHIPINSTRUCT CHAR(25) NOT NULL,\n"
                + "  L_SHIPMODE     CHAR(10) NOT NULL,\n"
                + "  L_COMMENT      VARCHAR(44) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)\n"
                + "PARTITION BY RANGE(L_SHIPDATE) (PARTITION `day_1` VALUES LESS THAN ('2017-02-01'))\n"
                + "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
    }

    @Test
    public void getRelatedTableInfoTestWithoutGroupTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT (o.c1_abs + ps.c2_abs) as add_alias, l.L_SHIPDATE, l.L_ORDERKEY, o.O_ORDERDATE, "
                                + "ps.PS_AVAILQTY "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", rewrittenPlan);
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithoutGroupNullTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT (o.c1_abs + ps.c2_abs) as add_alias, l.L_SHIPDATE, l.L_ORDERKEY, o.O_ORDERDATE, "
                                + "ps.PS_AVAILQTY "
                                + "FROM "
                                + "lineitem_null as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", rewrittenPlan);
                            Assertions.assertFalse(relatedTableInfo.isPresent());
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithSubqueryTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT l.L_SHIPDATE AS ship_data_alias, o.O_ORDERDATE, count(*) "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY "
                                + "GROUP BY l.L_SHIPDATE, o.O_ORDERDATE ",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("ship_data_alias", rewrittenPlan);
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "l_shipdate",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithAliasAndGroupTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT t1.L_SHIPDATE, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
                                + "count(distinct case when t1.L_SUPPKEY > 0 then t2.O_ORDERSTATUS else null end) as cnt_1 "
                                + "from "
                                + "  (select * from "
                                + "  lineitem "
                                + "  where L_SHIPDATE in ('2017-01-30')) t1 "
                                + "left join "
                                + "  (select * from "
                                + "  orders "
                                + "  where O_ORDERDATE in ('2017-01-30')) t2 "
                                + "on t1.L_ORDERKEY = t2.O_ORDERKEY "
                                + "group by "
                                + "t1.L_SHIPDATE, "
                                + "t2.O_ORDERDATE, "
                                + "t1.L_QUANTITY, "
                                + "t2.O_ORDERSTATUS;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", rewrittenPlan);
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoUseRightTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT t1.L_SHIPDATE, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
                                + "count(distinct case when t1.L_SUPPKEY > 0 then t2.O_ORDERSTATUS else null end) as cnt_1 "
                                + "from "
                                + "  (select * from "
                                + "  lineitem "
                                + "  where L_SHIPDATE in ('2017-01-30')) t1 "
                                + "right join "
                                + "  (select * from "
                                + "  orders "
                                + "  where O_ORDERDATE in ('2017-01-30')) t2 "
                                + "on t1.L_ORDERKEY = t2.O_ORDERKEY "
                                + "group by "
                                + "t1.L_SHIPDATE, "
                                + "t2.O_ORDERDATE, "
                                + "t1.L_QUANTITY, "
                                + "t2.O_ORDERSTATUS;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("o_orderdate", rewrittenPlan);
                            checkRelatedTableInfo(relatedTableInfo,
                                    "orders",
                                    "O_ORDERDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoUseNullGenerateSideTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT t1.L_SHIPDATE, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
                                + "count(distinct case when t1.L_SUPPKEY > 0 then t2.O_ORDERSTATUS else null end) as cnt_1 "
                                + "from "
                                + "  (select * from "
                                + "  lineitem "
                                + "  where L_SHIPDATE in ('2017-01-30')) t1 "
                                + "left join "
                                + "  (select * from "
                                + "  orders "
                                + "  where O_ORDERDATE in ('2017-01-30')) t2 "
                                + "on t1.L_ORDERKEY = t2.O_ORDERKEY "
                                + "group by "
                                + "t1.L_SHIPDATE, "
                                + "t2.O_ORDERDATE, "
                                + "t1.L_QUANTITY, "
                                + "t2.O_ORDERSTATUS;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("o_orderdate", rewrittenPlan);
                            Assertions.assertFalse(relatedTableInfo.isPresent());
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithoutPartitionTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT ps_1.PS_SUPPLYCOST "
                                + "FROM "
                                + "partsupp as ps_1 "
                                + "LEFT JOIN "
                                + "partsupp as ps_2 "
                                + "ON ps_1.PS_PARTKEY = ps_2.PS_SUPPKEY ",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("PS_SUPPLYCOST", rewrittenPlan);
                            Assertions.assertFalse(relatedTableInfo.isPresent());
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithWindowTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT (o.c1_abs + ps.c2_abs) as add_alias, l.L_SHIPDATE, l.L_ORDERKEY, o.O_ORDERDATE, "
                                + "count(o.O_ORDERDATE) over (partition by l.L_SHIPDATE order by l.L_ORDERKEY  rows between unbounded preceding and current row) as window_count "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", rewrittenPlan);
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithWindowButNotPartitionTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT (o.c1_abs + ps.c2_abs) as add_alias, l.L_SHIPDATE, l.L_ORDERKEY, o.O_ORDERDATE, "
                                + "count(o.O_ORDERDATE) over (partition by l.L_ORDERKEY order by l.L_ORDERKEY  rows between unbounded preceding and current row) as window_count "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            Optional<RelatedTableInfo> relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("L_SHIPDATE", rewrittenPlan);
                            Assertions.assertFalse(relatedTableInfo.isPresent());
                        });
    }

    @Test
    public void containTableQueryOperatorWithTabletTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select * from orders TABLET(11080)",
                        nereidsPlanner -> {
                            Plan analyzedPlan = nereidsPlanner.getAnalyzedPlan();
                            Assertions.assertTrue(MaterializedViewUtils.containTableQueryOperator(analyzedPlan));
                        });
    }

    @Test
    public void containTableQueryOperatorTableSampleTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select * from orders TABLESAMPLE(20 percent)",
                        nereidsPlanner -> {
                            Plan analyzedPlan = nereidsPlanner.getAnalyzedPlan();
                            Assertions.assertTrue(MaterializedViewUtils.containTableQueryOperator(analyzedPlan));
                        });
    }

    @Test
    public void containTableQueryOperatorPartitionTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select * from orders PARTITION day_2",
                        nereidsPlanner -> {
                            Plan analyzedPlan = nereidsPlanner.getAnalyzedPlan();
                            Assertions.assertTrue(MaterializedViewUtils.containTableQueryOperator(analyzedPlan));
                        });
    }

    @Test
    public void containTableQueryOperatorWithoutOperatorTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select * from orders",
                        nereidsPlanner -> {
                            Plan analyzedPlan = nereidsPlanner.getAnalyzedPlan();
                            Assertions.assertFalse(MaterializedViewUtils.containTableQueryOperator(analyzedPlan));
                        });
    }

    private void checkRelatedTableInfo(Optional<RelatedTableInfo> relatedTableInfo,
            String expectTableName,
            String expectColumnName,
            boolean pctPossible) {
        Assertions.assertTrue(relatedTableInfo.isPresent());
        BaseTableInfo relatedBaseTableInfo = relatedTableInfo.get().getTableInfo();
        try {
            TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(relatedBaseTableInfo.getCtlId())
                    .getDbOrAnalysisException(relatedBaseTableInfo.getDbId())
                    .getTableOrAnalysisException(relatedBaseTableInfo.getTableId());
            Assertions.assertEquals(tableIf.getName(), expectTableName);
        } catch (Exception exception) {
            Assertions.fail();
        }
        Assertions.assertEquals(relatedTableInfo.get().getColumn().toLowerCase(), expectColumnName.toLowerCase());
        Assertions.assertTrue(pctPossible);
    }
}
