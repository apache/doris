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

import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PartitionColumnTraceTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("partition_column_trace_test");
        useDatabase("partition_column_trace_test");

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
                + "  O_ORDERDATE_NOT      DATE NOT NULL,\n"
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

        createTable("CREATE TABLE IF NOT EXISTS orders_no_part  (\n"
                + "  O_ORDERKEY       INTEGER NOT NULL,\n"
                + "  O_CUSTKEY        INTEGER NOT NULL,\n"
                + "  O_ORDERSTATUS    CHAR(1) NOT NULL,\n"
                + "  O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,\n"
                + "  O_ORDERDATE      DATE NOT NULL,\n"
                + "  O_ORDERDATE_NOT      DATE NOT NULL,\n"
                + "  O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n"
                + "  O_CLERK          CHAR(15) NOT NULL, \n"
                + "  O_SHIPPRIORITY   INTEGER NOT NULL,\n"
                + "  O_COMMENT        VARCHAR(79) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)\n"
                + "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");

        createTable("CREATE TABLE IF NOT EXISTS lineitem_auto (\n"
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
                + "auto partition by range (date_trunc(`L_SHIPDATE`, 'day')) ()\n"
                + "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");

        createTable("CREATE TABLE IF NOT EXISTS orders_auto  (\n"
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
                + "auto partition by range (date_trunc(`O_ORDERDATE`, 'day')) ()\n"
                + "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");

        createTable("CREATE TABLE `lineitem_list_partition` (\n"
                + "      `l_orderkey` BIGINT not NULL,\n"
                + "      `l_linenumber` INT NULL,\n"
                + "      `l_partkey` INT NULL,\n"
                + "      `l_suppkey` INT NULL,\n"
                + "      `l_quantity` DECIMAL(15, 2) NULL,\n"
                + "      `l_extendedprice` DECIMAL(15, 2) NULL,\n"
                + "      `l_discount` DECIMAL(15, 2) NULL,\n"
                + "      `l_tax` DECIMAL(15, 2) NULL,\n"
                + "      `l_returnflag` VARCHAR(1) NULL,\n"
                + "      `l_linestatus` VARCHAR(1) NULL,\n"
                + "      `l_commitdate` DATE NULL,\n"
                + "      `l_receiptdate` DATE NULL,\n"
                + "      `l_shipinstruct` VARCHAR(25) NULL,\n"
                + "      `l_shipmode` VARCHAR(10) NULL,\n"
                + "      `l_comment` VARCHAR(44) NULL,\n"
                + "      `l_shipdate` DATE NULL\n"
                + "    ) ENGINE=OLAP\n"
                + "    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )\n"
                + "    COMMENT 'OLAP'\n"
                + "    PARTITION BY list(l_orderkey) (\n"
                + "    PARTITION p1 VALUES in ('1'),\n"
                + "    PARTITION p2 VALUES in ('2'),\n"
                + "    PARTITION p3 VALUES in ('3')\n"
                + "    )\n"
                + "    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n"
                + "    PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + "    )");

        createView("CREATE VIEW lineitem_daily_summary_view AS\n"
                + "SELECT \n"
                + "    DATE_TRUNC('day', L_SHIPDATE) AS ship_date,\n"
                + "    L_RETURNFLAG,\n"
                + "    L_LINESTATUS,\n"
                + "    COUNT(*) AS order_count,\n"
                + "    SUM(L_QUANTITY) AS total_quantity,\n"
                + "    SUM(L_EXTENDEDPRICE) AS total_price,\n"
                + "    AVG(L_DISCOUNT) AS avg_discount\n"
                + "FROM lineitem\n"
                + "WHERE L_SHIPDATE IS NOT NULL\n"
                + "GROUP BY ship_date, L_RETURNFLAG, L_LINESTATUS;");

        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules("OLAP_SCAN_PARTITION_PRUNE,PRUNE_EMPTY_PARTITION");
    }

    // inner join + self join + partition in join condition + valid side
    @Test
    public void test1() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(
                                    ImmutableList.of("lineitem", "l_shipdate", "true", "true")),
                                    "");
                        });
    }

    // with sort
    @Test
    public void test101() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        order by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(
                                            ImmutableList.of("lineitem", "l_shipdate", "true", "true")),
                                    "");
                        });
    }

    @Test
    public void test100() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(l1.l_shipdate) as col_count\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("col_count", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "partition column use invalid implicit expression"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
                        });
    }

    // inner join + self join + partition in join condition + invalid side
    @Test
    public void test2() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(
                                    ImmutableList.of("lineitem", "l_shipdate", "true", "true")),
                                    "");
                        });
    }

    // inner join + self join + partition not in join condition +  valid side
    @Test
    public void test3() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
                        });
    }

    // inner join + self join + partition not in join condition + invalid side
    @Test
    public void test4() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        inner join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in invalid catalog relation to check");
                        });
    }

    // inner join + not self join + partition in join condition + valid side
    @Test
    public void test5() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true"), ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "");
                        });
    }

    @Test
    public void test502() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem_auto\n"
                                + "        inner join orders_auto\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem_auto", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders_auto", "o_orderdate", "true", "true")), "");
                        });
    }

    @Test
    public void test500() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o.o_orderdate_alias, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join (select date_trunc(o_orderdate, 'day') o_orderdate_alias from orders) o\n"
                                + "        on l_shipdate = o.o_orderdate_alias\n"
                                + "        group by l_shipdate, o.o_orderdate_alias",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders", "o_orderdate", "true", "true")), "");
                        });
    }

    @Test
    public void test5000() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o.o_orderdate_alias, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join (select date_trunc(o_orderdate, 'day') o_orderdate_alias from orders) o\n"
                                + "        on l_shipdate = o.o_orderdate_alias\n"
                                + "        group by l_shipdate, o.o_orderdate_alias",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate_alias", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true"),
                                            ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "day");
                        });
    }

    // test with date_trunc with alias
    @Test
    public void test5001() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, date_trunc(o_orderdate, 'day') o_orderdate_alias, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join orders o\n"
                                + "        on l_shipdate = o.o_orderdate\n"
                                + "        group by l_shipdate, o.o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate_alias", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true"),
                                            ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "day");
                        });
    }

    @Test
    public void test501() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate_alias, o.o_orderdate_alias, count(l_shipdate_alias) \n"
                                + "        from (select date_trunc(l_shipdate, 'day') l_shipdate_alias from lineitem) l\n"
                                + "        inner join (select date_trunc(o_orderdate, 'day') o_orderdate_alias from orders) o\n"
                                + "        on l_shipdate_alias = o.o_orderdate_alias\n"
                                + "        group by l_shipdate_alias, o.o_orderdate_alias",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate_alias", "month",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "month");
                        });
    }

    // inner join + not self join + partition in join condition + invalid side
    @Test
    public void test6() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders", "o_orderdate", "true", "true")), "");
                        });
    }

    @Test
    public void test601() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem_auto\n"
                                + "        inner join orders_auto\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem_auto", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders_auto", "o_orderdate", "true", "true")), "");
                        });
    }

    // inner join + not self join + partition not in join condition +  valid side
    @Test
    public void test7() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "");
                        });
    }

    // inner join + not self join + partition not in join condition + invalid side
    @Test
    public void test8() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        inner join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true")), "");
                        });
    }



    // left outer join + self join + partition in join condition + valid side
    @Test
    public void test9() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        left outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "partition column is in join invalid side");
                        });
    }

    // left outer join + self join + partition in join condition + invalid side
    @Test
    public void test10() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        left outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "partition column is in un supported join null generate side");
                        });
    }

    // left outer join + self join + partition not in join condition +  valid side
    @Test
    public void test11() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        left outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in invalid catalog relation to check");
                        });
    }

    // left outer join + self join + partition not in join condition + invalid side
    @Test
    public void test12() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        left outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // left outer join + not self join + partition in join condition + valid side
    @Test
    public void test13() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        left outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "");
                        });
    }

    // left outer join + not self join + partition in join condition + invalid side
    @Test
    public void test14() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        left outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // left outer join + not self join + partition not in join condition +  valid side
    @Test
    public void test15() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        left outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true")), "");
                        });
    }

    // left outer join + not self join + partition not in join condition + invalid side
    @Test
    public void test16() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        left outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }


    // right outer join + self join + partition in join condition + valid side
    @Test
    public void test17() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        right outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // right outer join + self join + partition in join condition + invalid side
    @Test
    public void test18() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        right outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "partition column is in join invalid side, but is not in join condition");
                        });
    }

    // right outer join + self join + partition not in join condition +  valid side
    @Test
    public void test19() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        right outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // right outer join + self join + partition not in join condition + invalid side
    @Test
    public void test20() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        right outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
                        });
    }

    // right outer join + not self join + partition in join condition + invalid side
    @Test
    public void test21() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        right outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // right outer join + not self join + partition in join condition + valid side
    @Test
    public void test22() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        right outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true")), "");
                        });
    }

    // right outer join + not self join + partition not in join condition +  invalid side
    @Test
    public void test23() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        right outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // right outer join + not self join + partition not in join condition + valid side
    @Test
    public void test24() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        right outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("orders", "o_orderdate", "true", "true")), "");
                        });
    }


    // full outer join + self join + partition in join condition + valid side
    @Test
    public void test25() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        full outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + self join + partition in join condition + invalid side
    @Test
    public void test26() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        full outer join lineitem l2\n"
                                + "        on l1.l_shipdate = l2.l_shipdate\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + self join + partition not in join condition +  valid side
    @Test
    public void test27() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l1.l_shipdate, l2.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        full outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l1.l_shipdate, l2.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + self join + partition not in join condition + invalid side
    @Test
    public void test28() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l2.l_shipdate, l1.L_ORDERKEY, count(*)\n"
                                + "        from lineitem l1\n"
                                + "        full outer join lineitem l2\n"
                                + "        on l1.l_orderkey = l2.l_orderkey\n"
                                + "        group by l2.l_shipdate, l1.L_ORDERKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + not self join + partition in join condition + invalid side
    @Test
    public void test29() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        full outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + not self join + partition in join condition + valid side
    @Test
    public void test30() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        full outer join orders\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + not self join + partition not in join condition +  invalid side
    @Test
    public void test31() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        full outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // full outer join + not self join + partition not in join condition + valid side
    @Test
    public void test32() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, o_orderdate, count(l_shipdate) \n"
                                + "        from lineitem\n"
                                + "        full outer join orders\n"
                                + "        on l_orderkey = o_orderkey\n"
                                + "        group by l_shipdate, o_orderdate",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo,
                                    "partition column is in un supported join null generate side");
                        });
    }

    // union all input1 is partitioned + input2 is not partitioned but match incremental refresh
    @Test
    public void test33() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select O_ORDERDATE, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders_no_part;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "not union all output pass partition increment check");
                        });
    }

    // union all input1 is partitioned + input2 is not partitioned not match incremental refresh
    @Test
    public void test34() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select date_add(O_ORDERDATE, INTERVAL 1 day) as a, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders_no_part",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "union all output doesn't match the partition increment check");
                        });
    }

    // union all input1 is partitioned + input2 is partitioned but match incremental refresh + use partition column
    @Test
    public void test35() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select O_ORDERDATE, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders", "o_orderdate", "true", "true")),
                                    "");
                        });
    }


    // union all input1 is partitioned + input2 is partitioned not match incremental refresh + use partition column
    @Test
    public void test36() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select date_add(O_ORDERDATE, INTERVAL 1 day) as a, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "union all output doesn't match the partition increment check");
                        });
    }


    // union all input1 is partitioned + input2 is partitioned but match incremental refresh + not use partition column
    @Test
    public void test37() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select O_ORDERDATE_NOT, O_ORDERDATE\n"
                                + "from\n"
                                + "orders;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "not union all output pass partition increment check");
                        });
    }

    // union all input1 is partitioned + input2 is partitioned not match incremental refresh + not use partition column
    @Test
    public void test38() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select date_add(O_ORDERDATE_NOT, INTERVAL 1 day) as a, O_ORDERDATE\n"
                                + "from\n"
                                + "orders;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "not union all output pass partition increment check");
                        });
    }

    // union all input1 is partitioned + input2 is not partitioned but match incremental refresh with date_trunc
    @Test
    public void test39() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union all\n"
                                + "select O_ORDERDATE, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders_no_part;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", "month",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            failWith(relatedTableInfo, "not union all output pass partition increment check");
                        });
    }


    // test with cte
    @Test
    public void test40() {
        PlanChecker.from(connectContext)
                .checkExplain("with c1 as (\n"
                                + "  select \n"
                                + "    l_shipdate, \n"
                                + "    o_orderdate, \n"
                                + "    count(l_shipdate) as count_s \n"
                                + "  from \n"
                                + "    lineitem \n"
                                + "    inner join orders on l_shipdate = o_orderdate \n"
                                + "  group by \n"
                                + "    l_shipdate, \n"
                                + "    o_orderdate\n"
                                + "), \n"
                                + "c2 as (\n"
                                + "  select \n"
                                + "    l_shipdate, \n"
                                + "    o_orderdate, \n"
                                + "    count_s \n"
                                + "  from \n"
                                + "    c1\n"
                                + "), \n"
                                + "c3 as (\n"
                                + "  select \n"
                                + "    l_shipdate, \n"
                                + "    count_s \n"
                                + "  from \n"
                                + "    c1\n"
                                + ") \n"
                                + "select \n"
                                + "  c2.l_shipdate, \n"
                                + "  c3.count_s \n"
                                + "from \n"
                                + "  c2 \n"
                                + "  inner join c3 on c2.l_shipdate = c3.l_shipdate;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(
                                            ImmutableList.of("lineitem", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders", "o_orderdate", "true", "true")),
                                    null);
                        });
    }


    // test with union but not union all
    @Test
    public void test41() {
        PlanChecker.from(connectContext)
                .checkExplain("select L_SHIPDATE, L_COMMITDATE\n"
                                + "from\n"
                                + "lineitem\n"
                                + "union\n"
                                + "select O_ORDERDATE, O_ORDERDATE_NOT\n"
                                + "from\n"
                                + "orders;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo, ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true"),
                                            ImmutableList.of("orders", "o_orderdate", "true", "true")),
                                    "");
                        });
    }

    // test with view which contains date_trunc
    @Test
    public void test42() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT\n"
                                + "    ship_date,\n"
                                + "    L_RETURNFLAG,\n"
                                + "    SUM(order_count) AS total_orders,\n"
                                + "    SUM(total_quantity) AS sum_quantity,\n"
                                + "    SUM(total_price) AS sum_price,\n"
                                + "    AVG(avg_discount) AS average_discount\n"
                                + "FROM lineitem_daily_summary_view\n"
                                + "GROUP BY ship_date, L_RETURNFLAG\n"
                                + "ORDER BY ship_date, L_RETURNFLAG, total_orders, sum_quantity, sum_price;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfos("ship_date", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            successWith(relatedTableInfo,
                                    ImmutableSet.of(ImmutableList.of("lineitem", "l_shipdate", "true", "true")),
                                    "day");
                        });
    }

    private static void successWith(RelatedTableInfo relatedTableInfo,
            Set<List<String>> expectTableColumnPairSet, String timeUnit) {
        Assertions.assertFalse(relatedTableInfo.getTableColumnInfos().isEmpty());
        Assertions.assertTrue(relatedTableInfo.isPctPossible());

        Set<List<String>> relatedTableColumnPairs = new HashSet<>();
        List<RelatedTableColumnInfo> tableColumnInfos = relatedTableInfo.getTableColumnInfos();
        boolean anyFoundDateTrunc = false;
        for (RelatedTableColumnInfo info : tableColumnInfos) {
            Optional<Expression> partitionExpression = info.getPartitionExpression();
            if (StringUtils.isNotEmpty(timeUnit) && !partitionExpression.isPresent()) {
                Assertions.fail("excepted time unit in partition expression but not");
            }
            if (StringUtils.isNotEmpty(timeUnit) && partitionExpression.isPresent()) {
                List<DateTrunc> dateTruncs = partitionExpression.get().collectToList(DateTrunc.class::isInstance);
                anyFoundDateTrunc = anyFoundDateTrunc || (dateTruncs.size() == 1
                        && (Objects.equals("'" + timeUnit + "'", dateTruncs.get(0).getArgument(0).toString().toLowerCase())
                        || Objects.equals("'" + timeUnit + "'", dateTruncs.get(0).getArgument(1).toString().toLowerCase())));
            }
            try {
                relatedTableColumnPairs.add(
                        ImmutableList.of(info.getTableInfo().getTableName(), info.getColumnStr().toLowerCase(),
                                String.valueOf(info.isReachRelationCheck()), String.valueOf(info.isFromTablePartitionColumn())));
            } catch (Exception exception) {
                Assertions.fail("excepted table and column in related table column info but not");
            }
        }
        if (StringUtils.isNotEmpty(timeUnit)) {
            Assertions.assertTrue(anyFoundDateTrunc);
        }
        Assertions.assertEquals(expectTableColumnPairSet, relatedTableColumnPairs);
    }

    private static void failWith(RelatedTableInfo relatedTableInfo,
            String failInfo) {
        Assertions.assertFalse(relatedTableInfo.isPctPossible());
        Assertions.assertTrue(relatedTableInfo.getFailReason().contains(failInfo));
    }
}
