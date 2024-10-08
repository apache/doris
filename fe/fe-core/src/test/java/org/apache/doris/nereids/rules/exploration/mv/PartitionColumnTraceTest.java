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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.TableColumnInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
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
                            successWith(relatedTableInfo, ImmutableSet.of(Pair.of("lineitem", "l_shipdate")),
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
                            successWith(relatedTableInfo, ImmutableSet.of(Pair.of("lineitem", "l_shipdate")),
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
                                    "partition column is in join invalid side, but is not in join condition");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate"),
                                            Pair.of("orders", "o_orderdate")), "");
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
                            failWith(relatedTableInfo, "partition rollup expressions is not consistent");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate")), "month");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate"),
                                            Pair.of("orders", "o_orderdate")), "");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate")), "");
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
                                    ImmutableSet.of(Pair.of("orders", "o_orderdate")), "");
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
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
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
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
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
                                    "partition column is in join invalid side, but is not in join condition");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate")), "");
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
                                    ImmutableSet.of(Pair.of("lineitem", "l_shipdate")), "");
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
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
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
                            // tmp, wait equal set ignore null ready
                            failWith(relatedTableInfo,
                                    "partition column is in join invalid side, but is not in join condition");
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
                                    ImmutableSet.of(Pair.of("orders", "o_orderdate")), "");
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
                                    ImmutableSet.of(Pair.of("orders", "o_orderdate")), "");
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

    private static void successWith(RelatedTableInfo relatedTableInfo,
            Set<Pair<String, String>> expectTableColumnPairSet, String timeUnit) {
        Assertions.assertFalse(relatedTableInfo.getTableColumnInfos().isEmpty());
        Assertions.assertTrue(relatedTableInfo.isPctPossible());

        Set<Pair<String, String>> relatedTableColumnPairs = new HashSet<>();
        List<TableColumnInfo> tableColumnInfos = relatedTableInfo.getTableColumnInfos();
        for (TableColumnInfo info : tableColumnInfos) {
            Optional<Expression> partitionExpression = info.getPartitionExpression();
            if (StringUtils.isNotEmpty(timeUnit)) {
                Assertions.assertTrue(partitionExpression.isPresent());
                List<DateTrunc> dateTruncs = partitionExpression.get().collectToList(DateTrunc.class::isInstance);
                Assertions.assertEquals(1, dateTruncs.size());
                Assertions.assertEquals(dateTruncs.get(0).getArgument(1).toString().toLowerCase(),
                        "'" + timeUnit + "'");
            }
            if (StringUtils.isEmpty(timeUnit)) {
                Assertions.assertFalse(partitionExpression.isPresent());
            }
            try {
                relatedTableColumnPairs.add(
                        Pair.of(info.getTableInfo().getTableName(), info.getColumn().toLowerCase()));
            } catch (Exception exception) {
                Assertions.fail();
            }
        }
        Assertions.assertEquals(expectTableColumnPairSet, relatedTableColumnPairs);
    }

    private static void failWith(RelatedTableInfo relatedTableInfo,
            String failInfo) {
        Assertions.assertFalse(relatedTableInfo.isPctPossible());
        Assertions.assertTrue(relatedTableInfo.getFailReason().contains(failInfo));
    }
}
