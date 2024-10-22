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

        createTable("CREATE TABLE `orders_list_partition` (\n"
                + "      `o_orderkey` BIGINT not NULL,\n"
                + "      `o_custkey` INT NULL,\n"
                + "      `o_orderstatus` VARCHAR(1) NULL,\n"
                + "      `o_totalprice` DECIMAL(15, 2)  NULL,\n"
                + "      `o_orderpriority` VARCHAR(15) NULL,\n"
                + "      `o_clerk` VARCHAR(15) NULL,\n"
                + "      `o_shippriority` INT NULL,\n"
                + "      `o_comment` VARCHAR(79) NULL,\n"
                + "      `o_orderdate` DATE NULL\n"
                + "    ) ENGINE=OLAP\n"
                + "    DUPLICATE KEY(`o_orderkey`, `o_custkey`)\n"
                + "    COMMENT 'OLAP'\n"
                + "    PARTITION BY list(o_orderkey) (\n"
                + "    PARTITION p1 VALUES in ('1'),\n"
                + "    PARTITION p2 VALUES in ('2'),\n"
                + "    PARTITION p3 VALUES in ('3'),\n"
                + "    PARTITION p4 VALUES in ('4')\n"
                + "    )\n"
                + "    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 3\n"
                + "    PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + "    )");
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

        createTable("CREATE TABLE `lineitem_no_data` (\n"
                + "      `l_orderkey` BIGINT NOT NULL,\n"
                + "      `l_linenumber` INT NOT NULL,\n"
                + "      `l_partkey` INT NOT NULL,\n"
                + "      `l_suppkey` INT NOT NULL,\n"
                + "      `l_quantity` DECIMAL(15, 2) NOT NULL,\n"
                + "      `l_extendedprice` DECIMAL(15, 2) NOT NULL,\n"
                + "      `l_discount` DECIMAL(15, 2) NOT NULL,\n"
                + "      `l_tax` DECIMAL(15, 2) NOT NULL,\n"
                + "      `l_returnflag` VARCHAR(1) NOT NULL,\n"
                + "      `l_linestatus` VARCHAR(1) NOT NULL,\n"
                + "      `l_commitdate` DATE NOT NULL,\n"
                + "      `l_receiptdate` DATE NOT NULL,\n"
                + "      `l_shipinstruct` VARCHAR(25) NOT NULL,\n"
                + "      `l_shipmode` VARCHAR(10) NOT NULL,\n"
                + "      `l_comment` VARCHAR(44) NOT NULL,\n"
                + "      `l_shipdate` DATE NOT NULL\n"
                + "    ) ENGINE=OLAP\n"
                + "    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )\n"
                + "    COMMENT 'OLAP'\n"
                + "    AUTO PARTITION BY range (date_trunc(`l_shipdate`, 'day')) ()\n"
                + "    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n"
                + "    PROPERTIES (\n"
                + "       \"replication_num\" = \"1\"\n"
                + "    );\n"
                + "\n");

        createTable("CREATE TABLE `test1` (\n"
                + "`pre_batch_no` VARCHAR(100) NULL COMMENT 'pre_batch_no',\n"
                + "`batch_no` VARCHAR(100) NULL COMMENT 'batch_no',\n"
                + "`vin_type1` VARCHAR(50) NULL COMMENT 'vin',\n"
                + "`upgrade_day` date COMMENT 'upgrade_day'\n"
                + ") ENGINE=OLAP\n"
                + "unique KEY(`pre_batch_no`,`batch_no`, `vin_type1`, `upgrade_day`)\n"
                + "COMMENT 'OLAP'\n"
                + "PARTITION BY RANGE(`upgrade_day`)\n"
                + "(\n"
                + "FROM (\"2024-03-20\") TO (\"2024-03-31\") INTERVAL 1 DAY\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`vin_type1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "       \"replication_num\" = \"1\"\n"
                + ");\n"
        );

        createTable("CREATE TABLE `test2` (\n"
                + "`batch_no` VARCHAR(100) NULL COMMENT 'batch_no',\n"
                + "`vin_type2` VARCHAR(50) NULL COMMENT 'vin',\n"
                + "`status` VARCHAR(50) COMMENT 'status',\n"
                + "`upgrade_day` date  not null COMMENT 'upgrade_day' \n"
                + ") ENGINE=OLAP\n"
                + "Duplicate KEY(`batch_no`,`vin_type2`)\n"
                + "COMMENT 'OLAP'\n"
                + "PARTITION BY RANGE(`upgrade_day`)\n"
                + "(\n"
                + "FROM (\"2024-01-01\") TO (\"2024-01-10\") INTERVAL 1 DAY\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`vin_type2`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "       \"replication_num\" = \"1\"\n"
                + ");\n"
        );
        createTable("CREATE TABLE `test3` (\n"
                + "  `id` VARCHAR(36) NOT NULL COMMENT 'id',\n"
                + "  `created_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT ''\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`)\n"
                + "COMMENT ''\n"
                + "PARTITION BY RANGE(`created_time`)\n"
                + "(PARTITION P_2024071713 VALUES [('2024-07-17 13:00:00'), ('2024-07-17 14:00:00')),\n"
                + "PARTITION P_2024071714 VALUES [('2024-07-17 14:00:00'), ('2024-07-17 15:00:00')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");\n");
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules("OLAP_SCAN_PARTITION_PRUNE,PRUNE_EMPTY_PARTITION");
    }

    // Test when join both side are all partition table and partition column name is same
    @Test
    public void joinPartitionNameSameTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select t1.upgrade_day, t2.batch_no, count(*) "
                                + "from test2 t2 join test1 t1 on "
                                + "t1.upgrade_day = t2.upgrade_day "
                                + "group by t1.upgrade_day, t2.batch_no;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("upgrade_day", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "test1",
                                    "upgrade_day",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoWhenAutoPartitionTest() {
        PlanChecker.from(connectContext)
                .checkExplain("select * from "
                                + "(select * from lineitem_no_data "
                                + "where l_shipdate >= \"2023-12-01\" and l_shipdate <= \"2023-12-03\") t1 "
                                + "left join "
                                + "(select * from orders where o_orderdate >= \"2023-12-01\" and o_orderdate <= \"2023-12-03\" ) t2 "
                                + "on t1.l_orderkey = o_orderkey;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem_no_data",
                                    "L_SHIPDATE",
                                    true);
                        });
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
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY limit 1",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.isPctPossible());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("ship_data_alias", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoLeftAntiJoinTest() {
        PlanChecker.from(connectContext)
                .checkExplain("        select l_shipdate, l_orderkey, count(l_shipdate), count(l_orderkey) \n"
                                + "        from lineitem_list_partition\n"
                                + "        left anti join orders_list_partition\n"
                                + "        on l_shipdate = o_orderdate\n"
                                + "        group by l_shipdate, l_orderkey",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_orderkey", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem_list_partition",
                                    "l_orderkey",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoSelfJoinTest() {
        PlanChecker.from(connectContext)
                .checkExplain("    select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1\n"
                                + "    from lineitem_list_partition t1\n"
                                + "    join lineitem_list_partition t2\n"
                                + "    on t1.l_shipdate = t2.l_shipdate\n"
                                + "    group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_orderkey", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "self join doesn't support partition update"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
                        });

        PlanChecker.from(connectContext)
                .checkExplain("    select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1\n"
                                + "    from lineitem_list_partition t1\n"
                                + "    left outer join lineitem_list_partition t2\n"
                                + "    on t1.l_shipdate = t2.l_shipdate\n"
                                + "    group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_orderkey", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "self join doesn't support partition update"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
                        });

        PlanChecker.from(connectContext)
                .checkExplain("    select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1\n"
                                + "    from lineitem_list_partition t1\n"
                                + "    right outer join lineitem_list_partition t2\n"
                                + "    on t1.l_shipdate = t2.l_shipdate\n"
                                + "    group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_orderkey", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "partition column is in un supported join null generate side"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("o_orderdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "partition column is in un supported join null generate side"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("PS_SUPPLYCOST", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "self join doesn't support partition update"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("L_SHIPDATE", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "window partition sets doesn't contain the target partition"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
                        });
    }

    @Test
    public void getRelatedTableInfoTestWithLimitTest() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT l.L_SHIPDATE, l.L_ORDERKEY "
                               + "FROM "
                               + "lineitem as l limit 1",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("l_shipdate", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void testPartitionDateTrunc() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT date_trunc(t1.L_SHIPDATE, 'hour') as date_alias, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("date_alias", "day",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void testPartitionDateTruncShouldNotTrack() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT date_trunc(t1.L_SHIPDATE, 'day') as date_alias, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("date_alias", "hour",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            Assertions.assertTrue(relatedTableInfo.getFailReason().contains(
                                    "partition column time unit level should be greater than sql select column"));
                            Assertions.assertFalse(relatedTableInfo.isPctPossible());
                        });
    }

    @Test
    public void testPartitionDateTruncShouldTrack() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT date_trunc(t1.L_SHIPDATE, 'day') as date_alias, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
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
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("date_alias", "month",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void testPartitionDateTruncInGroupByShouldTrack() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT date_trunc(t1.L_SHIPDATE, 'day') as date_alias, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
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
                                + "date_alias, "
                                + "t2.O_ORDERDATE, "
                                + "t1.L_QUANTITY, "
                                + "t2.O_ORDERSTATUS;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("date_alias", "month",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void testPartitionDateTruncExpressionInGroupByShouldTrack() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT date_trunc(t1.L_SHIPDATE, 'day') as date_alias, t2.O_ORDERDATE, t1.L_QUANTITY, t2.O_ORDERSTATUS, "
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
                                + "date_trunc(t1.L_SHIPDATE, 'day'), "
                                + "t2.O_ORDERDATE, "
                                + "t1.L_QUANTITY, "
                                + "t2.O_ORDERSTATUS;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("date_alias", "month",
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "lineitem",
                                    "L_SHIPDATE",
                                    true);
                        });
    }

    @Test
    public void getRelatedTableInfoWhenMultiBaseTablePartition() {
        PlanChecker.from(connectContext)
                .checkExplain("select\n"
                                + "t1.upgrade_day,\n"
                                + "t1.batch_no,\n"
                                + "t1.vin_type1\n"
                                + "from\n"
                                + "(\n"
                                + "SELECT\n"
                                + "batch_no,\n"
                                + "vin_type1,\n"
                                + "upgrade_day\n"
                                + "FROM test1\n"
                                + "where batch_no like 'c%'\n"
                                + "group by batch_no,\n"
                                + "vin_type1,\n"
                                + "upgrade_day\n"
                                + ")t1\n"
                                + "left join\n"
                                + "(\n"
                                + "select\n"
                                + "batch_no,\n"
                                + "vin_type2,\n"
                                + "status\n"
                                + "from test2\n"
                                + "group by batch_no,\n"
                                + "vin_type2,\n"
                                + "status\n"
                                + ")t2 on t1.vin_type1 = t2.vin_type2;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("upgrade_day", null,
                                            rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "test1",
                                    "upgrade_day",
                                    true);
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

    @Test
    public void getRelatedTableInfoWhenMultiPartitionExprs() {
        PlanChecker.from(connectContext)
                .checkExplain("select  id, date_trunc(created_time, 'minute') as created_time_minute,"
                                + "        min(created_time) as start_time,"
                                + "        if(count(id) > 0, 1, 0) as status\n"
                                + "        from test3 \n"
                                + "        group by id, date_trunc(created_time, 'minute')",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("created_time_minute",
                                            "day", rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "test3",
                                    "created_time",
                                    true);
                        });
        PlanChecker.from(connectContext)
                .checkExplain("select  id, date_trunc(created_time, 'hour') as created_time_hour,"
                                + "        min(created_time) as start_time\n"
                                + "        from test3 \n"
                                + "        group by id, date_trunc(created_time, 'minute'),"
                                + "        date_trunc(created_time, 'hour');",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            RelatedTableInfo relatedTableInfo =
                                    MaterializedViewUtils.getRelatedTableInfo("created_time_hour",
                                            "day", rewrittenPlan, nereidsPlanner.getCascadesContext());
                            checkRelatedTableInfo(relatedTableInfo,
                                    "test3",
                                    "created_time",
                                    true);
                        });
    }

    private void checkRelatedTableInfo(RelatedTableInfo relatedTableInfo,
            String expectTableName,
            String expectColumnName,
            boolean pctPossible) {
        Assertions.assertNotNull(relatedTableInfo);
        BaseTableInfo relatedBaseTableInfo = relatedTableInfo.getTableInfo();
        try {
            TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(relatedBaseTableInfo.getCtlId())
                    .getDbOrAnalysisException(relatedBaseTableInfo.getDbId())
                    .getTableOrAnalysisException(relatedBaseTableInfo.getTableId());
            Assertions.assertEquals(tableIf.getName(), expectTableName);
        } catch (Exception exception) {
            Assertions.fail();
        }
        Assertions.assertEquals(relatedTableInfo.getColumn().toLowerCase(), expectColumnName.toLowerCase());
        Assertions.assertTrue(pctPossible);
    }
}
