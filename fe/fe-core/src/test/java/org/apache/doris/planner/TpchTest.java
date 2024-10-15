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

package org.apache.doris.planner;

import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class TpchTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("db1");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        // Create tables.
        String lineitem = "CREATE TABLE db1.lineitem (\n"
                + "    l_shipdate    DATEV2 NOT NULL,\n"
                + "    l_orderkey    bigint NOT NULL,\n"
                + "    l_linenumber  int not null,\n"
                + "    l_partkey     int NOT NULL,\n"
                + "    l_suppkey     int not null,\n"
                + "    l_quantity    decimal(15, 2) NOT NULL,\n"
                + "    l_extendedprice  decimal(15, 2) NOT NULL,\n"
                + "    l_discount    decimal(15, 2) NOT NULL,\n"
                + "    l_tax         decimal(15, 2) NOT NULL,\n"
                + "    l_returnflag  VARCHAR(1) NOT NULL,\n"
                + "    l_linestatus  VARCHAR(1) NOT NULL,\n"
                + "    l_commitdate  DATEV2 NOT NULL,\n"
                + "    l_receiptdate DATEV2 NOT NULL,\n"
                + "    l_shipinstruct VARCHAR(25) NOT NULL,\n"
                + "    l_shipmode     VARCHAR(10) NOT NULL,\n"
                + "    l_comment      VARCHAR(44) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`l_shipdate`, `l_orderkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"lineitem_orders\"\n"
                + ");";

        String orders = "CREATE TABLE db1.orders  (\n"
                + "    o_orderkey       bigint NOT NULL,\n"
                + "    o_orderdate      DATEV2 NOT NULL,\n"
                + "    o_custkey        int NOT NULL,\n"
                + "    o_orderstatus    VARCHAR(1) NOT NULL,\n"
                + "    o_totalprice     decimal(15, 2) NOT NULL,\n"
                + "    o_orderpriority  VARCHAR(15) NOT NULL,\n"
                + "    o_clerk          VARCHAR(15) NOT NULL,\n"
                + "    o_shippriority   int NOT NULL,\n"
                + "    o_comment        VARCHAR(79) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`o_orderkey`, `o_orderdate`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"lineitem_orders\"\n"
                + ");";

        String partsupp = "CREATE TABLE db1.partsupp (\n"
                + "    ps_partkey          int NOT NULL,\n"
                + "    ps_suppkey     int NOT NULL,\n"
                + "    ps_availqty    int NOT NULL,\n"
                + "    ps_supplycost  decimal(15, 2)  NOT NULL,\n"
                + "    ps_comment     VARCHAR(199) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`ps_partkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"part_partsupp\"\n"
                + ");";

        String part = "CREATE TABLE db1.part (\n"
                + "    p_partkey          int NOT NULL,\n"
                + "    p_name        VARCHAR(55) NOT NULL,\n"
                + "    p_mfgr        VARCHAR(25) NOT NULL,\n"
                + "    p_brand       VARCHAR(10) NOT NULL,\n"
                + "    p_type        VARCHAR(25) NOT NULL,\n"
                + "    p_size        int NOT NULL,\n"
                + "    p_container   VARCHAR(10) NOT NULL,\n"
                + "    p_retailprice decimal(15, 2) NOT NULL,\n"
                + "    p_comment     VARCHAR(23) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`p_partkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"part_partsupp\"\n"
                + ");";

        String customer = "CREATE TABLE db1.customer (\n"
                + "    c_custkey     int NOT NULL,\n"
                + "    c_name        VARCHAR(25) NOT NULL,\n"
                + "    c_address     VARCHAR(40) NOT NULL,\n"
                + "    c_nationkey   int NOT NULL,\n"
                + "    c_phone       VARCHAR(15) NOT NULL,\n"
                + "    c_acctbal     decimal(15, 2)   NOT NULL,\n"
                + "    c_mktsegment  VARCHAR(10) NOT NULL,\n"
                + "    c_comment     VARCHAR(117) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c_custkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");";

        String supplier = "CREATE TABLE db1.supplier (\n"
                + "    s_suppkey       int NOT NULL,\n"
                + "    s_name        VARCHAR(25) NOT NULL,\n"
                + "    s_address     VARCHAR(40) NOT NULL,\n"
                + "    s_nationkey   int NOT NULL,\n"
                + "    s_phone       VARCHAR(15) NOT NULL,\n"
                + "    s_acctbal     decimal(15, 2) NOT NULL,\n"
                + "    s_comment     VARCHAR(101) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`s_suppkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");";

        String nation = "CREATE TABLE db1.`nation` (\n"
                + "  `n_nationkey` int(11) NOT NULL,\n"
                + "  `n_name`      varchar(25) NOT NULL,\n"
                + "  `n_regionkey` int(11) NOT NULL,\n"
                + "  `n_comment`   varchar(152) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`N_NATIONKEY`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");";

        String region = "CREATE TABLE db1.region  (\n"
                + "    r_regionkey      int NOT NULL,\n"
                + "    r_name       VARCHAR(25) NOT NULL,\n"
                + "    r_comment    VARCHAR(152)\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`r_regionkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");";

        createTables(lineitem, orders, partsupp, part, customer, supplier, nation, region);
        createView("create view db1.revenue0 (supplier_no, total_revenue) as\n"
                + "select\n"
                + "    l_suppkey,\n"
                + "    sum(l_extendedprice * (1 - l_discount))\n"
                + "from\n"
                + "    db1.lineitem\n"
                + "where\n"
                + "    l_shipdate >= date '1996-01-01'\n"
                + "    and l_shipdate < date '1996-01-01' + interval '3' month\n"
                + "group by\n"
                + "    l_suppkey;");
    }

    @Test
    public void testExplain() throws Exception {
        useDatabase("db1");
        String explain = getSQLPlanOrErrorMsg("SELECT\n"
                + "  o_year,\n"
                + "  sum(CASE\n"
                + "      WHEN nation = 'BRAZIL'\n"
                + "        THEN volume\n"
                + "      ELSE 0\n"
                + "      END) / sum(volume) AS mkt_share\n"
                + "FROM (\n"
                + "       SELECT\n"
                + "         extract(YEAR FROM o_orderdate)     AS o_year,\n"
                + "         l_extendedprice * (1 - l_discount) AS volume,\n"
                + "         n2.n_name                          AS nation\n"
                + "       FROM\n"
                + "         part,\n"
                + "         supplier,\n"
                + "         lineitem,\n"
                + "         orders,\n"
                + "         customer,\n"
                + "         nation n1,\n"
                + "         nation n2,\n"
                + "         region\n"
                + "       WHERE\n"
                + "         p_partkey = l_partkey\n"
                + "         AND s_suppkey = l_suppkey\n"
                + "         AND l_orderkey = o_orderkey\n"
                + "         AND o_custkey = c_custkey\n"
                + "         AND c_nationkey = n1.n_nationkey\n"
                + "         AND n1.n_regionkey = r_regionkey\n"
                + "         AND r_name = 'AMERICA'\n"
                + "         AND s_nationkey = n2.n_nationkey\n"
                + "         AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'\n"
                + "         AND p_type = 'ECONOMY ANODIZED STEEL'\n"
                + "     ) AS all_nations\n"
                + "GROUP BY\n"
                + "  o_year\n"
                + "ORDER BY\n"
                + "  o_year");

        Assert.assertTrue(explain.contains("db1.lineitem(lineitem)"));
    }
}
