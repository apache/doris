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

package org.apache.doris.nereids.lineage;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class LineageInfoExtractorTest extends TestWithFeService {

    private String tpchDb;

    @Override
    protected void runBeforeAll() throws Exception {
        tpchDb = "lineage_tpch_" + UUID.randomUUID().toString().replace("-", "");
        createDatabaseAndUse(tpchDb);
        createTpchSourceTables();
        createTpchTargetTables();
    }

    @Override
    protected void runAfterAll() throws Exception {
        if (tpchDb != null) {
            dropDatabase(tpchDb);
        }
    }

    @Test
    public void testJoinAndFilterLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_join_customer")
                + " select c.c_name as customer_name"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("customer") + " c on o.o_custkey = c.c_custkey"
                + " where o.o_orderstatus = 'F'";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_join_customer");
        assertDirectContainsAny(lineageInfo, "customer_name",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "customer_name",
                LineageInfo.IndirectLineageType.JOIN, LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "customer");
    }

    @Test
    public void testFilterOnlyLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_filter_only")
                + " select l.l_orderkey as orderkey, l.l_extendedprice as price"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_shipdate >= date '1995-01-01' and l.l_shipmode = 'AIR'";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_filter_only");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "orderkey", LineageInfo.IndirectLineageType.FILTER);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "lineitem");
    }

    @Test
    public void testGroupBySumLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_group_sum")
                + " select o.o_custkey as custkey, sum(l.l_extendedprice) as total_price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey"
                + " where l.l_returnflag = 'R'"
                + " group by o.o_custkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_group_sum");
        assertDirectContainsAny(lineageInfo, "custkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "total_price", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "custkey", LineageInfo.IndirectLineageType.GROUP_BY);
        assertIndirectContains(lineageInfo, "total_price",
                LineageInfo.IndirectLineageType.JOIN,
                LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testGroupByExpressionLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_group_only")
                + " select sum(l.l_extendedprice) as total_price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey"
                + " group by year(o.o_orderdate)";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_group_only");
        assertDirectContains(lineageInfo, "total_price", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "total_price",
                LineageInfo.IndirectLineageType.JOIN,
                LineageInfo.IndirectLineageType.GROUP_BY);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testSortLimitLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_sort_limit")
                + " select l.l_orderkey as orderkey, l.l_extendedprice as price"
                + " from " + dbTable("lineitem") + " l"
                + " order by l.l_shipdate desc"
                + " limit 10";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_sort_limit");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "orderkey", LineageInfo.IndirectLineageType.SORT);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.SORT);
        assertTableLineageContains(lineageInfo, "lineitem");
    }

    @Test
    public void testWindowRowNumberLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_window_rn")
                + " select o.o_orderkey as orderkey,"
                + " row_number() over (partition by o.o_custkey order by o.o_orderdate desc) as rn"
                + " from " + dbTable("orders") + " o";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_window_rn");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "rn", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "rn", LineageInfo.IndirectLineageType.WINDOW);
        assertTableLineageContains(lineageInfo, "orders");
    }

    @Test
    public void testWindowRunningSumLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_window_sum")
                + " select l.l_orderkey as orderkey,"
                + " sum(l.l_extendedprice) over ("
                + " partition by l.l_orderkey"
                + " order by l.l_shipdate"
                + " rows between unbounded preceding and current row"
                + " ) as running_sum"
                + " from " + dbTable("lineitem") + " l";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_window_sum");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "running_sum", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "running_sum", LineageInfo.IndirectLineageType.WINDOW);
        assertTableLineageContains(lineageInfo, "lineitem");
    }

    @Test
    public void testConditionalCaseLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_conditional_case")
                + " select o.o_orderkey as orderkey,"
                + " case when o.o_orderstatus = 'F'"
                + " then l.l_extendedprice"
                + " else cast(0 as decimal(18,2))"
                + " end as final_price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_conditional_case");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "final_price", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "final_price", LineageInfo.IndirectLineageType.CONDITIONAL);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testConditionalIfLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_conditional_if")
                + " select o.o_orderkey as orderkey,"
                + " if(o.o_orderstatus = 'F', l.l_extendedprice, cast(0 as decimal(18,2))) as final_price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_conditional_if");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "final_price", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "final_price", LineageInfo.IndirectLineageType.CONDITIONAL);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testConditionalCoalesceLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_conditional_coalesce")
                + " select l.l_orderkey as orderkey,"
                + " coalesce(l.l_shipmode, l.l_shipinstruct, 'UNKNOWN') as ship_info"
                + " from " + dbTable("lineitem") + " l";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_conditional_coalesce");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "ship_info", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "ship_info", LineageInfo.IndirectLineageType.CONDITIONAL);
        assertTableLineageContains(lineageInfo, "lineitem");
    }

    @Test
    public void testJoinFilterGroupByLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_region_revenue")
                + " select n.n_name as nation_name,"
                + " sum(l.l_extendedprice * (1 - l.l_discount)) as revenue"
                + " from " + dbTable("customer") + " c"
                + " join " + dbTable("orders") + " o on c.c_custkey = o.o_custkey"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey"
                + " join " + dbTable("supplier") + " s on l.l_suppkey = s.s_suppkey"
                + " join " + dbTable("nation") + " n on c.c_nationkey = n.n_nationkey"
                + " join " + dbTable("region") + " r on n.n_regionkey = r.r_regionkey"
                + " where r.r_name = 'ASIA'"
                + " and o.o_orderdate >= date '1994-01-01'"
                + " and o.o_orderdate < date '1995-01-01'"
                + " group by n.n_name";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_region_revenue");
        assertDirectContainsAny(lineageInfo, "nation_name",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "revenue", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "nation_name", LineageInfo.IndirectLineageType.GROUP_BY);
        assertIndirectContains(lineageInfo, "revenue",
                LineageInfo.IndirectLineageType.JOIN,
                LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "customer", "orders", "lineitem", "supplier", "nation", "region");
    }

    @Test
    public void testCtasSortLimitLineage() throws Exception {
        String sql = "create table " + dbTable("tgt_ctas_top10")
                + " distributed by hash(orderkey) buckets 1"
                + " properties('replication_num'='1') as"
                + " select o.o_orderkey as orderkey, o.o_totalprice as price"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderdate >= date '1996-01-01'"
                + " order by o.o_totalprice desc"
                + " limit 10";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_ctas_top10");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.SORT);
        assertTableLineageContains(lineageInfo, "orders");
    }

    @Test
    public void testCtasJoinFilterLineage() throws Exception {
        String sql = "create table " + dbTable("tgt_ctas_join_filter")
                + " distributed by hash(orderkey) buckets 1"
                + " properties('replication_num'='1') as"
                + " select o.o_orderkey as orderkey, l.l_extendedprice as price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey"
                + " where o.o_orderstatus = 'F'";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_ctas_join_filter");
        assertSourceCommand(lineageInfo, CreateTableCommand.class);
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "orderkey", LineageInfo.IndirectLineageType.JOIN);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testInsertOverwriteSortLimitLineage() throws Exception {
        String sql = "insert overwrite table " + dbTable("tgt_sort_limit")
                + " select l.l_orderkey as orderkey, l.l_extendedprice as price"
                + " from " + dbTable("lineitem") + " l"
                + " order by l.l_shipdate desc"
                + " limit 10";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_sort_limit");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "orderkey", LineageInfo.IndirectLineageType.SORT);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.SORT);
        assertTableLineageContains(lineageInfo, "lineitem");
    }

    @Test
    public void testInsertOverwriteGroupByLineage() throws Exception {
        String sql = "insert overwrite table " + dbTable("tgt_group_sum")
                + " select o.o_custkey as custkey, sum(l.l_extendedprice) as total_price"
                + " from " + dbTable("orders") + " o"
                + " join " + dbTable("lineitem") + " l on o.o_orderkey = l.l_orderkey"
                + " where l.l_returnflag = 'R'"
                + " group by o.o_custkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_group_sum");
        assertSourceCommand(lineageInfo, InsertOverwriteTableCommand.class);
        assertDirectContainsAny(lineageInfo, "custkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "total_price", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "custkey", LineageInfo.IndirectLineageType.GROUP_BY);
        assertIndirectContains(lineageInfo, "total_price",
                LineageInfo.IndirectLineageType.JOIN,
                LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testUnionAllOrdersLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_all_orders")
                + " select o.o_orderkey as orderkey, o.o_totalprice as price, 'F' as tag"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union all"
                + " select l.l_orderkey, l.l_extendedprice, 'R' as tag"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_returnflag = 'R'";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_all_orders");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "tag",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "orderkey", LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testUnionDistinctCustLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_distinct_cust")
                + " select o.o_custkey as custkey"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union"
                + " select c.c_custkey"
                + " from " + dbTable("customer") + " c"
                + " where c.c_nationkey >= 0";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_distinct_cust");
        assertDirectContainsAny(lineageInfo, "custkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "custkey", LineageInfo.IndirectLineageType.FILTER);
        assertTableLineageContains(lineageInfo, "orders", "customer");
    }

    @Test
    public void testUnionAllDerivedAggregateLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_agg")
                + " select custkey, sum(price) as total_price"
                + " from ("
                + " select o.o_custkey as custkey, o.o_totalprice as price"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderdate >= date '1994-01-01'"
                + " union all"
                + " select c.c_custkey, cast(c.c_nationkey as decimal(18,2))"
                + " from " + dbTable("customer") + " c"
                + " where c.c_nationkey >= 0"
                + " ) u"
                + " group by custkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_agg");
        assertDirectContainsAny(lineageInfo, "custkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "total_price", LineageInfo.DirectLineageType.AGGREGATION);
        assertIndirectContains(lineageInfo, "custkey", LineageInfo.IndirectLineageType.GROUP_BY);
        assertTableLineageContains(lineageInfo, "orders", "customer");
    }

    @Test
    public void testCteUnionJoinLineage() throws Exception {
        String sql = "with order_keys as ("
                + " select l.l_orderkey as orderkey"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_shipdate >= date '1996-01-01'"
                + " union all"
                + " select o.o_orderkey"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " )"
                + " insert into " + dbTable("tgt_union_join")
                + " select c.c_name as customer_name, o.o_orderstatus as order_status"
                + " from " + dbTable("orders") + " o"
                + " join order_keys k on o.o_orderkey = k.orderkey"
                + " join " + dbTable("customer") + " c on o.o_custkey = c.c_custkey";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_join");
        assertDirectContainsAny(lineageInfo, "customer_name",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "order_status",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "customer_name", LineageInfo.IndirectLineageType.JOIN);
        assertTableLineageContains(lineageInfo, "orders", "lineitem", "customer");
    }

    @Test
    public void testUnionWindowLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_window")
                + " select orderkey, rn"
                + " from ("
                + " select o.o_orderkey as orderkey,"
                + " row_number() over (partition by o.o_custkey order by o.o_orderdate desc) as rn"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union all"
                + " select l.l_orderkey,"
                + " row_number() over (partition by l.l_orderkey order by l.l_shipdate desc) as rn"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_returnflag = 'R'"
                + " ) u";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_window");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "rn", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "rn", LineageInfo.IndirectLineageType.WINDOW);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testUnionInSubqueryLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_in")
                + " select l.l_orderkey as orderkey, l.l_extendedprice as price"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_orderkey in ("
                + " select o.o_orderkey"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderdate >= date '1995-01-01'"
                + " union all"
                + " select l2.l_orderkey"
                + " from " + dbTable("lineitem") + " l2"
                + " where l2.l_shipdate < date '1993-01-01'"
                + " )";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_in");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertTableLineageContains(lineageInfo, "lineitem", "orders");
    }

    @Test
    public void testUnionConditionalLineage() throws Exception {
        String sql = "insert into " + dbTable("tgt_union_conditional")
                + " select orderkey,"
                + " case when orderstatus = 'F' then price else cast(0 as decimal(18,2)) end as final_price"
                + " from ("
                + " select o.o_orderkey as orderkey, o.o_totalprice as price, o.o_orderstatus as orderstatus"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union all"
                + " select l.l_orderkey, l.l_extendedprice, l.l_returnflag as orderstatus"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_returnflag = 'R'"
                + " ) u";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_conditional");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContains(lineageInfo, "final_price", LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "final_price", LineageInfo.IndirectLineageType.CONDITIONAL);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testCtasUnionAllLineage() throws Exception {
        String sql = "create table " + dbTable("tgt_ctas_union")
                + " distributed by hash(orderkey) buckets 1"
                + " properties('replication_num'='1') as"
                + " select orderkey, price, tag"
                + " from ("
                + " select o.o_orderkey as orderkey, o.o_totalprice as price, 'F' as tag"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union all"
                + " select l.l_orderkey, l.l_extendedprice, 'R' as tag"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_returnflag = 'R'"
                + " ) u";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_ctas_union");
        assertSourceCommand(lineageInfo, CreateTableCommand.class);
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "tag",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    @Test
    public void testInsertOverwriteUnionAllSortLimitLineage() throws Exception {
        String sql = "insert overwrite table " + dbTable("tgt_union_all_orders")
                + " select orderkey, price, tag"
                + " from ("
                + " select o.o_orderkey as orderkey, o.o_totalprice as price, 'F' as tag"
                + " from " + dbTable("orders") + " o"
                + " where o.o_orderstatus = 'F'"
                + " union all"
                + " select l.l_orderkey, l.l_extendedprice, 'R' as tag"
                + " from " + dbTable("lineitem") + " l"
                + " where l.l_returnflag = 'R'"
                + " ) u"
                + " order by price desc"
                + " limit 100";
        LineageInfo lineageInfo = buildLineageInfo(sql);

        assertTargetTable(lineageInfo, "tgt_union_all_orders");
        assertDirectContainsAny(lineageInfo, "orderkey",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "price",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertDirectContainsAny(lineageInfo, "tag",
                LineageInfo.DirectLineageType.IDENTITY, LineageInfo.DirectLineageType.TRANSFORMATION);
        assertIndirectContains(lineageInfo, "price", LineageInfo.IndirectLineageType.SORT);
        assertTableLineageContains(lineageInfo, "orders", "lineitem");
    }

    private void createTpchSourceTables() throws Exception {
        createTables(
                "create table " + dbTable("orders") + " ("
                        + "o_orderkey bigint,"
                        + "o_custkey bigint,"
                        + "o_orderstatus char(1),"
                        + "o_orderdate date,"
                        + "o_totalprice decimal(18,2)"
                        + ") distributed by hash(o_orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("customer") + " ("
                        + "c_custkey bigint,"
                        + "c_name varchar(25),"
                        + "c_nationkey bigint"
                        + ") distributed by hash(c_custkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("lineitem") + " ("
                        + "l_orderkey bigint,"
                        + "l_suppkey bigint,"
                        + "l_extendedprice decimal(18,2),"
                        + "l_shipdate date,"
                        + "l_shipmode varchar(10),"
                        + "l_shipinstruct varchar(25),"
                        + "l_discount decimal(18,2),"
                        + "l_returnflag char(1)"
                        + ") distributed by hash(l_orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("supplier") + " ("
                        + "s_suppkey bigint,"
                        + "s_nationkey bigint"
                        + ") distributed by hash(s_suppkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("nation") + " ("
                        + "n_nationkey bigint,"
                        + "n_name varchar(25),"
                        + "n_regionkey bigint"
                        + ") distributed by hash(n_nationkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("region") + " ("
                        + "r_regionkey bigint,"
                        + "r_name varchar(25)"
                        + ") distributed by hash(r_regionkey) buckets 1 properties('replication_num'='1');"
        );
    }

    private void createTpchTargetTables() throws Exception {
        createTables(
                "create table " + dbTable("tgt_join_customer") + " ("
                        + "customer_name varchar(25)"
                        + ") distributed by hash(customer_name) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_filter_only") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_group_sum") + " ("
                        + "custkey bigint,"
                        + "total_price decimal(18,2)"
                        + ") distributed by hash(custkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_group_only") + " ("
                        + "total_price decimal(18,2)"
                        + ") distributed by hash(total_price) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_sort_limit") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_window_rn") + " ("
                        + "orderkey bigint,"
                        + "rn bigint"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_window_sum") + " ("
                        + "orderkey bigint,"
                        + "running_sum decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_conditional_case") + " ("
                        + "orderkey bigint,"
                        + "final_price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_conditional_if") + " ("
                        + "orderkey bigint,"
                        + "final_price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_conditional_coalesce") + " ("
                        + "orderkey bigint,"
                        + "ship_info varchar(25)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_region_revenue") + " ("
                        + "nation_name varchar(25),"
                        + "revenue decimal(18,2)"
                        + ") distributed by hash(nation_name) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_all_orders") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2),"
                        + "tag varchar(8)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_distinct_cust") + " ("
                        + "custkey bigint"
                        + ") distributed by hash(custkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_agg") + " ("
                        + "custkey bigint,"
                        + "total_price decimal(18,2)"
                        + ") distributed by hash(custkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_join") + " ("
                        + "customer_name varchar(25),"
                        + "order_status char(1)"
                        + ") distributed by hash(customer_name) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_window") + " ("
                        + "orderkey bigint,"
                        + "rn bigint"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_in") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_union_conditional") + " ("
                        + "orderkey bigint,"
                        + "final_price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_ctas_top10") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_ctas_union") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2),"
                        + "tag varchar(8)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');",
                "create table " + dbTable("tgt_ctas_join_filter") + " ("
                        + "orderkey bigint,"
                        + "price decimal(18,2)"
                        + ") distributed by hash(orderkey) buckets 1 properties('replication_num'='1');"
        );
    }

    private LineageInfo buildLineageInfo(String sql) throws Exception {
        StatementContext statementContext = createStatementCtx(sql);
        LogicalPlan parsedPlan = new NereidsParser().parseSingle(sql);
        InsertIntoTableCommand insertCommand = toInsertCommand(parsedPlan);
        Class<? extends Command> sourceCommand = ((Command) parsedPlan).getClass();

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(insertCommand, statementContext);
        logicalPlanAdapter.setOrigStmt(statementContext.getOriginStatement());

        StmtExecutor executor = new StmtExecutor(connectContext, logicalPlanAdapter);
        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        insertCommand.initPlan(connectContext, executor, false);
        Plan lineagePlan = insertCommand.getLineagePlan().orElse(null);
        Assertions.assertNotNull(lineagePlan);

        LineageInfo lineageInfo = LineageUtils.buildLineageInfo(lineagePlan, sourceCommand, connectContext, executor);
        Assertions.assertNotNull(lineageInfo);
        return lineageInfo;
    }

    private InsertIntoTableCommand toInsertCommand(LogicalPlan parsedPlan) throws Exception {
        if (parsedPlan instanceof InsertIntoTableCommand) {
            return (InsertIntoTableCommand) parsedPlan;
        }
        if (parsedPlan instanceof InsertOverwriteTableCommand) {
            InsertOverwriteTableCommand overwrite = (InsertOverwriteTableCommand) parsedPlan;
            return new InsertIntoTableCommand(overwrite.getLogicalQuery(), Optional.empty(), Optional.empty(),
                    Optional.empty(), true, Optional.empty());
        }
        if (parsedPlan instanceof CreateTableCommand) {
            CreateTableCommand createTableCommand = (CreateTableCommand) parsedPlan;
            Assertions.assertTrue(createTableCommand.isCtasCommand());
            LogicalPlan query = createTableCommand.getCtasQuery()
                    .orElseThrow(() -> new IllegalStateException("ctas query is missing"));
            LogicalPlan sink = UnboundTableSinkCreator.createUnboundTableSink(
                    createTableCommand.getCreateTableInfo().getTableNameParts(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    query);
            return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty(), true,
                    Optional.empty());
        }
        throw new IllegalArgumentException("unsupported command type: " + parsedPlan.getClass().getSimpleName());
    }

    private void assertTargetTable(LineageInfo lineageInfo, String tableName) {
        Assertions.assertNotNull(lineageInfo.getTargetTable());
        Assertions.assertEquals(tableName, lineageInfo.getTargetTable().getName());
    }

    private void assertSourceCommand(LineageInfo lineageInfo, Class<? extends Command> sourceCommand) {
        Assertions.assertNotNull(lineageInfo.getContext());
        Assertions.assertEquals(sourceCommand, lineageInfo.getContext().getSourceCommand());
    }

    private void assertTableLineageContains(LineageInfo lineageInfo, String... tableNames) {
        Set<String> actual = lineageInfo.getTableLineageSet().stream()
                .map(TableIf::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        for (String tableName : tableNames) {
            Assertions.assertTrue(actual.contains(tableName.toLowerCase()),
                    "missing table lineage: " + tableName);
        }
    }

    private void assertIndirectContains(LineageInfo lineageInfo, String outputName,
                                        LineageInfo.IndirectLineageType... types) {
        Set<LineageInfo.IndirectLineageType> actual = getIndirectTypes(lineageInfo, outputName);
        for (LineageInfo.IndirectLineageType type : types) {
            Assertions.assertTrue(actual.contains(type),
                    "missing indirect type " + type + " for output " + outputName);
        }
    }

    private void assertDirectContains(LineageInfo lineageInfo, String outputName,
                                      LineageInfo.DirectLineageType type) {
        Set<LineageInfo.DirectLineageType> actual = getDirectTypes(lineageInfo, outputName);
        Assertions.assertTrue(actual.contains(type),
                "missing direct type " + type + " for output " + outputName);
    }

    private void assertDirectContainsAny(LineageInfo lineageInfo, String outputName,
                                         LineageInfo.DirectLineageType... types) {
        Set<LineageInfo.DirectLineageType> actual = getDirectTypes(lineageInfo, outputName);
        for (LineageInfo.DirectLineageType type : types) {
            if (actual.contains(type)) {
                return;
            }
        }
        Assertions.fail("missing direct types " + String.join(", ",
                java.util.Arrays.stream(types).map(Enum::name).collect(Collectors.toList()))
                + " for output " + outputName);
    }

    private Set<LineageInfo.IndirectLineageType> getIndirectTypes(LineageInfo lineageInfo, String outputName) {
        Set<LineageInfo.IndirectLineageType> actual = new HashSet<>();
        Map<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> indirectMap =
                lineageInfo.getInDirectLineageMapByDataset();
        for (Map.Entry<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> entry
                : indirectMap.entrySet()) {
            if (entry.getKey().getName().equalsIgnoreCase(outputName)) {
                actual.addAll(entry.getValue().keySet());
            }
        }
        Map<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> perOutputMap =
                lineageInfo.getOutputIndirectLineageMap();
        for (Map.Entry<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> entry
                : perOutputMap.entrySet()) {
            if (entry.getKey().getName().equalsIgnoreCase(outputName)) {
                actual.addAll(entry.getValue().keySet());
            }
        }
        return actual;
    }

    private Set<LineageInfo.DirectLineageType> getDirectTypes(LineageInfo lineageInfo, String outputName) {
        Map<SlotReference, SetMultimap<LineageInfo.DirectLineageType, Expression>> directMap =
                lineageInfo.getDirectLineageMap();
        for (Map.Entry<SlotReference, SetMultimap<LineageInfo.DirectLineageType, Expression>> entry
                : directMap.entrySet()) {
            if (entry.getKey().getName().equalsIgnoreCase(outputName)) {
                return new HashSet<>(entry.getValue().keySet());
            }
        }
        return Collections.emptySet();
    }

    private String dbTable(String table) {
        return tpchDb + "." + table;
    }
}
