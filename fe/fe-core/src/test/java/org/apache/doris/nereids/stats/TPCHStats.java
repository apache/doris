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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.datasets.tpch.TPCHUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.statistics.ColumnLevelStatisticCache;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

// CHECKSTYLE OFF
public class TPCHStats extends TestStats {

    @Test
    public void testTPCHStats() throws Exception {
        run();
    }

    @Override
    protected void initEnv() throws Exception {
        createDatabase("tpch");
        connectContext.setDatabase("default_cluster:tpch");
        TPCHUtils.createTables(this);
    }

    @Override
    protected void initQError() {
        avgQError = 385.5745449282787;
    }

    @Override
    protected void initColNameToType() {

        colType.put("c_custkey", ScalarType.INT);

        colType.put("c_name", ScalarType.VARCHAR);

        colType.put("c_address", ScalarType.VARCHAR);

        colType.put("c_nationkey", ScalarType.INT);

        colType.put("c_phone", ScalarType.VARCHAR);

        colType.put("c_acctbal", ScalarType.createDecimalType(15,2));

        colType.put("c_mktsegment", ScalarType.VARCHAR);

        colType.put("c_comment", ScalarType.VARCHAR);

        colType.put("l_shipdate", ScalarType.DATE);

        colType.put("l_orderkey", ScalarType.BIGINT);

        colType.put("l_linenumber", ScalarType.INT);

        colType.put("l_partkey", ScalarType.INT);

        colType.put("l_suppkey", ScalarType.INT);

        colType.put("l_quantity", ScalarType.createDecimalType(15,2));

        colType.put("l_extendedprice", ScalarType.createDecimalType(15,2));

        colType.put("l_discount", ScalarType.createDecimalType(15,2));

        colType.put("l_tax", ScalarType.createDecimalType(15,2));

        colType.put("l_returnflag", ScalarType.VARCHAR);

        colType.put("l_linestatus", ScalarType.VARCHAR);

        colType.put("l_commitdate", ScalarType.DATE);

        colType.put("l_receiptdate", ScalarType.DATE);

        colType.put("l_shipinstruct", ScalarType.VARCHAR);

        colType.put("l_shipmode", ScalarType.VARCHAR);

        colType.put("l_comment", ScalarType.VARCHAR);

        colType.put("n_nationkey", ScalarType.INT);

        colType.put("n_name", ScalarType.VARCHAR);

        colType.put("n_regionkey", ScalarType.INT);

        colType.put("n_comment", ScalarType.VARCHAR);

        colType.put("o_orderkey", ScalarType.BIGINT);

        colType.put("o_orderdate", ScalarType.DATE);

        colType.put("o_custkey", ScalarType.INT);

        colType.put("o_orderstatus", ScalarType.VARCHAR);

        colType.put("o_totalprice", ScalarType.createDecimalType(15,2));

        colType.put("o_orderpriority", ScalarType.VARCHAR);

        colType.put("o_clerk", ScalarType.VARCHAR);

        colType.put("o_shippriority", ScalarType.INT);

        colType.put("o_comment", ScalarType.VARCHAR);

        colType.put("p_partkey", ScalarType.INT);

        colType.put("p_name", ScalarType.VARCHAR);

        colType.put("p_mfgr", ScalarType.VARCHAR);

        colType.put("p_brand", ScalarType.VARCHAR);

        colType.put("p_type", ScalarType.VARCHAR);

        colType.put("p_size", ScalarType.INT);

        colType.put("p_container", ScalarType.VARCHAR);

        colType.put("p_retailprice", ScalarType.createDecimalType(15,2));

        colType.put("p_comment", ScalarType.VARCHAR);

        colType.put("ps_partkey", ScalarType.INT);

        colType.put("ps_suppkey", ScalarType.INT);

        colType.put("ps_availqty", ScalarType.INT);

        colType.put("ps_supplycost", ScalarType.createDecimalType(15,2));

        colType.put("ps_comment", ScalarType.VARCHAR);

        colType.put("r_regionkey", ScalarType.INT);

        colType.put("r_name", ScalarType.VARCHAR);

        colType.put("r_comment", ScalarType.VARCHAR);

        colType.put("s_suppkey", ScalarType.INT);

        colType.put("s_name", ScalarType.VARCHAR);

        colType.put("s_address", ScalarType.VARCHAR);

        colType.put("s_nationkey", ScalarType.INT);

        colType.put("s_phone", ScalarType.VARCHAR);

        colType.put("s_acctbal", ScalarType.createDecimalType(15,2));

        colType.put("s_comment", ScalarType.VARCHAR);

    }

    @Override
    protected void initMockedReturnedRows() {

        queryIdToQError.put(1, 10.666666666666666);

        mockedExactReturnedRows.put(1, new HashMap<>());

        mockedExactReturnedRows.get(1).put(new PlanNodeId(0), 5916591.0);

        mockedExactReturnedRows.get(1).put(new PlanNodeId(1), 64.0);

        mockedExactReturnedRows.get(1).put(new PlanNodeId(2), 64.0);

        mockedExactReturnedRows.get(1).put(new PlanNodeId(3), 4.0);

        mockedExactReturnedRows.get(1).put(new PlanNodeId(4), 4.0);

        mockedExactReturnedRows.get(1).put(new PlanNodeId(5), 4.0);

        queryIdToQError.put(2, 460.00000000000006);

        mockedExactReturnedRows.put(2, new HashMap<>());

        mockedExactReturnedRows.get(2).put(new PlanNodeId(0), 747.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(1), 800000.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(2), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(3), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(4), 10000.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(5), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(6), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(7), 25.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(8), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(9), 2988.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(10), 1.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(11), 642.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(12), 642.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(13), 642.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(14), 460.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(15), 100.0);

        mockedExactReturnedRows.get(2).put(new PlanNodeId(16), 100.0);

        queryIdToQError.put(3, 16.289786674344537);

        mockedExactReturnedRows.put(3, new HashMap<>());

        mockedExactReturnedRows.get(3).put(new PlanNodeId(0), 727305.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(1), 3241776.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(2), 151331.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(3), 151331.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(4), 30142.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(5), 30519.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(6), 30519.0);

        mockedExactReturnedRows.get(3).put(new PlanNodeId(9), 10.0);

        queryIdToQError.put(4, 7.612047232987107);

        mockedExactReturnedRows.put(4, new HashMap<>());

        mockedExactReturnedRows.get(4).put(new PlanNodeId(0), 3793296.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(1), 57218.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(2), 52523.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(3), 52523.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(4), 5.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(5), 5.0);

        mockedExactReturnedRows.get(4).put(new PlanNodeId(6), 5.0);

        queryIdToQError.put(5, 14.939600829885562);

        mockedExactReturnedRows.put(5, new HashMap<>());

        mockedExactReturnedRows.get(5).put(new PlanNodeId(0), 150000.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(1), 150000.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(2), 227597.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(3), 6001215.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(4), 910519.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(5), 910519.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(6), 10000.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(7), 910519.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(8), 910519.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(9), 25.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(10), 910519.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(11), 36236.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(12), 36236.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(13), 1.0);

        mockedExactReturnedRows.get(5).put(new PlanNodeId(14), 7243.0);

        queryIdToQError.put(6, 22.704312185172565);

        mockedExactReturnedRows.put(6, new HashMap<>());

        mockedExactReturnedRows.get(6).put(new PlanNodeId(0), 114160.0);

        mockedExactReturnedRows.get(6).put(new PlanNodeId(1), 8.0);

        mockedExactReturnedRows.get(6).put(new PlanNodeId(2), 8.0);

        mockedExactReturnedRows.get(6).put(new PlanNodeId(3), 1.0);

        queryIdToQError.put(7, 16.98406908487828);

        mockedExactReturnedRows.put(7, new HashMap<>());

        mockedExactReturnedRows.get(7).put(new PlanNodeId(0), 1500000.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(1), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(2), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(3), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(4), 150000.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(5), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(6), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(7), 10000.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(8), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(9), 1828450.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(10), 2.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(11), 145703.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(12), 145703.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(13), 2.0);

        mockedExactReturnedRows.get(7).put(new PlanNodeId(14), 5924.0);

        queryIdToQError.put(8, 172.3936534440346);

        mockedExactReturnedRows.put(8, new HashMap<>());

        mockedExactReturnedRows.get(8).put(new PlanNodeId(0), 1.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(1), 1.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(2), 25.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(3), 25.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(4), 25.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(5), 25.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(6), 150000.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(7), 150000.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(8), 457263.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(9), 457263.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(10), 6001215.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(11), 6001215.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(12), 10000.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(13), 80000.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(14), 1451.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(15), 14510000.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(16), 43693.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(17), 13389.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(18), 13389.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(19), 13389.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(20), 13389.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(21), 2603.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(22), 2603.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(23), 2.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(24), 2.0);

        mockedExactReturnedRows.get(8).put(new PlanNodeId(25), 2.0);

        queryIdToQError.put(9, 24.523403087988246);

        mockedExactReturnedRows.put(9, new HashMap<>());

        mockedExactReturnedRows.get(9).put(new PlanNodeId(0), 800000.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(1), 800000.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(2), 6001215.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(3), 6001215.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(4), 1500000.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(5), 6001215.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(6), 6001215.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(7), 10664.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(8), 319404.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(9), 319404.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(10), 10000.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(11), 319404.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(12), 319404.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(13), 25.0);

        mockedExactReturnedRows.get(9).put(new PlanNodeId(14), 319404.0);

        queryIdToQError.put(10, 17.53057668876071);

        mockedExactReturnedRows.put(10, new HashMap<>());

        mockedExactReturnedRows.get(10).put(new PlanNodeId(0), 25.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(1), 25.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(2), 1478870.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(3), 1478870.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(4), 57069.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(5), 57069.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(6), 150000.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(7), 57069.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(8), 114705.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(9), 114705.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(10), 37967.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(11), 320.0);

        mockedExactReturnedRows.get(10).put(new PlanNodeId(12), 20.0);

        queryIdToQError.put(11, 1.1582088481002604);

        mockedExactReturnedRows.put(11, new HashMap<>());

        mockedExactReturnedRows.get(11).put(new PlanNodeId(0), 10000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(1), 10000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(2), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(3), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(4), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(5), 1.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(6), 31680.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(7), 29818.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(8), 29818.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(9), 10000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(10), 10000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(11), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(12), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(13), 800000.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(14), 1.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(15), 31680.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(16), 1.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(17), 1.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(18), 27604.0);

        mockedExactReturnedRows.get(11).put(new PlanNodeId(19), 27604.0);

        queryIdToQError.put(12, 13.846666567293623);

        mockedExactReturnedRows.put(12, new HashMap<>());

        mockedExactReturnedRows.get(12).put(new PlanNodeId(0), 30988.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(1), 1500000.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(2), 30988.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(3), 30988.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(4), 2.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(5), 2.0);

        mockedExactReturnedRows.get(12).put(new PlanNodeId(6), 2.0);

        queryIdToQError.put(13, 590.0);

        mockedExactReturnedRows.put(13, new HashMap<>());

        mockedExactReturnedRows.get(13).put(new PlanNodeId(0), 1483918.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(1), 1483918.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(2), 150000.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(3), 1533923.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(4), 150000.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(5), 590.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(6), 590.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(7), 42.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(8), 42.0);

        mockedExactReturnedRows.get(13).put(new PlanNodeId(9), 42.0);

        queryIdToQError.put(14, 37.62936505020045);

        mockedExactReturnedRows.put(14, new HashMap<>());

        mockedExactReturnedRows.get(14).put(new PlanNodeId(0), 200000.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(1), 200000.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(2), 75983.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(3), 75983.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(4), 8.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(5), 8.0);

        mockedExactReturnedRows.get(14).put(new PlanNodeId(6), 1.0);

        queryIdToQError.put(15, 89.86896174525964);

        mockedExactReturnedRows.put(15, new HashMap<>());

        mockedExactReturnedRows.get(15).put(new PlanNodeId(0), 225954.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(1), 225954.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(2), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(3), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(4), 1.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(5), 1.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(6), 1.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(7), 225954.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(8), 225954.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(9), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(10), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(11), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(12), 10000.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(13), 1.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(14), 1.0);

        mockedExactReturnedRows.get(15).put(new PlanNodeId(15), 1.0);

        queryIdToQError.put(16, 1250.0);

        mockedExactReturnedRows.put(16, new HashMap<>());

        mockedExactReturnedRows.get(16).put(new PlanNodeId(0), 800000.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(1), 29581.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(2), 118324.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(3), 118324.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(4), 4.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(5), 118274.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(6), 118274.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(7), 118250.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(8), 18314.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(9), 18314.0);

        mockedExactReturnedRows.get(16).put(new PlanNodeId(10), 18314.0);

        queryIdToQError.put(17, 1.1230855208874255);

        mockedExactReturnedRows.put(17, new HashMap<>());

        mockedExactReturnedRows.get(17).put(new PlanNodeId(0), 204.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(1), 204.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(2), 6001215.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(3), 6088.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(4), 6088.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(5), 6088.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(6), 587.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(7), 587.0);

        mockedExactReturnedRows.get(17).put(new PlanNodeId(8), 1.0);

        queryIdToQError.put(18, 1052.8447368421052);

        mockedExactReturnedRows.put(18, new HashMap<>());

        mockedExactReturnedRows.get(18).put(new PlanNodeId(0), 6001215.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(1), 6001215.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(2), 6001215.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(3), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(4), 1500000.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(5), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(6), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(7), 150000.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(8), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(9), 399.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(10), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(11), 57.0);

        mockedExactReturnedRows.get(18).put(new PlanNodeId(12), 57.0);

        queryIdToQError.put(19, 4.944574565005941);

        mockedExactReturnedRows.put(19, new HashMap<>());

        mockedExactReturnedRows.get(19).put(new PlanNodeId(0), 485.0);

        mockedExactReturnedRows.get(19).put(new PlanNodeId(1), 485.0);

        mockedExactReturnedRows.get(19).put(new PlanNodeId(2), 128371.0);

        mockedExactReturnedRows.get(19).put(new PlanNodeId(3), 121.0);

        mockedExactReturnedRows.get(19).put(new PlanNodeId(4), 121.0);

        mockedExactReturnedRows.get(19).put(new PlanNodeId(5), 1.0);

        queryIdToQError.put(20, 47.94983607065881);

        mockedExactReturnedRows.put(20, new HashMap<>());

        mockedExactReturnedRows.get(20).put(new PlanNodeId(0), 1.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(1), 1.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(2), 909455.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(3), 909455.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(4), 543210.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(5), 543210.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(6), 2127.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(7), 800000.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(8), 8508.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(9), 5833.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(10), 5833.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(11), 10000.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(12), 4397.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(13), 186.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(14), 186.0);

        mockedExactReturnedRows.get(20).put(new PlanNodeId(15), 186.0);

        queryIdToQError.put(21, 24.328358395723587);

        mockedExactReturnedRows.put(21, new HashMap<>());

        mockedExactReturnedRows.get(21).put(new PlanNodeId(0), 3793296.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(1), 3793296.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(2), 1.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(3), 1.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(4), 729413.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(5), 6001215.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(6), 3793296.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(7), 3657708.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(8), 3657708.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(9), 10000.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(10), 3657708.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(11), 3657708.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(12), 1762253.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(14), 73089.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(15), 4141.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(16), 4141.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(17), 411.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(18), 411.0);

        mockedExactReturnedRows.get(21).put(new PlanNodeId(19), 100.0);

        queryIdToQError.put(22, 4604.465736181143);

        mockedExactReturnedRows.put(22, new HashMap<>());

        mockedExactReturnedRows.get(22).put(new PlanNodeId(0), 1500000.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(1), 1500000.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(2), 42015.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(3), 14086.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(4), 14086.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(5), 38120.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(6), 38120.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(7), 1.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(8), 1.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(9), 6384.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(10), 7.0);

        mockedExactReturnedRows.get(22).put(new PlanNodeId(11), 7.0);

    }

    @Override
    protected void initMockedColumnsStats() {

        values = new ArrayList<>();
        values.add("10313--1-l_linenumber");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_linenumber");
        values.add("None");
        values.add("6001215");
        values.add("7");
        values.add("0");
        values.add("1");
        values.add("7");
        values.add("24004860");
        values.add("2023-03-26 22:38:18");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_linenumber",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_shipdate");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_shipdate");
        values.add("None");
        values.add("6001215");
        values.add("2549");
        values.add("0");
        values.add("1992-01-02");
        values.add("1998-12-01");
        values.add("96019440");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_shipdate",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_comment");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_comment");
        values.add("None");
        values.add("1500000");
        values.add("1465415");
        values.add("0");
        values.add(" Tiresias about the blithely ironic a");
        values.add("zzle? furiously ironic instructions among the unusual t");
        values.add("72770808");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10704--1-ps_partkey");
        values.add("0");
        values.add("10310");
        values.add("10704");
        values.add("-1");
        values.add("ps_partkey");
        values.add("None");
        values.add("800000");
        values.add("196099");
        values.add("0");
        values.add("1");
        values.add("200000");
        values.add("3200000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("ps_partkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_name");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_name");
        values.add("None");
        values.add("200000");
        values.add("200265");
        values.add("0");
        values.add("almond antique blue royal burnished");
        values.add("yellow white seashell lavender black");
        values.add("6550221");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_name",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_phone");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_phone");
        values.add("None");
        values.add("150000");
        values.add("152666");
        values.add("0");
        values.add("10-100-106-1617");
        values.add("34-999-618-6881");
        values.add("2250000");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_phone",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_acctbal");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_acctbal");
        values.add("None");
        values.add("10000");
        values.add("9954");
        values.add("0");
        values.add("-998.220000000");
        values.add("9999.720000000");
        values.add("160000");
        values.add("2023-03-26 22:38:25");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_acctbal",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_linestatus");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_linestatus");
        values.add("None");
        values.add("6001215");
        values.add("2");
        values.add("0");
        values.add("F");
        values.add("O");
        values.add("6001215");
        values.add("2023-03-26 22:38:21");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_linestatus",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_orderkey");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_orderkey");
        values.add("None");
        values.add("6001215");
        values.add("1510112");
        values.add("0");
        values.add("1");
        values.add("6000000");
        values.add("48009720");
        values.add("2023-03-26 22:38:18");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_orderkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_partkey");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_partkey");
        values.add("None");
        values.add("6001215");
        values.add("196099");
        values.add("0");
        values.add("1");
        values.add("200000");
        values.add("24004860");
        values.add("2023-03-26 22:38:18");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_partkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_shipmode");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_shipmode");
        values.add("None");
        values.add("6001215");
        values.add("7");
        values.add("0");
        values.add("AIR");
        values.add("TRUCK");
        values.add("25717034");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_shipmode",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10704--1-ps_supplycost");
        values.add("0");
        values.add("10310");
        values.add("10704");
        values.add("-1");
        values.add("ps_supplycost");
        values.add("None");
        values.add("800000");
        values.add("100274");
        values.add("0");
        values.add("1.000000000");
        values.add("1000.000000000");
        values.add("12800000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("ps_supplycost",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_address");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_address");
        values.add("None");
        values.add("150000");
        values.add("150133");
        values.add("0");
        values.add("   2uZwVhQvwA");
        values.add("zzxGktzXTMKS1BxZlgQ9nqQ");
        values.add("3758056");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_address",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_address");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_address");
        values.add("None");
        values.add("10000");
        values.add("9888");
        values.add("0");
        values.add("  9aW1wwnBJJPnCx,nox0MA48Y0zpI1IeVfYZ");
        values.add("zzfDhdtZcvmVzA8rNFU,Yctj1zBN");
        values.add("249771");
        values.add("2023-03-26 22:38:25");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_address",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_suppkey");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_suppkey");
        values.add("None");
        values.add("10000");
        values.add("10009");
        values.add("0");
        values.add("1");
        values.add("10000");
        values.add("40000");
        values.add("2023-03-26 22:38:25");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_suppkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_returnflag");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_returnflag");
        values.add("None");
        values.add("6001215");
        values.add("3");
        values.add("0");
        values.add("A");
        values.add("R");
        values.add("6001215");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_returnflag",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_shipinstruct");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_shipinstruct");
        values.add("None");
        values.add("6001215");
        values.add("4");
        values.add("0");
        values.add("COLLECT COD");
        values.add("TAKE BACK RETURN");
        values.add("72006409");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_shipinstruct",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_tax");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_tax");
        values.add("None");
        values.add("6001215");
        values.add("9");
        values.add("0");
        values.add("0.000000000");
        values.add("0.080000000");
        values.add("96019440");
        values.add("2023-03-26 22:38:19");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_tax",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_clerk");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_clerk");
        values.add("None");
        values.add("1500000");
        values.add("988");
        values.add("0");
        values.add("Clerk#000000001");
        values.add("Clerk#000001000");
        values.add("22500000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_clerk",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_shippriority");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_shippriority");
        values.add("None");
        values.add("1500000");
        values.add("1");
        values.add("0");
        values.add("0");
        values.add("0");
        values.add("6000000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_shippriority",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_totalprice");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_totalprice");
        values.add("None");
        values.add("1500000");
        values.add("1462416");
        values.add("0");
        values.add("857.710000000");
        values.add("555285.160000000");
        values.add("24000000");
        values.add("2023-03-26 22:38:21");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_totalprice",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10704--1-ps_comment");
        values.add("0");
        values.add("10310");
        values.add("10704");
        values.add("-1");
        values.add("ps_comment");
        values.add("None");
        values.add("800000");
        values.add("794782");
        values.add("0");
        values.add(
                " Tiresias according to the quiet courts sleep against the ironic, final requests. carefully unusual requests affix fluffily quickly ironic packages. regular ");
        values.add(
                "zzle. unusual decoys detect slyly blithely express frays. furiously ironic packages about the bold accounts are close requests. slowly silent reque");
        values.add("98891983");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("ps_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10704--1-ps_suppkey");
        values.add("0");
        values.add("10310");
        values.add("10704");
        values.add("-1");
        values.add("ps_suppkey");
        values.add("None");
        values.add("800000");
        values.add("10009");
        values.add("0");
        values.add("1");
        values.add("10000");
        values.add("3200000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("ps_suppkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_mfgr");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_mfgr");
        values.add("None");
        values.add("200000");
        values.add("5");
        values.add("0");
        values.add("Manufacturer#1");
        values.add("Manufacturer#5");
        values.add("2800000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_mfgr",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_type");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_type");
        values.add("None");
        values.add("200000");
        values.add("150");
        values.add("0");
        values.add("ECONOMY ANODIZED BRASS");
        values.add("STANDARD POLISHED TIN");
        values.add("4119946");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_type",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_nationkey");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_nationkey");
        values.add("None");
        values.add("10000");
        values.add("25");
        values.add("0");
        values.add("0");
        values.add("24");
        values.add("40000");
        values.add("2023-03-26 22:38:24");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_nationkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10885--1-n_nationkey");
        values.add("0");
        values.add("10310");
        values.add("10885");
        values.add("-1");
        values.add("n_nationkey");
        values.add("None");
        values.add("25");
        values.add("25");
        values.add("0");
        values.add("0");
        values.add("24");
        values.add("100");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("n_nationkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10890--1-r_comment");
        values.add("0");
        values.add("10310");
        values.add("10890");
        values.add("-1");
        values.add("r_comment");
        values.add("None");
        values.add("5");
        values.add("5");
        values.add("0");
        values.add("ges. thinly even pinto beans ca");
        values.add(
                "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl");
        values.add("330");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("r_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_extendedprice");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_extendedprice");
        values.add("None");
        values.add("6001215");
        values.add("929697");
        values.add("0");
        values.add("901.000000000");
        values.add("104949.500000000");
        values.add("96019440");
        values.add("2023-03-26 22:38:21");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_extendedprice",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_receiptdate");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_receiptdate");
        values.add("None");
        values.add("6001215");
        values.add("2576");
        values.add("0");
        values.add("1992-01-04");
        values.add("1998-12-31");
        values.add("96019440");
        values.add("2023-03-26 22:38:18");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_receiptdate",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_orderstatus");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_orderstatus");
        values.add("None");
        values.add("1500000");
        values.add("3");
        values.add("0");
        values.add("F");
        values.add("P");
        values.add("1500000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_orderstatus",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10704--1-ps_availqty");
        values.add("0");
        values.add("10310");
        values.add("10704");
        values.add("-1");
        values.add("ps_availqty");
        values.add("None");
        values.add("800000");
        values.add("10008");
        values.add("0");
        values.add("1");
        values.add("9999");
        values.add("3200000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("ps_availqty",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_comment");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_comment");
        values.add("None");
        values.add("150000");
        values.add("147491");
        values.add("0");
        values.add(
                " Tiresias according to the slyly blithe instructions detect quickly at the slyly express courts. express dinos wake ");
        values.add("zzle. blithely regular instructions cajol");
        values.add("10876099");
        values.add("2023-03-26 22:38:15");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_mktsegment");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_mktsegment");
        values.add("None");
        values.add("150000");
        values.add("5");
        values.add("0");
        values.add("AUTOMOBILE");
        values.add("MACHINERY");
        values.add("1349610");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_mktsegment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10890--1-r_name");
        values.add("0");
        values.add("10310");
        values.add("10890");
        values.add("-1");
        values.add("r_name");
        values.add("None");
        values.add("5");
        values.add("5");
        values.add("0");
        values.add("AFRICA");
        values.add("MIDDLE EAST");
        values.add("34");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("r_name",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_discount");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_discount");
        values.add("None");
        values.add("6001215");
        values.add("11");
        values.add("0");
        values.add("0.000000000");
        values.add("0.100000000");
        values.add("96019440");
        values.add("2023-03-26 22:38:19");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_discount",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_quantity");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_quantity");
        values.add("None");
        values.add("6001215");
        values.add("50");
        values.add("0");
        values.add("1.000000000");
        values.add("50.000000000");
        values.add("96019440");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_quantity",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_orderpriority");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_orderpriority");
        values.add("None");
        values.add("1500000");
        values.add("5");
        values.add("0");
        values.add("1-URGENT");
        values.add("5-LOW");
        values.add("12599829");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_orderpriority",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_brand");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_brand");
        values.add("None");
        values.add("200000");
        values.add("25");
        values.add("0");
        values.add("Brand#11");
        values.add("Brand#55");
        values.add("1600000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_brand",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_size");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_size");
        values.add("None");
        values.add("200000");
        values.add("50");
        values.add("0");
        values.add("1");
        values.add("50");
        values.add("800000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_size",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10890--1-r_regionkey");
        values.add("0");
        values.add("10310");
        values.add("10890");
        values.add("-1");
        values.add("r_regionkey");
        values.add("None");
        values.add("5");
        values.add("5");
        values.add("0");
        values.add("0");
        values.add("4");
        values.add("20");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("r_regionkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_comment");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_comment");
        values.add("None");
        values.add("6001215");
        values.add("4619207");
        values.add("0");
        values.add(" Tiresias ");
        values.add("zzle? slyly final platelets sleep quickly. ");
        values.add("158997209");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_commitdate");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_commitdate");
        values.add("None");
        values.add("6001215");
        values.add("2485");
        values.add("0");
        values.add("1992-01-31");
        values.add("1998-10-31");
        values.add("96019440");
        values.add("2023-03-26 22:38:18");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_commitdate",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10313--1-l_suppkey");
        values.add("0");
        values.add("10310");
        values.add("10313");
        values.add("-1");
        values.add("l_suppkey");
        values.add("None");
        values.add("6001215");
        values.add("10009");
        values.add("0");
        values.add("1");
        values.add("10000");
        values.add("24004860");
        values.add("2023-03-26 22:38:19");

        resultRow = new ResultRow(cols, types, values);
        stats.put("l_suppkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_custkey");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_custkey");
        values.add("None");
        values.add("1500000");
        values.add("99149");
        values.add("0");
        values.add("1");
        values.add("149999");
        values.add("6000000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_custkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_orderdate");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_orderdate");
        values.add("None");
        values.add("1500000");
        values.add("2428");
        values.add("0");
        values.add("1992-01-01");
        values.add("1998-08-02");
        values.add("24000000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_orderdate",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10509--1-o_orderkey");
        values.add("0");
        values.add("10310");
        values.add("10509");
        values.add("-1");
        values.add("o_orderkey");
        values.add("None");
        values.add("1500000");
        values.add("1510112");
        values.add("0");
        values.add("1");
        values.add("6000000");
        values.add("12000000");
        values.add("2023-03-26 22:38:21");

        resultRow = new ResultRow(cols, types, values);
        stats.put("o_orderkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_comment");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_comment");
        values.add("None");
        values.add("200000");
        values.add("133106");
        values.add("0");
        values.add(" Tire");
        values.add("zzle. quickly si");
        values.add("2702438");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_partkey");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_partkey");
        values.add("None");
        values.add("200000");
        values.add("196099");
        values.add("0");
        values.add("1");
        values.add("200000");
        values.add("800000");
        values.add("2023-03-26 22:38:23");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_partkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_retailprice");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_retailprice");
        values.add("None");
        values.add("200000");
        values.add("21096");
        values.add("0");
        values.add("901.000000000");
        values.add("2098.990000000");
        values.add("3200000");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_retailprice",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_name");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_name");
        values.add("None");
        values.add("150000");
        values.add("150431");
        values.add("0");
        values.add("Customer#000000001");
        values.add("Customer#000150000");
        values.add("2700000");
        values.add("2023-03-26 22:38:15");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_name",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10885--1-n_comment");
        values.add("0");
        values.add("10310");
        values.add("10885");
        values.add("-1");
        values.add("n_comment");
        values.add("None");
        values.add("25");
        values.add("25");
        values.add("0");
        values.add(" haggle. carefully final deposits detect slyly agai");
        values.add(
                "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be");
        values.add("1857");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("n_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10885--1-n_name");
        values.add("0");
        values.add("10310");
        values.add("10885");
        values.add("-1");
        values.add("n_name");
        values.add("None");
        values.add("25");
        values.add("25");
        values.add("0");
        values.add("ALGERIA");
        values.add("VIETNAM");
        values.add("177");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("n_name",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10885--1-n_regionkey");
        values.add("0");
        values.add("10310");
        values.add("10885");
        values.add("-1");
        values.add("n_regionkey");
        values.add("None");
        values.add("25");
        values.add("5");
        values.add("0");
        values.add("0");
        values.add("4");
        values.add("100");
        values.add("2023-03-26 22:38:20");

        resultRow = new ResultRow(cols, types, values);
        stats.put("n_regionkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10756--1-p_container");
        values.add("0");
        values.add("10310");
        values.add("10756");
        values.add("-1");
        values.add("p_container");
        values.add("None");
        values.add("200000");
        values.add("40");
        values.add("0");
        values.add("JUMBO BAG");
        values.add("WRAP PKG");
        values.add("1514971");
        values.add("2023-03-26 22:38:22");

        resultRow = new ResultRow(cols, types, values);
        stats.put("p_container",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_acctbal");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_acctbal");
        values.add("None");
        values.add("150000");
        values.add("142496");
        values.add("0");
        values.add("-999.990000000");
        values.add("9999.990000000");
        values.add("2400000");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_acctbal",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_custkey");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_custkey");
        values.add("None");
        values.add("150000");
        values.add("149087");
        values.add("0");
        values.add("1");
        values.add("150000");
        values.add("600000");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_custkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10807--1-c_nationkey");
        values.add("0");
        values.add("10310");
        values.add("10807");
        values.add("-1");
        values.add("c_nationkey");
        values.add("None");
        values.add("150000");
        values.add("25");
        values.add("0");
        values.add("0");
        values.add("24");
        values.add("600000");
        values.add("2023-03-26 22:38:16");

        resultRow = new ResultRow(cols, types, values);
        stats.put("c_nationkey",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_comment");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_comment");
        values.add("None");
        values.add("10000");
        values.add("10039");
        values.add("0");
        values.add(" about the blithely express foxes. bli");
        values.add("zzle furiously. bold accounts haggle furiously ironic excuses. fur");
        values.add("625695");
        values.add("2023-03-26 22:38:24");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_comment",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_name");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_name");
        values.add("None");
        values.add("10000");
        values.add("10002");
        values.add("0");
        values.add("Supplier#000000001");
        values.add("Supplier#000010000");
        values.add("180000");
        values.add("2023-03-26 22:38:24");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_name",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

        values = new ArrayList<>();
        values.add("10858--1-s_phone");
        values.add("0");
        values.add("10310");
        values.add("10858");
        values.add("-1");
        values.add("s_phone");
        values.add("None");
        values.add("10000");
        values.add("10021");
        values.add("0");
        values.add("10-102-116-6785");
        values.add("34-998-900-4911");
        values.add("150000");
        values.add("2023-03-26 22:38:24");

        resultRow = new ResultRow(cols, types, values);
        stats.put("s_phone",
                new ColumnLevelStatisticCache(null, ColumnStatistic.fromResultRow(resultRow)));

    }

    public void t2est() {
        new MockUp<ColumnStatistic>() {

            @Mock
            public ColumnStatistic fromResultRow(ResultRow resultRow) {
                try {
                    ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
                    double count = Double.parseDouble(resultRow.getColumnValue("count"));
                    columnStatisticBuilder.setCount(count);
                    double ndv = Double.parseDouble(resultRow.getColumnValue("ndv"));
                    if (0.99 * count < ndv && ndv < 1.01 * count) {
                        ndv = count;
                    }
                    columnStatisticBuilder.setNdv(ndv);
                    columnStatisticBuilder.setNumNulls(Double.parseDouble(resultRow.getColumnValue("null_count")));
                    columnStatisticBuilder.setDataSize(Double
                            .parseDouble(resultRow.getColumnValue("data_size_in_bytes")));
                    columnStatisticBuilder.setAvgSizeByte(columnStatisticBuilder.getDataSize()
                            / columnStatisticBuilder.getCount());
                    long catalogId = Long.parseLong(resultRow.getColumnValue("catalog_id"));
                    long idxId = Long.parseLong(resultRow.getColumnValue("idx_id"));
                    long dbID = Long.parseLong(resultRow.getColumnValue("db_id"));
                    long tblId = Long.parseLong(resultRow.getColumnValue("tbl_id"));
                    String colName = resultRow.getColumnValue("col_id");
                    Column col = new Column(colName, colType.get(colName));
                    String min = resultRow.getColumnValue("min");
                    String max = resultRow.getColumnValue("max");
                    columnStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
                    columnStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
                    columnStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
                    columnStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
                    columnStatisticBuilder.setSelectivity(1.0);
                    Histogram histogram = Env.getCurrentEnv().getStatisticsCache().getHistogram(tblId, idxId, colName);
                    columnStatisticBuilder.setHistogram(histogram);
                    return columnStatisticBuilder.build();
                } catch (Exception e) {
                    return ColumnStatistic.UNKNOWN;
                }
            }
        };
    }
}
