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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RuntimeFilterTest extends SSBTestBase {

    @Override
    public void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setEnableNereidsRuntimeFilter(true);
    }

    @Test
    public void testGenerateRuntimeFilter() throws AnalysisException {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 1
                && checkRuntimeFilterExpr(filters.get(0), "c_custkey", "lo_custkey"));
    }

    @Test
    public void testGenerateRuntimeFilterByIllegalSrcExpr() throws AnalysisException {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testComplexExpressionToRuntimeFilter() throws AnalysisException {
        String sql
                = "SELECT * FROM supplier JOIN customer on c_name = s_name and s_city = c_city and s_nation = c_nation";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 3
                && checkRuntimeFilterExpr(filters.get(0), "c_name", "s_name")
                && checkRuntimeFilterExpr(filters.get(1), "c_city", "s_city")
                && checkRuntimeFilterExpr(filters.get(2), "c_nation", "s_nation"));
    }

    @Test
    public void testNestedJoinGenerateRuntimeFilter() throws AnalysisException {
        String sql = SSBUtils.Q4_1;
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 4
                && checkRuntimeFilterExpr(filters.get(0), "p_partkey", "lo_partkey")
                && checkRuntimeFilterExpr(filters.get(1), "s_suppkey", "lo_suppkey")
                && checkRuntimeFilterExpr(filters.get(2), "c_custkey", "lo_custkey")
                && checkRuntimeFilterExpr(filters.get(3), "lo_orderdate", "d_datekey"));
    }

    @Test
    public void testUnsupportedJoinType() throws AnalysisException {
        String sql = "select d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS PROFIT"
                + " from lineorder inner join dates on lo_orderdate = d_datekey"
                + " left outer join supplier on s_suppkey = lo_suppkey"
                + " full outer join customer on c_custkey = lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 1
                && checkRuntimeFilterExpr(filters.get(0), "d_datekey", "lo_orderdate"));
    }

    @Test
    public void testSubTreeInUnsupportedJoinType() throws AnalysisException {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder inner join dates on lo_orderdate = d_datekey) a"
                + " left outer join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 2
                && checkRuntimeFilterExpr(filters.get(0),  "d_datekey", "lo_orderdate")
                && checkRuntimeFilterExpr(filters.get(1), "s_suppkey", "c_custkey"));
    }

    @Test
    public void testPushDownEncounterUnsupportedJoinType() throws AnalysisException {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder left outer join dates on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 1
                && checkRuntimeFilterExpr(filters.get(0), "s_suppkey", "c_custkey"));
    }

    @Test
    public void testPushDownThroughAggNode() throws AnalysisException {
        String sql = "select profit"
                + " from (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder inner join dates"
                + " on lo_orderdate = d_datekey group by lo_custkey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 3
                && checkRuntimeFilterExpr(filters.get(0), "c_custkey", "lo_custkey")
                && checkRuntimeFilterExpr(filters.get(1), "d_datekey", "lo_orderdate")
                && checkRuntimeFilterExpr(filters.get(2), "s_suppkey", "c_custkey"));
    }

    @Test
    public void testDoNotPushDownThroughAggFunction() throws AnalysisException {
        String sql = "select profit"
                + " from (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder inner join dates"
                + " on lo_orderdate = d_datekey group by lo_custkey) a"
                + " inner join (select sum(c_custkey) c_custkey from customer inner join supplier on c_custkey = s_suppkey group by s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 2
                && checkRuntimeFilterExpr(filters.get(0), "d_datekey", "lo_orderdate")
                && checkRuntimeFilterExpr(filters.get(1), "s_suppkey", "c_custkey"));
    }

    @Test
    public void testCrossJoin() throws AnalysisException {
        String sql = "select c_custkey, lo_custkey from lineorder, customer where lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 1
                && checkRuntimeFilterExpr(filters.get(0), "c_custkey", "lo_custkey"));
    }

    @Test
    public void testSubQueryAlias() throws AnalysisException {
        String sql = "select c_custkey, lo_custkey from lineorder l, customer c where c.c_custkey = l.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql);
        Assertions.assertTrue(filters.size() == 1
                && checkRuntimeFilterExpr(filters.get(0), "c_custkey", "lo_custkey"));
    }

    private List<RuntimeFilter> getRuntimeFilters(String sql) throws AnalysisException {
        NereidsPlanner planner = new NereidsPlanner(createStatementCtx(sql));
        planner.plan(new NereidsParser().parseSingle(sql), PhysicalProperties.ANY);
        return planner.getCascadesContext().getRuntimeGenerator().getNereridsRuntimeFilter();
    }

    private boolean checkRuntimeFilterExpr(RuntimeFilter filter, String srcColName, String targetColName) {
        return filter.getSrcExpr().getColumn().getName().equals(srcColName)
                && filter.getTargetExpr().getColumn().getName().equals(targetColName);
    }
}
