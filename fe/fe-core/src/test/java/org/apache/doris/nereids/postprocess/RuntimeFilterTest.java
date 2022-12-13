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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class RuntimeFilterTest extends SSBTestBase {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setEnableNereidsRuntimeFilter(true);
        connectContext.getSessionVariable().setRuntimeFilterType(8);
    }

    @Test
    public void testGenerateRuntimeFilter() {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testGenerateRuntimeFilterByIllegalSrcExpr() {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testComplexExpressionToRuntimeFilter() {
        String sql
                = "SELECT * FROM supplier JOIN customer on c_name = s_name and s_city = c_city and s_nation = c_nation";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_name", "s_name"),
                Pair.of("c_city", "s_city"),
                Pair.of("c_nation", "s_nation")));
    }

    @Test
    public void testNestedJoinGenerateRuntimeFilter() {
        String sql = SSBUtils.Q4_1;
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("p_partkey", "lo_partkey"), Pair.of("s_suppkey", "lo_suppkey"),
                Pair.of("c_custkey", "lo_custkey"), Pair.of("lo_orderdate", "d_datekey")));
    }

    @Test
    public void testSubTreeInUnsupportedJoinType() {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder inner join dates on lo_orderdate = d_datekey) a"
                + " left outer join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testPushDownEncounterUnsupportedJoinType() {
        String sql = "select c_custkey"
                + " from (select lo_custkey from lineorder left outer join dates on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testPushDownThroughAggNode() {
        String sql = "select profit"
                + " from (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder inner join dates"
                + " on lo_orderdate = d_datekey group by lo_custkey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey"), Pair.of("d_datekey", "lo_orderdate"),
                Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testDoNotPushDownThroughAggFunction() {
        String sql = "select profit"
                + " from (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder inner join dates"
                + " on lo_orderdate = d_datekey group by lo_custkey) a"
                + " inner join (select sum(c_custkey) c_custkey from customer inner join supplier on c_custkey = s_suppkey group by s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey")));
    }

    @Test
    public void testCrossJoin() {
        String sql = "select c_custkey, lo_custkey from lineorder, customer where lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testSubQueryAlias() {
        String sql = "select c_custkey, lo_custkey from lineorder l, customer c where c.c_custkey = l.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey")));
    }

    @Test
    public void testView() throws Exception {
        createView("create view if not exists v1 as \n"
                + "        select * \n"
                + "        from customer");
        createView("create view if not exists v2 as\n"
                + "        select *\n"
                + "        from lineorder");
        createView("create view if not exists v3 as \n"
                + "        select *\n"
                + "        from v1 join (\n"
                + "            select *\n"
                + "            from v2\n"
                + "            ) t \n"
                + "        on v1.c_custkey = t.lo_custkey");
        String sql = "select * from (\n"
                + "            select * \n"
                + "            from part p \n"
                + "            join v2 on p.p_partkey = v2.lo_partkey) t1 \n"
                + "        join (\n"
                + "            select * \n"
                + "            from supplier s \n"
                + "            join v3 on s.s_region = v3.c_region) t2 \n"
                + "        on t1.p_partkey = t2.lo_partkey\n"
                + "        order by t1.lo_custkey, t1.p_partkey, t2.s_suppkey, t2.c_custkey, t2.lo_orderkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("lo_partkey", "p_partkey"), Pair.of("lo_partkey", "p_partkey"),
                Pair.of("c_region", "s_region"), Pair.of("lo_custkey", "c_custkey")));
    }

    @Test
    public void testPushDownThroughJoin() {
        String sql = "select c_custkey from (select c_custkey from (select lo_custkey from lineorder inner join dates"
                + " on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer inner join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey) c inner join (select lo_custkey from customer inner join lineorder"
                + " on c_custkey = lo_custkey) d on c.c_custkey = d.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(5, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("lo_custkey", "c_custkey"), Pair.of("c_custkey", "lo_custkey"),
                Pair.of("d_datekey", "lo_orderdate"), Pair.of("s_suppkey", "c_custkey"),
                Pair.of("lo_custkey", "c_custkey")));
    }

    @Test
    public void testPushDownThroughUnsupportedJoinType() {
        String sql = "select c_custkey from (select c_custkey from (select lo_custkey from lineorder inner join dates"
                + " on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer left outer join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey) c inner join (select lo_custkey from customer inner join lineorder"
                + " on c_custkey = lo_custkey) d on c.c_custkey = d.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("c_custkey", "lo_custkey"), Pair.of("d_datekey", "lo_orderdate"),
                Pair.of("lo_custkey", "c_custkey")));
    }

    private Optional<List<RuntimeFilter>> getRuntimeFilters(String sql) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        System.out.println(plan.treeString());
        new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext(checker.getCascadesContext()));
        RuntimeFilterContext context = checker.getCascadesContext().getRuntimeFilterContext();
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertEquals(filters.size(), context.getLegacyFilters().size() + context.getTargetNullCount());
        return Optional.of(filters);
    }

    private void checkRuntimeFilterExprs(List<RuntimeFilter> filters, List<Pair<String, String>> colNames) {
        Assertions.assertEquals(filters.size(), colNames.size());
        for (RuntimeFilter filter : filters) {
            Assertions.assertTrue(colNames.contains(Pair.of(
                    filter.getSrcExpr().getName(),
                    filter.getTargetExpr().getName())));
        }
    }
}
