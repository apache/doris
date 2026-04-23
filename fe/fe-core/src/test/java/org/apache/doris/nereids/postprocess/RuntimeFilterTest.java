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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RuntimeFilterTest extends SSBTestBase {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setRuntimeFilterMode("Global");
        connectContext.getSessionVariable().setRuntimeFilterType(8);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        connectContext.getSessionVariable().setDisableJoinReorder(true);
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
        String sql = SSBUtils.Q4_4;
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("p_partkey", "lo_partkey"), Pair.of("s_suppkey", "lo_suppkey"),
                Pair.of("c_custkey", "lo_custkey"), Pair.of("d_datekey", "lo_orderdate")));
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
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "c_custkey"), Pair.of("c_custkey", "lo_custkey")));
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
                + " from (select sum(c_custkey) c_custkey from customer inner join supplier"
                + " on c_custkey = s_suppkey group by s_suppkey) a"
                + " inner join (select lo_custkey, sum(lo_revenue - lo_supplycost) as profit from lineorder"
                + " inner join dates on lo_orderdate = d_datekey group by lo_custkey) b"
                + " on a.c_custkey = b.lo_custkey";
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

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // V2-style: expanded multi-target RFs become separate single-target RFs
        // Original 5 RFs, one multi-target RF (3 targets) becomes 3 separate RFs → 5 + 2 = 7
        Assertions.assertEquals(7, filters.size());
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    @Test
    public void testPushDownThroughUnsupportedJoinType() {
        String sql = "select c_custkey from (select c_custkey from (select lo_custkey from lineorder inner join dates"
                + " on lo_orderdate = d_datekey) a"
                + " inner join (select c_custkey from customer left outer join supplier on c_custkey = s_suppkey) b"
                + " on b.c_custkey = a.lo_custkey) c inner join (select lo_custkey from customer inner join lineorder"
                + " on c_custkey = lo_custkey) d on c.c_custkey = d.lo_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(4, filters.size());

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // Expansion through inner joins creates additional RFs
        Assertions.assertEquals(5, filters.size());
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    @Test
    public void testAliasCastAtLeftAndExpressionAtRight() {
        String sql = "select c_custkey from (select cast(lo_custkey as bigint) c from lineorder) a"
                + " inner join customer b on a.c = b.c_custkey + 5";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("expr_(cast(c_custkey as BIGINT) + 5)", "lo_custkey")));
    }

    @Test
    public void testCastAtOnExpression() {
        String sql = "select * from part p, supplier s where p.p_name = s.s_name";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_name", "p_name")));
    }

    @Test
    public void testExpandRfByInnerJoin() {
        String sql = "select * "
                + "from lineorder join part on lo_partkey=p_partkey "
                + "join supplier on s_suppkey=lo_partkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("p_partkey", "lo_partkey")));
        connectContext.getSessionVariable().enableRuntimeFilterPrune = false;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        // V2-style: expansion creates separate RFs instead of multi-target RF
        // s_suppkey→lo_partkey (original), s_suppkey→p_partkey (expanded), p_partkey→lo_partkey
        Assertions.assertEquals(3, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("s_suppkey", "p_partkey"),
                Pair.of("p_partkey", "lo_partkey")));
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;

    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql) {
        return getRuntimeFilterContext(sql, context -> {
        });
    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql, Consumer<RuntimeFilterContext> beforeTranslate) {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize();
        PhysicalPlan plan = checker.getBestPlanTree();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        RuntimeFilterContext context = checker.getCascadesContext().getRuntimeFilterContext();
        beforeTranslate.accept(context);
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertTrue(filters.size() >= context.getLegacyFilters().size() + context.getTargetNullCount(),
                "nereidsRF count (" + filters.size() + ") should be >= legacyRF count ("
                        + context.getLegacyFilters().size() + ") + nullTargets ("
                        + context.getTargetNullCount() + ")");
        return context;
    }

    private Optional<List<RuntimeFilter>> getRuntimeFilters(String sql) {
        return Optional.of(getRuntimeFilterContext(sql).getNereidsRuntimeFilter());
    }

    private void checkRuntimeFilterExprs(List<RuntimeFilter> filters, List<Pair<String, String>> colNames) {
        Assertions.assertEquals(filters.size(), colNames.size());
        for (RuntimeFilter filter : filters) {
            Assertions.assertTrue(colNames.contains(Pair.of(
                    filter.getSrcExpr().toSql(),
                    filter.getTargetSlot().getName())));
        }
    }

    @Test
    public void testRuntimeFilterBlockByWindow() {
        String sql = "SELECT * FROM (select rank() over(partition by lo_partkey), lo_custkey from lineorder) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testRuntimeFilterNotBlockByWindow() {
        String sql = "SELECT * FROM (select rank() over(partition by lo_custkey), lo_custkey from lineorder) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
    }

    @Test
    public void testRuntimeFilterBlockByTopN() {
        String sql = "SELECT * FROM (select lo_custkey from lineorder order by lo_custkey limit 10) t JOIN customer on lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size());
    }

    @Test
    public void testRuntimeFilterShapeInfoWithoutBrackets() {
        String sql = "SELECT * FROM lineorder JOIN customer ON lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size());
        Assertions.assertFalse(filters.get(0).shapeInfo().contains("->["));
        Assertions.assertTrue(filters.get(0).shapeInfo().contains("c_custkey->lo_custkey"));
    }

    @Test
    public void testRuntimeFilterBlockByGroupingSetsPartialColumn() {
        // RF on lo_custkey should be blocked because lo_custkey is NOT in all grouping sets.
        // grouping sets ((lo_partkey), (lo_custkey, lo_partkey)) — first set lacks lo_custkey.
        // Subquery must be on LEFT (probe) side so the RF pushes through Repeat.
        String sql = "SELECT lo_custkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t INNER JOIN customer ON t.lo_custkey = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        // No RF should push to lineorder scan because lo_custkey is not in all grouping sets
        Assertions.assertEquals(0, filters.size(),
                "RF should be blocked when probe slot is not in all grouping sets");
    }

    @Test
    public void testRuntimeFilterPushThroughGroupingSetsCommonColumn() {
        // RF on lo_partkey should push through because lo_partkey IS in all grouping sets.
        // grouping sets ((lo_partkey), (lo_custkey, lo_partkey)) — lo_partkey is common.
        // Subquery on LEFT (probe) side so RF pushes through Repeat.
        String sql = "SELECT lo_partkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t INNER JOIN part ON t.lo_partkey = p_partkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "RF should push through when probe slot is in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterBlockByGroupingSetsPartialColumn() {
        // SetOp RF should also block pushdown through Repeat when the target slot
        // is not present in all grouping sets.
        String sql = "SELECT c_custkey FROM customer INTERSECT SELECT lo_custkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size(),
                "SetOp RF should be blocked when probe slot is not in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterPushThroughGroupingSetsCommonColumn() {
        // SetOp RF should still push through Repeat for a slot that is common to all grouping sets.
        String sql = "SELECT p_partkey FROM part INTERSECT SELECT lo_partkey FROM ("
                + "  SELECT lo_custkey, lo_partkey FROM lineorder"
                + "  GROUP BY GROUPING SETS ((lo_partkey), (lo_custkey, lo_partkey))"
                + ") t";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "SetOp RF should push through when probe slot is in all grouping sets");
    }

    @Test
    public void testSetOperationRuntimeFilterExpandThroughInnerJoin() {
        String sql = "SELECT s_suppkey FROM supplier INTERSECT "
                + "SELECT lo_suppkey FROM lineorder INNER JOIN supplier s2 ON lo_suppkey = s2.s_suppkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get().stream()
                .filter(rf -> rf.getBuilderNode() instanceof PhysicalSetOperation)
                .collect(Collectors.toList());
        Assertions.assertEquals(1, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_suppkey")));

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get().stream()
                .filter(rf -> rf.getBuilderNode() instanceof PhysicalSetOperation)
                .collect(Collectors.toList());
        Assertions.assertEquals(2, filters.size());
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_suppkey"),
                Pair.of("s_suppkey", "s_suppkey")));
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void testNotGenerateRfOnDanglingSlot() {
        String sql = "select lo_custkey from lineorder union all select c_custkey from customer union all select p_partkey from part;";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();

        /* construct plan for
         join (#18=p_partkey)
            -->join()
               -->project(null as #18, ...)
                  -->lineorder
               -->project(c_custkey#17)
                  -->customer(output: c_custkey#17, c_name#18, ...)
            -->project(p_partkey#25)
               -->part

         test purpose:
         do not generate RF by "#18=p_partkey" and apply this rf on customer
         */
        PhysicalProject<Plan> projectCustomer = (PhysicalProject<Plan>) plan.child(0).child(1);
        SlotReference cCustkey = (SlotReference) projectCustomer.getProjects().get(0);
        PhysicalProject<Plan> projectPart = (PhysicalProject<Plan>) plan.child(0).child(2);
        SlotReference pPartkey = (SlotReference) projectPart.getProjects().get(0);

        PhysicalOlapScan lo = (PhysicalOlapScan) plan.child(0).child(0).child(0);
        SlotReference loCustkey = (SlotReference) lo.getBaseOutputs().get(2);
        SlotReference loPartkey = (SlotReference) lo.getBaseOutputs().get(3);
        Alias nullAlias = new Alias(new ExprId(18), new NullLiteral(), ""); // expr#18 is used by c_name
        List<NamedExpression> projList = new ArrayList<>();
        projList.add(loCustkey);
        projList.add(loPartkey);
        projList.add(nullAlias);
        PhysicalProject projLo = new PhysicalProject(projList, null, lo);

        PhysicalHashJoin joinLoC = new PhysicalHashJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(loCustkey, cCustkey)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                null,
                projLo,
                projectCustomer
                );
        PhysicalHashJoin joinLoCP = new PhysicalHashJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(nullAlias.toSlot(), pPartkey)),
                ImmutableList.of(),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                null,
                joinLoC,
                projectPart
                );
        checker.getCascadesContext().getConnectContext().getSessionVariable().enableRuntimeFilterPrune = false;
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(joinLoCP);
        System.out.println(plan.treeString());
        Assertions.assertEquals(0, ((AbstractPhysicalPlan) plan.child(0).child(1).child(0))
                .getAppliedRuntimeFilters().size());
    }

    @Test
    public void testFunctionExprRejectedOnNonNumericType() {
        // substring() on varchar columns should NOT generate RF pushed to scan
        String sql = "SELECT * FROM supplier JOIN customer"
                + " on substring(s_name, 1, 2) = substring(c_name, 1, 2)";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(0, filters.size(),
                "substring() on varchar should not generate scan-level RF");
    }

    @Test
    public void testFunctionExprAllowedOnNumericType() {
        // abs() on numeric columns should still generate RF
        String sql = "SELECT * FROM lineorder JOIN customer"
                + " on abs(lo_custkey) = c_custkey";
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(1, filters.size(),
                "abs() on numeric type should generate scan-level RF");
    }

    @Test
    public void testExpandRfCreatesSeparateRfsPerTarget() {
        // V2-style: expansion creates separate RF objects per target.
        // Query: supplier join (lineorder join part on lo_partkey=p_partkey) on s_suppkey=lo_partkey
        // Without expand: RF(s_suppkey → lo_partkey), RF(p_partkey → lo_partkey) = 2 RFs
        // With expand:    RF(s_suppkey → lo_partkey), RF(s_suppkey → p_partkey), RF(p_partkey → lo_partkey) = 3 RFs
        String sql = "select * "
                + "from lineorder join part on lo_partkey=p_partkey "
                + "join supplier on s_suppkey=lo_partkey";

        // Without expand: 2 RFs, each with 1 target
        connectContext.getSessionVariable().enableRuntimeFilterPrune = false;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        List<RuntimeFilter> filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(2, filters.size(), "without expand: should have 2 RFs");

        // With expand: 3 separate RFs, each with 1 target
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        filters = getRuntimeFilters(sql).get();
        Assertions.assertEquals(3, filters.size(),
                "with expand: should have 3 separate RFs (V2-style, one per target)");
        // Verify the specific RF (src, target) pairs
        checkRuntimeFilterExprs(filters, ImmutableList.of(
                Pair.of("s_suppkey", "lo_partkey"),
                Pair.of("s_suppkey", "p_partkey"),
                Pair.of("p_partkey", "lo_partkey")));

        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void testLegacyRuntimeFilterKeepsSeparateMinAndMaxForSameSource() {
        int oldType = connectContext.getSessionVariable().getRuntimeFilterType();
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.MIN_MAX.getValue());
        try {
            String sql = "select * from lineorder a join supplier b"
                    + " on a.lo_partkey < b.s_suppkey and a.lo_suppkey > b.s_suppkey";
            RuntimeFilterContext context = getRuntimeFilterContext(sql);
            List<String> legacyTypes = context.getLegacyFilters().stream()
                    .map(org.apache.doris.planner.RuntimeFilter::getTypeDesc)
                    .sorted()
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, legacyTypes.size());
            Assertions.assertEquals(ImmutableList.of("max", "min"), legacyTypes);
        } finally {
            connectContext.getSessionVariable().setRuntimeFilterType(oldType);
        }
    }

    @Test
    public void testIgnoredRuntimeFilterIdDoesNotDropGroupedLegacyFilter() {
        String oldIgnoredIds = connectContext.getSessionVariable().ignoreRuntimeFilterIds;
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = true;
        try {
            String sql = "select * "
                    + "from lineorder join part on lo_partkey=p_partkey "
                    + "join supplier on s_suppkey=lo_partkey";
            RuntimeFilterContext context = getRuntimeFilterContext(sql, rfContext -> {
                List<RuntimeFilter> groupedFilters = rfContext.getNereidsRuntimeFilter().stream()
                        .filter(rf -> rf.getSrcExpr().toSql().equals("s_suppkey"))
                        .collect(Collectors.toList());
                Assertions.assertEquals(2, groupedFilters.size());
                connectContext.getSessionVariable().setIgnoreRuntimeFilterIds(
                        String.valueOf(groupedFilters.get(0).getId().asInt()));
            });
            Assertions.assertEquals(2, context.getLegacyFilters().size());
            Assertions.assertTrue(context.getLegacyFilters().stream()
                    .map(rf -> rf.getSrcExpr().toString())
                    .anyMatch("s_suppkey"::equals));
        } finally {
            connectContext.getSessionVariable().setIgnoreRuntimeFilterIds(oldIgnoredIds);
            connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
        }
    }

    @Test
    public void testRuntimeFilterBlockByRecCte() {
        String sql = new StringBuilder().append("with recursive xx as (\n").append("  select\n")
                .append("    c_custkey as c1\n").append("  from\n").append("    customer\n").append("  union\n")
                .append("  select\n").append("    xx.c1 as c1\n").append("  from\n").append("    xx\n").append(")\n")
                .append("select\n").append("    xx.c1\n").append("  from\n").append("    xx\n")
                .append("    join lineorder on lineorder.lo_custkey = xx.c1").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.OPTIMIZED_PLAN);
        MemoTestUtils.initMemoAndValidState(planner.getCascadesContext());
        new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                .translatePlan((PhysicalPlan) planner.getOptimizedPlan());
        RuntimeFilterContext context = planner.getCascadesContext().getRuntimeFilterContext();
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertEquals(0, filters.size());
    }
}
