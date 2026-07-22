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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.datasets.tpch.TPCHUtils;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AggScalarSubQueryToWindowFunctionTest extends TPCHTestBase implements MemoPatternMatchSupported {
    private static final String SQL_TEMPLATE = "    select\n"
            + "        sum(l_extendedprice) / 7.0 as avg_yearly\n"
            + "    from\n"
            + "        lineitem,\n"
            + "        part\n"
            + "    where\n"
            + "        p_partkey = l_partkey\n"
            + "        and p_brand = 'Brand#23'\n"
            + "        and p_container = 'MED BOX'\n"
            + "        (p1) (p2)";
    private static final String SUB_QUERY_TEMPLATE = "(\n"
            + "            select\n"
            + "                %s\n"
            + "            from\n"
            + "                lineitem\n"
            + "            where\n"
            + "                l_partkey = p_partkey\n"
            + "        )";
    private static final String AVG = "0.2 * avg(l_quantity)";
    private static final String MAX = "max(l_quantity) / 2";
    private static final String MIN = "min(l_extendedprice) * 5";

    private static final String[] queries = {
            buildSubQuery(AVG),
            buildSubQuery(MAX),
            buildSubQuery(MIN)
    };

    @BeforeEach
    public void addConstraints() throws Exception {
        // Add UNIQUE constraint on part.p_partkey so DataTrait recognizes
        // uniqueness for the TPC-H tables used in positive tests.
        // Ignore if constraint already exists from a previous test.
        try {
            addConstraint("alter table part add constraint uq_part_pkey unique (p_partkey)");
        } catch (Exception e) {
            // Constraint may already exist; ignore
        }
    }

    private static String buildFromTemplate(String[] predicate, String[] query) {
        String sql = SQL_TEMPLATE;
        for (int i = 0; i < predicate.length; ++i) {
            for (int j = 0; j < query.length; ++j) {
                predicate[i] = predicate[i].replace(String.format("(q%d)", j + 1), query[j]);
            }
            sql = sql.replace(String.format("(p%d)", i + 1), predicate[i]);
        }
        return sql;
    }

    private static String buildSubQuery(String res) {
        return String.format(SUB_QUERY_TEMPLATE, res);
    }

    @Test
    public void testRuleOnTPCHTest() {
        connectContext.getSessionVariable().feDebug = false;
        check(TPCHUtils.Q2);
        check(TPCHUtils.Q17);
    }

    @Disabled
    @Test
    public void testComplexPredicates() {
        // we ensure there's one sub-query in a predicate and in-predicates do not contain sub-query,
        // so we test compound predicates and sub-query in more than one predicates
        // now we disabled them temporarily, and enable when the rule support the cases.
        String[] testCases = {
                "and l_quantity > 10 or l_quantity < (q1)",
                "or l_quantity < (q3)",
                "and l_extendedprice > (q2)",
        };

        check(buildFromTemplate(new String[] {testCases[0], testCases[1]}, queries));
        check(buildFromTemplate(new String[] {testCases[0], testCases[2]}, queries));
        check(buildFromTemplate(new String[] {testCases[1], testCases[2]}, queries));
    }

    @Test
    public void testNotMatchTheRule() {
        String[] testCases = {
                // not correlated
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem);",
                // no standalone correlated table in outer scope
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem, part\n"
                        + "            where l_partkey = p_partkey);",
                // inner scope table not appear in outer scope
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem, partsupp\n"
                        + "            where l_partkey = p_partkey);",
                // inner filter not a subset of outer filter
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where\n"
                        + "        p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem\n"
                        + "            where l_partkey = p_partkey\n"
                        + "            and p_brand = 'Brand#24');",
                // inner filter not a subset of outer filter
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where\n"
                        + "        p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem\n"
                        + "            where l_partkey = p_partkey\n"
                        + "            and l_partkey = 10);",
                // inner has a mapping project
                "select\n"
                        + "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    lineitem,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            (select l_partkey, l_quantity * 0.2 as l_quantity from lineitem) b\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // inner has an unexpected filter
                "select\n"
                        + "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    lineitem,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            (select l_partkey, l_quantity from lineitem where l_quantity > 5) b\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // outer has a mapping project
                "select\n"
                        + "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    (select l_extendedprice, l_partkey, l_quantity * 0.2 as l_quantity from lineitem) b,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            lineitem\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // outer has an unexpected filter
                "select\n"
                        + "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    (select l_extendedprice, l_partkey, l_quantity from lineitem where l_quantity > 5) b,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            lineitem\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // outer has additional table
                "select\n"
                        + "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    orders,\n"
                        + "    lineitem,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l_partkey\n"
                        + "    and O_SHIPPRIORITY = 5\n"
                        + "    and O_ORDERKEY = L_ORDERKEY\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            lineitem\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // outer has same table
                "select\n"
                        + "    sum(l1.l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    lineitem l2,\n"
                        + "    lineitem l1,\n"
                        + "    part\n"
                        + "where\n"
                        + "    p_partkey = l1.l_partkey\n"
                        + "    and l2.l_partkey = 5\n"
                        + "    and l2.l_partkey = l1.l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l1.l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l_quantity)\n"
                        + "        from\n"
                        + "            lineitem\n"
                        + "        where\n"
                        + "            l_partkey = p_partkey\n"
                        + "    );",
                // outer and inner with different join condition
                "select\n"
                        + "    s_acctbal,\n"
                        + "    s_name,\n"
                        + "    n_name,\n"
                        + "    p_partkey,\n"
                        + "    p_mfgr,\n"
                        + "    s_address,\n"
                        + "    s_phone,\n"
                        + "    s_comment\n"
                        + "from\n"
                        + "    part,\n"
                        + "    supplier,\n"
                        + "    partsupp,\n"
                        + "    nation join\n"
                        + "    region on n_regionkey = r_regionkey\n"
                        + "where\n"
                        + "    p_partkey = ps_partkey\n"
                        + "    and s_suppkey = ps_suppkey\n"
                        + "    and p_size = 15\n"
                        + "    and p_type like '%BRASS'\n"
                        + "    and s_nationkey = n_nationkey\n"
                        + "    and r_name = 'EUROPE'\n"
                        + "    and ps_supplycost = (\n"
                        + "        select\n"
                        + "            min(ps_supplycost)\n"
                        + "        from\n"
                        + "            partsupp,\n"
                        + "            supplier,\n"
                        + "            nation join\n"
                        + "            region on n_regionkey = r_regionkey + 1\n"
                        + "        where\n"
                        + "            p_partkey = ps_partkey\n"
                        + "            and s_suppkey = ps_suppkey\n"
                        + "            and s_nationkey = n_nationkey\n"
                        + "            and r_name = 'EUROPE'\n"
                        + "    )\n"
                        + "order by\n"
                        + "    s_acctbal desc,\n"
                        + "    n_name,\n"
                        + "    s_name,\n"
                        + "    p_partkey\n"
                        + "limit 100;",
                // outer and inner has same table with different join condition
                "select\n"
                        + "    sum(l1.l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "    lineitem l1,\n"
                        + "    lineitem l2,\n"
                        + "    part\n"
                        + "where\n"
                        + "    l2.l_quantity + 1 = l1.l_quantity\n"
                        + "    and p_partkey = l1.l_partkey\n"
                        + "    and p_brand = 'Brand#23'\n"
                        + "    and p_container = 'MED BOX'\n"
                        + "    and l1.l_quantity < (\n"
                        + "        select\n"
                        + "            0.2 * avg(l1.l_quantity)\n"
                        + "        from\n"
                        + "            lineitem l1,\n"
                        + "            lineitem l2\n"
                        + "        where\n"
                        + "            l1.l_quantity = l2.l_quantity + 1\n"
                        + "            and l2.l_partkey = p_partkey\n"
                        + "    );"
        };
        // notice: case 4 and 5 can apply the rule, but we support it later.
        for (String s : testCases) {
            checkNot(s);
        }
    }

    @Test
    public void testWindowPartitionsByOuterOnlyRelationSlots() throws Exception {
        // Use TPC-H Q17: correlated table is part (p_partkey is unique via constraint),
        // fact table is lineitem. The window PARTITION BY should contain all output
        // columns of the correlated table (part).
        String sql = TPCHUtils.Q17;

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        Assertions.assertEquals(1, windows.size());

        LogicalWindow<Plan> window = windows.get(0);
        List<NamedExpression> windowExpressions = window.getWindowExpressions();
        Assertions.assertEquals(1, windowExpressions.size());

        WindowExpression windowExpression = (WindowExpression) windowExpressions.get(0).child(0);
        Set<String> partitionKeys = windowExpression.getPartitionKeys().stream()
                .map(Expression.class::cast)
                .filter(Slot.class::isInstance)
                .map(Slot.class::cast)
                .map(Slot::getName)
                .collect(Collectors.toSet());
        // The window PARTITION BY should include the correlated column (p_partkey)
        Assertions.assertTrue(partitionKeys.contains("p_partkey"),
                "Expected partition keys to contain p_partkey, got: " + partitionKeys);
    }

    @Test
    public void testSharedTablePredicatesStayAboveWindow() throws Exception {
        createTable("CREATE TABLE fact_split (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_split (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // Add UNIQUE constraint so the rule matches
        addConstraint("alter table dim_split add constraint uq_dim_split_k unique (k)");

        // Query with extra predicate on shared table (f.v > 6).
        // This predicate must stay ABOVE the window, otherwise the window
        // function would aggregate fewer rows than the original scalar subquery.
        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_split f, dim_split d "
                + "WHERE f.k = d.k "
                + "  AND f.v > 6"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_split f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window for this query");

        // Collect the ExprIds of the shared table (fact_split – appears in both
        // outer and inner plans).  Predicates that reference ONLY these ExprIds
        // (shared-table-only filters) must stay ABOVE the window, otherwise the
        // window function would see fewer rows than the original scalar subquery.
        List<CatalogRelation> rels = plan.collectToList(CatalogRelation.class::isInstance);
        Set<ExprId> sharedExprIds = rels.stream()
                .filter(r -> r.getTable().getName().equals("fact_split"))
                .flatMap(r -> r.getOutputExprIdSet().stream())
                .collect(Collectors.toSet());

        // Verify that no filter below the window contains a predicate whose
        // input ExprIds are all from the shared table.  Such predicates
        // (e.g. f.v > 6) must have been placed above the window.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (!conjExprIds.isEmpty() && sharedExprIds.containsAll(conjExprIds)) {
                    Assertions.fail(
                            "Shared-table-only predicate should not be below window: " + conj.toSql());
                }
            }
        }
    }

    @Test
    public void testMixedSharedOuterPredicatesStayAboveWindow() throws Exception {
        // Mixed predicates that reference both shared-table and outer-only-table
        // columns (e.g. f.v > d.tag) must stay ABOVE the window.  Pushing them
        // below would restrict the rows seen by the window function, producing
        // a different aggregate than the original scalar subquery.
        //
        // Input plan shape:
        //   Filter(f.v > d.tag, f.v * 2 > sum_alias)       ← mixed + correlated
        //     Apply(correlation: d.k)
        //       CrossJoin
        //         Scan fact f
        //         Scan dim d   -- d.k is unique (constraint)
        //       Aggregate(sum(f2.v) AS sum_alias)
        //         Filter(f2.k = d.k)
        //           Scan fact f2
        //
        // Output plan shape:
        //   Filter(f.v > d.tag, f.v * 2 > sum_over_window)  ← mixed stays ABOVE
        //     Window(sum(v) OVER (PARTITION BY d.k))
        //       Filter(f.k = d.k)                           ← join cond BELOW
        //         CrossJoin
        //           Scan fact f
        //           Scan dim d
        createTable("CREATE TABLE fact_mixed (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_mixed (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_mixed add constraint uq_dim_mixed_k unique (k)");

        // Mixed predicate: f.v > d.tag references both shared (f.v) and
        // outer-only (d.tag) columns.  It must stay ABOVE the window.
        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_mixed f, dim_mixed d "
                + "WHERE f.k = d.k "
                + "  AND f.v > d.tag"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_mixed f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window for this query");

        // Collect shared table (fact_mixed) ExprIds.
        // A mixed predicate like f.v > d.tag references BOTH shared and
        // outer-only sets.
        List<CatalogRelation> rels = plan.collectToList(CatalogRelation.class::isInstance);
        Set<ExprId> sharedExprIds = rels.stream()
                .filter(r -> r.getTable().getName().equals("fact_mixed"))
                .flatMap(r -> r.getOutputExprIdSet().stream())
                .collect(Collectors.toSet());

        // Verify: the mixed predicate f.v > d.tag must NOT be below the window.
        // A mixed predicate references slots from BOTH shared and outer-only
        // tables.  We detect it by checking that at least one ExprId is in the
        // shared set AND at least one is outside the shared set.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (conjExprIds.isEmpty()) {
                    continue;
                }
                boolean hasShared = false;
                boolean hasNonShared = false;
                for (ExprId id : conjExprIds) {
                    if (sharedExprIds.contains(id)) {
                        hasShared = true;
                    } else {
                        hasNonShared = true;
                    }
                }
                if (hasShared && hasNonShared) {
                    // Join conditions (f.k = d.k) are expected below the
                    // window — they are matched from the inner filter and
                    // needed for the join.  Only flag non-equality mixed
                    // predicates like f.v > d.tag.
                    if (!(conj instanceof org.apache.doris.nereids.trees.expressions.EqualPredicate)) {
                        Assertions.fail(
                                "Mixed shared+outer predicate should not be below window: "
                                + conj.toSql());
                    }
                }
                if (!hasNonShared) {
                    // Shared-table-only predicate also belongs ABOVE.
                    Assertions.fail(
                            "Shared-table-only predicate should not be below window: "
                            + conj.toSql());
                }
            }
        }

        // Verify the mixed predicate IS present in a filter ABOVE the window.
        // Collect all filters above the window by excluding below-window filters.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        boolean foundMixedAbove = false;
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                boolean hasShared = false;
                boolean hasNonShared = false;
                for (ExprId id : conjExprIds) {
                    if (sharedExprIds.contains(id)) {
                        hasShared = true;
                    } else {
                        hasNonShared = true;
                    }
                }
                if (hasShared && hasNonShared) {
                    foundMixedAbove = true;
                }
            }
        }
        Assertions.assertTrue(foundMixedAbove,
                "Mixed predicate f.v > d.tag should be above the window");
    }

    @Test
    public void testEnsureProjectOutputExpandsPrunedColumn() throws Exception {
        // When a nested shared-table filter is extracted and hoisted above the
        // window, any pruning project inside apply.left() that dropped a column
        // referenced by the extracted conjunct must be expanded by
        // ensureProjectOutput().  Otherwise the reinserted predicate would have
        // a dangling slot reference.
        //
        // Plan shape:
        //   Filter(sf.k = d.k, sf.k * 2 > sum_alias)     ← sf.k comparison
        //     Apply(correlation: d.k)
        //       CrossJoin
        //         SubQueryAlias sf
        //           Project(k)                             ← prunes v
        //             Filter(v > 6)                        ← nested shared filter
        //               Scan fact(k, v)
        //         Scan dim_unique d
        //       Aggregate(SUM(f2.k) AS sum_alias)
        //         Filter(f2.k = d.k)
        //           Scan fact f2
        //
        // The subquery SELECTs only k; the outer query uses sf.k for both join
        // and comparison.  The column v appears ONLY in the nested filter v > 6.
        // After extracting v > 6, ensureProjectOutput() must expand Project(k)
        // to Project(k, v) so the hoisted predicate has access to v.
        createTable("CREATE TABLE fact_ensure_proj (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_ensure_proj (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_ensure_proj add constraint uq_dim_ep_k unique (k)");

        // The subquery SELECTs only k, pruning v.  v > 6 is a nested shared-table
        // filter that must be extracted.  The rule must produce a window with
        // v > 6 hoisted above it, and ensureProjectOutput must expand the
        // pruning project to carry v through.
        String sql = "SELECT sf.k, d.did "
                + "FROM (SELECT k FROM fact_ensure_proj WHERE v > 6) sf, "
                + "    dim_ensure_proj d "
                + "WHERE sf.k = d.k "
                + "  AND sf.k * 2 > ("
                + "    SELECT SUM(f2.k) "
                + "    FROM fact_ensure_proj f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must match and produce a window.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule must produce a window for ensureProjectOutput test");

        // Walk every filter in the plan and verify that every slot referenced
        // by each conjunct is produced by the filter's child.  If
        // ensureProjectOutput() did not expand the project, the reinserted
        // predicate v > 6 would have a dangling reference to v.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : allFilters) {
            Set<ExprId> childOutput = f.child().getOutputExprIdSet();
            for (Expression conj : f.getConjuncts()) {
                for (ExprId id : conj.getInputSlotExprIds()) {
                    Assertions.assertTrue(childOutput.contains(id),
                            "Filter conjunct slot " + id
                            + " must be produced by filter's child. "
                            + "ensureProjectOutput() may not have expanded "
                            + "a pruning project. Conjunct: " + conj.toSql());
                }
            }
        }
    }

    @Test
    public void testNotMatchWhenCorrelatedTableNotUnique() throws Exception {
        createTable("CREATE TABLE tpch.fact_dup (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE tpch.dim_dup (\n"
                + "  did INT,\n"
                + "  k INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");

        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_dup f, dim_dup d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_dup f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // DUPLICATE KEY table does not guarantee uniqueness, rule should not match
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance));
    }

    @Test
    public void testCompositeUniqueConstraintTriggersRewrite() throws Exception {
        // A table with a composite UNIQUE(a, b) constraint guarantees that
        // (a, b) pairs are unique and non-null.  When both columns are used
        // as correlated slots, the window-rewrite is safe: PARTITION BY a, b
        // produces exactly one row per outer row, matching the scalar
        // subquery semantics.
        //
        // This also validates findSlotsByColumn() in LogicalCatalogRelation:
        // only declared constraints whose FULL column set is present in the
        // scan output are propagated as uniqueness slots.  A rollup that
        // drops column b from UNIQUE(a, b) would NOT see {a} as unique.
        createTable("CREATE TABLE fact_comp (\n"
                + "  id INT,\n"
                + "  a INT,\n"
                + "  b INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_comp (\n"
                + "  did INT,\n"
                + "  a INT NOT NULL,\n"
                + "  b INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_comp add constraint uq_dim_comp_ab unique (a, b)");

        // Correlate on both columns of the composite constraint.
        String sql = "SELECT d.did, d.a, d.b, d.tag, f.id, f.v "
                + "FROM fact_comp f, dim_comp d "
                + "WHERE f.a = d.a AND f.b = d.b "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_comp f2 "
                + "    WHERE f2.a = d.a AND f2.b = d.b"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Composite UNIQUE(a,b) -- both columns correlated -- should match.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Composite UNIQUE(a,b) with both columns correlated must trigger the rewrite");
    }

    @Test
    public void testNotMatchWhenCorrelatedKeyIsNullableUnique() throws Exception {
        // A nullable column with a UNIQUE constraint is still unsafe for
        // the window rewrite when the correlation uses null-safe equality
        // (<=>).  PARTITION BY groups all NULL-key outer rows into one
        // partition, so those rows can join the same NULL-key inner rows
        // and multiply the window aggregate — while the original scalar
        // subquery is evaluated per outer row independently.
        // isUniqueAndNotNull() must reject this because the key is nullable.
        createTable("CREATE TABLE fact_nullable_uk (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // k is nullable but has a UNIQUE constraint — DataTrait sees
        // uniqueness without non-null, so isUniqueAndNotNull() is false.
        createTable("CREATE TABLE dim_nullable_uk (\n"
                + "  did INT,\n"
                + "  k INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nullable_uk add constraint uq_dim_nullable_uk_k unique (k)");

        // Use <=> (null-safe equals) correlation so NULL keys can join.
        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_nullable_uk f, dim_nullable_uk d "
                + "WHERE f.k <=> d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nullable_uk f2 "
                + "    WHERE f2.k <=> d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Nullable unique key is unsafe — all null keys partition together.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Nullable unique key with <=> correlation must not be rewritten");
    }

    @Test
    public void testUniqueKeyModelTriggersRewrite() throws Exception {
        // UNIQUE KEY model tables guarantee uniqueness + non-null on the key
        // column.  DataTrait recognizes this even without an explicit
        // ADD CONSTRAINT, so the rule should fire.
        createTable("CREATE TABLE tpch.fact_ukey (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // dim_ukey has UNIQUE KEY(k) with k INT NOT NULL — this implies
        // unique + non-null without needing an explicit constraint.
        createTable("CREATE TABLE tpch.dim_ukey (\n"
                + "  k INT NOT NULL,\n"
                + "  did INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");

        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_ukey f, dim_ukey d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_ukey f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // UNIQUE KEY model alone provides uniqueness + non-null.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "UNIQUE KEY model should trigger the window rewrite");
    }

    @Test
    public void testOuterOnlyRelationOutputPreserved() throws Exception {
        // When the outer query references the dim table directly (no
        // SubQueryAlias wrapping it), the Apply's correlation slot
        // ExprId IS the scan's original.  checkRelation() finds it
        // in the outer-only table's output, so the rewrite can proceed
        // when other guards (uniqueness, filters) pass.
        createTable("CREATE TABLE fact_oor (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_oor (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_oor add constraint uq_dim_oor_k unique (k)");

        // Direct table reference — no SubQueryAlias wrapping dim_oor.
        // The Apply's correlation slot ExprId matches the scan output.
        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_oor f, dim_oor d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_oor f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // With the scan's original ExprId preserved, the rewrite should fire.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must succeed when the correlated slot ExprId "
                + "is the scan's original (direct table reference)");
    }

    @Test
    public void testNotMatchWhenOuterOnlyRelationOutputIsPruned() throws Exception {
        // An aliased projection (d.k AS new_k) creates a new ExprId in
        // the outer scope that is NOT present in the outer-only table's
        // scan output.  checkRelation() compares each correlated slot's
        // ExprId against the scan's output ExprIdSet — the aliased slot
        // is genuinely missing, so the rewrite must be rejected for this
        // reason alone (not because of a missing uniqueness constraint).
        createTable("CREATE TABLE fact_oor2 (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_oor2 (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_oor2 add constraint uq_dim_oor2_k unique (k)");

        // d.k AS new_k creates an Alias with a new ExprId.  The inner
        // query references t.new_k, so the Apply's correlation slot is
        // this new ExprId.  The scan of dim_oor2 outputs d.k with its
        // original ExprId — the check in checkRelation() fails.
        String sql = "SELECT t.id, t.new_k, t.v "
                + "FROM ("
                + "    SELECT f.id, d.k AS new_k, f.v "
                + "    FROM fact_oor2 f, dim_oor2 d "
                + "    WHERE f.k = d.k"
                + ") t "
                + "WHERE t.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_oor2 f2 "
                + "    WHERE f2.k = t.new_k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // The aliased ExprId is not in the scan's output — must reject.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when the correlated slot ExprId "
                + "is not in the outer-only relation's scan output "
                + "(e.g. d.k AS new_k creates a new ExprId)");
    }

    @Test
    public void testNestedOuterFilterHoistedAboveWindow() throws Exception {
        // When the outer child of Apply contains a nested LogicalFilter
        // (e.g. a filter pushed into the FROM subquery like
        // FROM (SELECT * FROM fact WHERE v > 6) sf), the rule must extract
        // the nested conjuncts, classify them, and hoist shared-table
        // predicates ABOVE the window.  Otherwise the window would aggregate
        // over a filtered subset, while the original scalar subquery computes
        // over ALL fact rows for the key.
        createTable("CREATE TABLE fact_nested (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nested (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nested add constraint uq_dim_nested_k unique (k)");

        // The FROM-subquery with WHERE v > 6 produces a LogicalFilter(f.v > 6)
        // nested inside apply.left() under the LogicalSubQueryAlias.
        String sql = "SELECT d.did, sf.id, sf.k, sf.v "
                + "FROM (SELECT id, k, v FROM fact_nested WHERE v > 6) sf, dim_nested d "
                + "WHERE sf.k = d.k "
                + "  AND sf.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nested f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window when nested outer filter is present");

        // The nested shared-table predicate (v > 6) must be hoisted ABOVE the
        // window.  Verify it is both absent below AND preserved above.
        List<CatalogRelation> rels = plan.collectToList(CatalogRelation.class::isInstance);
        Set<ExprId> factExprIds = rels.stream()
                .filter(r -> r.getTable().getName().equals("fact_nested"))
                .flatMap(r -> r.getOutputExprIdSet().stream())
                .collect(Collectors.toSet());
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (!conjExprIds.isEmpty() && factExprIds.containsAll(conjExprIds)) {
                    Assertions.fail(
                            "Nested shared-table predicate should be hoisted above window: "
                            + conj.toSql());
                }
            }
        }

        // Preservation: the hoisted predicate must appear exactly once in
        // a filter ABOVE the window (not just absent below).
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        int sharedOnlyAboveCount = 0;
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (!conjExprIds.isEmpty() && factExprIds.containsAll(conjExprIds)) {
                    sharedOnlyAboveCount++;
                }
            }
        }
        Assertions.assertEquals(1, sharedOnlyAboveCount,
                "Hoisted shared-table predicate (v > 6) must be preserved "
                + "exactly once above the window, not silently dropped");
    }

    @Test
    public void testVolatilePredicateStaysAboveWindow() throws Exception {
        // Volatile predicates like random() > 0.5 have no table column
        // references but are non-deterministic.  They must stay ABOVE the
        // window, otherwise the window function would aggregate over a
        // different set of rows per partition than the original scalar
        // subquery.  This follows the same principle as
        // PushDownFilterThroughWindow.canPushDown().
        createTable("CREATE TABLE fact_volatile (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_volatile (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_volatile add constraint uq_dim_volatile_k unique (k)");

        // random() > 0.5 is a volatile predicate with no input slots.
        // It must be kept ABOVE the window.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_volatile f, dim_volatile d "
                + "WHERE f.k = d.k "
                + "  AND random() > 0.5"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_volatile f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window for volatile predicate query");

        // Verify the volatile predicate is NOT below the window.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Assertions.assertFalse(conj.containsVolatileExpression(),
                        "Volatile predicate should stay above window: " + conj.toSql());
            }
        }

        // Preservation: the volatile predicate must appear exactly once in
        // a filter ABOVE the window (not just absent below).
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        int volatileAboveCount = 0;
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.containsVolatileExpression()) {
                    volatileAboveCount++;
                }
            }
        }
        Assertions.assertEquals(1, volatileAboveCount,
                "Volatile predicate (random() > 0.5) must be preserved "
                + "exactly once above the window, not silently dropped");
    }

    @Test
    public void testInnerFilterConjunctsStayBelowWindow() throws Exception {
        // Regression test: shared-table predicates that were matched against
        // inner subquery filter conjuncts must stay BELOW the window.
        //
        // Without the matchedInnerFilterConjuncts tracking, f.v < 10 would be
        // classified as shared-table-only and placed ABOVE the window, letting
        // the window aggregate over rows the original scalar subquery excluded.
        createTable("CREATE TABLE fact_inner_filter (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_inner_filter (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // UNIQUE constraint so the rule matches
        addConstraint("alter table dim_inner_filter add constraint uq_dim_if_k unique (k)");

        // Inner subquery has f2.v < 10 as a filter.  After checkFilter matches
        // it against outer f.v < 10, the outer conjunct must go BELOW the window
        // because it is semantically part of the inner aggregate's filter.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_inner_filter f, dim_inner_filter d "
                + "WHERE f.k = d.k "
                + "  AND f.v < 10"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_inner_filter f2 "
                + "    WHERE f2.k = d.k"
                + "      AND f2.v < 10"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window for this query");

        // Collect the ExprIds of the shared table (fact_inner_filter).
        List<CatalogRelation> allRels = plan.collectToList(CatalogRelation.class::isInstance);
        Set<ExprId> sharedExprIds = allRels.stream()
                .filter(r -> r.getTable().getName().equals("fact_inner_filter"))
                .flatMap(r -> r.getOutputExprIdSet().stream())
                .collect(Collectors.toSet());

        // The conjunct f.v < 10, which was matched from the inner filter, must
        // appear in a filter BELOW the window (it is part of the aggregate
        // computation).  Verify it exists there and is NOT above.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);

        // Check below-window filters: there MUST be at least one shared-table-only
        // conjunct (f.v < 10) — this is the matched inner-filter predicate.
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        boolean foundSharedOnlyBelow = false;
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (!conjExprIds.isEmpty() && sharedExprIds.containsAll(conjExprIds)) {
                    foundSharedOnlyBelow = true;
                }
            }
        }
        Assertions.assertTrue(foundSharedOnlyBelow,
                "Matched inner-filter conjunct f.v < 10 must be below the window");

        // Check above-window filters: there should NOT be a shared-table-only
        // conjunct that is NOT the window comparison.  f.v < 10 should not leak
        // above.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                Set<ExprId> conjExprIds = conj.getInputSlotExprIds();
                if (!conjExprIds.isEmpty() && sharedExprIds.containsAll(conjExprIds)) {
                    Assertions.fail(
                            "Unexpected shared-table-only predicate above window: " + conj.toSql());
                }
            }
        }
    }

    @Test
    public void testSplitInnerFilterFromPushDown() throws Exception {
        // Regression: PushDownFilterThroughProject splits the inner WHERE clause
        // into multiple LogicalFilter nodes when a correlated predicate cannot
        // be pushed through a project (references a correlated slot not in the
        // project output) but a non-correlated predicate can.
        //
        // Before the fix, checkFilter() required exactly one inner filter and
        // would reject plans where the filter was split.  Now it collects
        // conjuncts from ALL inner filters.
        createTable("CREATE TABLE fact_split2 (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_split2 (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_split2 add constraint uq_dim_split2_k unique (k)");

        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_split2 f, dim_split2 d "
                + "WHERE f.k = d.k "
                + "  AND f.v < 10 "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_split2 f2 "
                + "    WHERE f2.k = d.k"
                + "      AND f2.v < 10"
                + "  )";

        // Full regression-style pipeline: PushDownFilterThroughProject splits
        // the inner filter, MergeFilters merges adjacent filters, then the
        // custom rule rewrites.
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyTopDown(new PushDownFilterThroughProject())
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new MergeFilters())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window even when inner filter is split by "
                + "PushDownFilterThroughProject");

        // Verify f.v < 10 (matched from inner filter) is below the window
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        boolean found = false;
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.toSql().contains("v < 10")) {
                    found = true;
                }
            }
        }
        Assertions.assertTrue(found,
                "Matched inner-filter conjunct f.v < 10 must be below the window");
    }

    @Test
    public void testNestedVolatilePredicateStaysInPlace() throws Exception {
        // Volatile predicates in nested filters (inside a FROM subquery like
        // (SELECT * FROM dim WHERE random() > 0.5)) must NOT be hoisted above
        // the window or join.  Moving a volatile predicate across a join
        // changes its evaluation frequency:
        //
        //   CrossJoin(fact, (SELECT * FROM dim WHERE random()>0.5) d)
        //
        // originally evaluates random() once per dim row BEFORE the join.
        // Hoisting it above the window/join evaluates random() once per
        // joined fact row — one dim row with two matching fact rows can
        // now keep one row instead of both or none.  We must keep volatile
        // nested-filter predicates at their original child position while
        // only extracting deterministic predicates.
        createTable("CREATE TABLE fact_nested_vol (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nested_vol (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nested_vol add constraint uq_dim_nested_vol_k unique (k)");

        // random() > 0.5 is nested inside the FROM subquery on dim_nested_vol.
        // It must stay at its original position, NOT appear in the top filter
        // above the window.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_nested_vol f, "
                + "    (SELECT * FROM dim_nested_vol WHERE random() > 0.5) d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nested_vol f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match and produce a window
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule should produce a window when nested volatile filter is present");

        // The volatile predicate must NOT be above the window, AND must
        // still be present below the window (proving it was preserved, not
        // silently dropped by stripOuterFilters or a later change).
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                Assertions.assertFalse(conj.containsVolatileExpression(),
                        "Nested volatile predicate should stay at its original position, "
                        + "not be hoisted above the window: " + conj.toSql());
            }
        }
        // Prove the volatile predicate was preserved below the window.
        boolean foundVolatileBelow = false;
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.containsVolatileExpression()) {
                    foundVolatileBelow = true;
                }
            }
        }
        Assertions.assertTrue(foundVolatileBelow,
                "Nested volatile predicate must still exist below the window — "
                + "it should have been preserved at its original position, "
                + "not silently dropped");
    }

    @Test
    public void testVolatileNestedOnSharedTableRejected() throws Exception {
        // A volatile predicate nested on a SHARED table (fact, which appears
        // in both outer and inner plans) must cause the rewrite to be REJECTED.
        //
        // Keeping the volatile filter in place below the window would let the
        // window aggregate over fewer fact rows than the original scalar
        // subquery (which sums ALL fact f2 rows per key).  The same hazard
        // does not apply to volatile filters on the outer-only table because
        // the original scalar subquery does not touch that table.
        //
        // Plan shape that would be unsafe:
        //   CrossJoin
        //     Filter(random() > 0.5)       ← volatile on shared table fact
        //       Scan fact sf
        //     Scan dim_shared_vol d         ← outer-only, unique k
        //
        // After rewrite, the window would compute SUM(sf.v) OVER(PARTITION BY
        // d.k) only over random-surviving fact rows, but the original scalar
        // subquery SELECT SUM(f2.v) FROM fact f2 WHERE f2.k = d.k does NOT
        // have a random filter — it always sees all fact rows.
        createTable("CREATE TABLE fact_shared_vol (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_shared_vol (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_shared_vol add constraint uq_dim_shared_vol_k unique (k)");

        // random() > 0.5 is on the shared table fact_shared_vol (inside the
        // FROM subquery on the SHARED table).  The rewrite must be rejected.
        String sql = "SELECT d.did, sf.id, sf.k, sf.v "
                + "FROM (SELECT * FROM fact_shared_vol WHERE random() > 0.5) sf, "
                + "    dim_shared_vol d "
                + "WHERE sf.k = d.k "
                + "  AND sf.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_shared_vol f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — volatile on shared table is unsafe
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when volatile predicate is on the "
                + "shared table (fact), otherwise the window would compute "
                + "over fewer rows than the original scalar subquery");
    }

    @Test
    public void testVolatileSplitFromOuterOnlySiblingRejected() throws Exception {
        // When the top-level filter (the filter on top of the Apply) contains
        // both a volatile/NoneMovableFunction conjunct AND another deterministic
        // conjunct that would go below the window (outer-only columns), the
        // rewrite must be rejected.  Splitting conjuncts from the same filter
        // operator changes which rows reach the side-effecting predicate.
        //
        // In the original plan:
        //   Filter(d.tag > 0 AND random() > 0.5 AND f.v * 2 > (...))
        // BE evaluates both d.tag > 0 and random() > 0.5 against the full
        // input block.  After the rewrite:
        //   Filter(random() > 0.5)      ← above window
        //     Window(...)
        //       Filter(d.tag > 0)       ← below window
        // Rows rejected by d.tag > 0 never reach random() above the window,
        // changing evaluation context and potentially suppressing errors.
        createTable("CREATE TABLE fact_split_vol (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_split_vol (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_split_vol add constraint uq_dim_split_vol_k unique (k)");

        // The top-level filter contains both d.tag > 0 (outer-only → below
        // window) and random() > 0.5 (volatile → above window).  The rewrite
        // would split them — must reject.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_split_vol f, dim_split_vol d "
                + "WHERE f.k = d.k "
                + "  AND d.tag > 0"
                + "  AND random() > 0.5"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_split_vol f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — volatile and outer-only conjuncts from the
        // same filter would be split across the window.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when the same filter contains both "
                + "a volatile predicate and an outer-only predicate, because "
                + "splitting them changes which rows reach the volatile expr");
    }

    @Test
    public void testNonMovableNestedOnOuterOnlyKeptInPlace() throws Exception {
        // NoneMovableFunction predicates like assert_true() have side effects
        // and must not be moved from their original branch position.  When
        // placed on the outer-only table, preserving them in place is safe.
        createTable("CREATE TABLE fact_nm_outer (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nm_outer (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nm_outer add constraint uq_dim_nm_outer_k unique (k)");

        // assert_true(tag > 0, 'bad') is on the outer-only table dim_nm_outer.
        // It must stay at its original position below the window.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_nm_outer f, "
                + "    (SELECT * FROM dim_nm_outer WHERE assert_true(tag > 0, 'bad')) d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nm_outer f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match — NoneMovable on outer-only is safe to keep in place
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite should succeed when NoneMovableFunction predicate is "
                + "on the outer-only table");

        // The assert_true predicate must NOT be hoisted above the window,
        // AND must still be present below the window (proving it was
        // preserved, not silently dropped).
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        for (LogicalFilter<Plan> f : aboveFilters) {
            for (Expression conj : f.getConjuncts()) {
                Assertions.assertFalse(
                        conj.containsType(
                                org.apache.doris.nereids.trees.expressions.functions
                                        .NoneMovableFunction.class),
                        "NoneMovableFunction predicate should stay at its original "
                        + "position, not be hoisted above the window: "
                        + conj.toSql());
            }
        }
        // Prove the NoneMovableFunction predicate was preserved below the window.
        boolean foundNoneMovableBelow = false;
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.containsType(
                        org.apache.doris.nereids.trees.expressions.functions
                                .NoneMovableFunction.class)) {
                    foundNoneMovableBelow = true;
                }
            }
        }
        Assertions.assertTrue(foundNoneMovableBelow,
                "NoneMovableFunction predicate must still exist below the window — "
                + "it should have been preserved at its original position, "
                + "not silently dropped");
    }

    @Test
    public void testNonMovableNestedOnSharedTableRejected() throws Exception {
        // NoneMovableFunction predicates like assert_true() must not be moved
        // from their original position.  When placed on a shared table, keeping
        // them in place would restrict the window's input relative to the
        // original scalar subquery (same hazard as volatile on shared).
        createTable("CREATE TABLE fact_nm_shared (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nm_shared (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nm_shared add constraint uq_dim_nm_shared_k unique (k)");

        // assert_true(v > 0, 'bad') is nested in a WHERE clause on the shared
        // table fact_nm_shared.  The rewrite must be rejected because the
        // window would compute over a subset of fact rows, while the original
        // scalar subquery sees all.
        String sql = "SELECT d.did, sf.id, sf.k, sf.v "
                + "FROM (SELECT * FROM fact_nm_shared "
                + "       WHERE assert_true(v > 0, 'bad')) sf, "
                + "    dim_nm_shared d "
                + "WHERE sf.k = d.k "
                + "  AND sf.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nm_shared f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — NoneMovable on shared table is unsafe
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when NoneMovableFunction predicate "
                + "is on the shared table");
    }

    @Test
    public void testNoneMovableInnerFilterRejected() throws Exception {
        // checkFilter() must reject NoneMovableFunction predicates in the
        // inner subquery filter, just like volatile expressions.  Two
        // syntactically identical assert_true() calls — one in the outer
        // filter, one in the inner filter — are independent evaluations:
        // the inner one is part of the aggregate filter, the outer one is
        // a per-row assertion.  ExpressionIdenticalChecker would match them
        // structurally (same class + children after slot replacement),
        // collapsing the independent evaluations into one below-window
        // predicate and effectively pruning one side-effecting assertion.
        // This guard must reject the match so the rule does not fire.
        createTable("CREATE TABLE fact_nm_inner (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nm_inner (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nm_inner add constraint uq_dim_nm_inner_k unique (k)");

        // assert_true(f.v > 0, 'bad') appears in BOTH the outer WHERE and
        // the inner subquery WHERE.  They are independent per-row
        // assertions — the rule must NOT collapse them into one.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_nm_inner f, dim_nm_inner d "
                + "WHERE f.k = d.k "
                + "  AND assert_true(f.v > 0, 'bad')"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nm_inner f2 "
                + "    WHERE f2.k = d.k"
                + "      AND assert_true(f2.v > 0, 'bad')"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — NoneMovableFunction inner conjunct must not
        // be matched against an outer NoneMovableFunction conjunct.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when inner subquery filter contains "
                + "NoneMovableFunction predicates, even if they structurally "
                + "match outer conjuncts");
    }

    @Test
    public void testNoneMovableInTopLevelWhereKeptAboveWindow() throws Exception {
        // A NoneMovableFunction predicate in the top-level outer WHERE
        // that references only the outer-only table — e.g.
        // assert_true(d.tag > 0, 'bad') — must NOT be pushed below the
        // window.  Without the guard, it falls through the split: it is
        // not volatile, not matched-inner, and hasShared=false, so it
        // ends up in belowWindowConjuncts, moving a side-effecting
        // evaluation to a different plan location.
        createTable("CREATE TABLE fact_nm_toplevel (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_nm_toplevel (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_nm_toplevel add constraint uq_dim_nm_toplevel_k unique (k)");

        // assert_true(d.tag > 0, 'bad') is in the top-level WHERE and
        // references only dim_nm_toplevel (outer-only).  It must stay
        // ABOVE the window, not be pushed below.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_nm_toplevel f, dim_nm_toplevel d "
                + "WHERE f.k = d.k "
                + "  AND assert_true(d.tag > 0, 'bad')"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_nm_toplevel f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match — NoneMovable on outer-only is safe when
        // kept at its original position above the window.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite should succeed when NoneMovableFunction predicate "
                + "is on outer-only table and kept above the window");

        // The assert_true predicate must NOT be pushed below the window.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> belowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : belowFilters) {
            for (Expression conj : f.getConjuncts()) {
                Assertions.assertFalse(
                        conj.containsType(
                                org.apache.doris.nereids.trees.expressions.functions
                                        .NoneMovableFunction.class),
                        "NoneMovableFunction predicate must stay above the "
                        + "window, not be pushed below: " + conj.toSql());
            }
        }

        // Prove the NoneMovableFunction predicate still exists above the
        // window — it must not be silently dropped.  All filters that are
        // not ancestors/descendants of the window's child are above-window
        // filters in the rewritten plan.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        List<LogicalFilter<Plan>> aboveOnlyFilters = allFilters.stream()
                .filter(f -> !belowFilters.contains(f))
                .collect(Collectors.toList());
        boolean foundNoneMovableAbove = false;
        for (LogicalFilter<Plan> f : aboveOnlyFilters) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.containsType(
                        org.apache.doris.nereids.trees.expressions.functions
                                .NoneMovableFunction.class)) {
                    foundNoneMovableAbove = true;
                }
            }
        }
        Assertions.assertTrue(foundNoneMovableAbove,
                "NoneMovableFunction predicate must exist above the window — "
                + "it should not be silently dropped");
    }

    @Test
    public void testVolatileInnerFilterRejected() throws Exception {
        // checkFilter() must reject volatile expressions in the inner subquery
        // filter.  Two syntactically identical volatile calls — like
        // volatile_bool_udf(f.k) in the outer filter and
        // volatile_bool_udf(f2.k) in the inner filter — are independent
        // evaluations with distinct VolatileIdentity.  ExpressionIdenticalChecker
        // would match them structurally (same class + children), collapsing
        // the independent outer-row filter and inner-aggregate filter into one
        // predicate.  Use random() > 0.5 as a built-in volatile predicate.
        createTable("CREATE TABLE fact_vinner (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_vinner (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_vinner add constraint uq_dim_vinner_k unique (k)");

        // random() > 0.5 appears in both the outer WHERE and the inner WHERE.
        // Even though they look identical, they are independent volatile calls.
        // checkFilter must reject this match.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_vinner f, dim_vinner d "
                + "WHERE f.k = d.k "
                + "  AND random() > 0.5"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_vinner f2 "
                + "    WHERE f2.k = d.k"
                + "      AND random() > 0.5"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — volatile inner conjunct cannot be matched
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when inner subquery filter contains "
                + "volatile predicates, even if they structurally match outer "
                + "conjuncts");
    }

    @Test
    public void testStateClearBetweenSiblingCandidates() throws Exception {
        // The same rule instance processes every LogicalFilter in one
        // rewriteRoot() call via deep traversal.  Two FROM-subquery
        // branches under a CrossJoin produce two sibling Filter→Apply
        // candidates.  The left branch is rejected (non-unique table);
        // the right branch should succeed (unique table).  Without the
        // clear() calls in check(), the rejected candidate's aggregate
        // leaks into functions, making checkAggregate() see
        // functions.size() != 1 for the valid candidate.
        createTable("CREATE TABLE fact_clear (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // dim_clear_dup: DUPLICATE KEY → k is NOT unique → rejected
        createTable("CREATE TABLE dim_clear_dup (\n"
                + "  did INT,\n"
                + "  k INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // dim_clear_uniq: add UNIQUE constraint → k IS unique → valid
        createTable("CREATE TABLE dim_clear_uniq (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_clear_uniq add constraint uq_dim_clear_uniq_k unique (k)");

        // Two FROM subqueries produce two sibling Filter→Apply candidates
        // under the CrossJoin.  The left one is rejected (dim_clear_dup.k
        // is not unique), the right one is valid (dim_clear_uniq.k is unique).
        // If per-candidate state is not cleared, the left candidate's
        // aggregate leaks into the right candidate's checkAggregate().
        String sql = "SELECT a.id, b.id "
                + "FROM "
                + "  (SELECT f.id FROM fact_clear f, dim_clear_dup d "
                + "    WHERE f.k = d.k "
                + "      AND f.v * 2 > ("
                + "        SELECT SUM(f2.v) FROM fact_clear f2 WHERE f2.k = d.k"
                + "      )) a, "
                + "  (SELECT f.id FROM fact_clear f, dim_clear_uniq du "
                + "    WHERE f.k = du.k "
                + "      AND f.v * 3 > ("
                + "        SELECT SUM(f2.v) FROM fact_clear f2 WHERE f2.k = du.k"
                + "      )) b";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // The right branch should be rewritten to a window.
        // If clear() is missing, stale state from the left (rejected)
        // branch contaminates the right candidate and no window appears.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Valid right candidate should produce a window after a "
                + "rejected left candidate, proving per-candidate state "
                + "is cleared in check()");
    }

    @Test
    public void testPrunedAggregateSlotExpandsProject() throws Exception {
        // The window function's aggregate (after slot replacement) references
        // shared-table slots.  When a pruning project inside apply.left()
        // drops those slots and there are no nested filters to extract,
        // ensureProjectOutput must still expand the project to carry the
        // aggregate's input slots through.  Otherwise the window child does
        // not expose the column and the rewritten plan has a dangling ref.
        //
        // Plan shape:
        //   Filter(sf.k = d.k, sf.k * 2 > sum_alias)
        //     Apply(correlation: d.k)
        //       CrossJoin
        //         SubQueryAlias sf
        //           Project(k)                     ← prunes v
        //             Scan fact(k, v)
        //         Scan dim_unique(k)
        //       Aggregate(SUM(f2.v) AS sum_alias)
        //         Filter(f2.k = d.k)
        //           Scan fact f2
        //
        // After slot replacement SUM(f2.v) → SUM(sf.v).  Without the fix,
        // Project(k) is not expanded because extractedConjunctExprIds is
        // empty (no nested filters).  The window references sf.v which is
        // not in the project output — dangling reference.
        createTable("CREATE TABLE fact_agg_prune (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_agg_prune (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_agg_prune add constraint uq_dim_agg_prune_k unique (k)");

        // Subquery prunes v; window aggregate SUM(f2.v) needs sf.v.
        // No nested filter to extract; extractedConjunctExprIds is empty.
        String sql = "SELECT sf.k, d.did "
                + "FROM (SELECT k FROM fact_agg_prune) sf, "
                + "    dim_agg_prune d "
                + "WHERE sf.k = d.k "
                + "  AND sf.k * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_agg_prune f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must match and produce a window.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule must produce a window when aggregate slot is pruned");

        // Walk every filter; verify every conjunct slot is produced by
        // the filter's child.  Also walk every window and verify its
        // input slots are produced by the window's child.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : allFilters) {
            Set<ExprId> childOutput = f.child().getOutputExprIdSet();
            for (Expression conj : f.getConjuncts()) {
                for (ExprId id : conj.getInputSlotExprIds()) {
                    Assertions.assertTrue(childOutput.contains(id),
                            "Filter conjunct slot " + id
                            + " not produced by child. Conjunct: "
                            + conj.toSql());
                }
            }
        }
        List<LogicalWindow<Plan>> windows = plan
                .collectToList(LogicalWindow.class::isInstance);
        for (LogicalWindow<Plan> w : windows) {
            Set<ExprId> childOutput = w.child().getOutputExprIdSet();
            for (NamedExpression ne : w.getWindowExpressions()) {
                for (ExprId id : ne.getInputSlotExprIds()) {
                    Assertions.assertTrue(childOutput.contains(id),
                            "Window expression slot " + id
                            + " not produced by child. Expression: "
                            + ne.toSql());
                }
            }
        }
    }

    @Test
    public void testDifferentUdfNamesRejectedInCheckFilter() throws Exception {
        // checkFilter() must reject deterministic UDF predicates when the
        // inner and outer UDF have different function names, even if they
        // share the same runtime class (PythonUdf / JavaUdf) and have the
        // same children after slot replacement.
        //
        // ExpressionIdenticalChecker dispatches UDF wrapper nodes to the
        // generic visitor, which only checks runtime class and children.
        // Two syntactically identical calls to different UDFs — like
        // outer_bool_udf(f.v) and inner_bool_udf(f2.v) after slot
        // replacement — would incorrectly match without the BoundFunction
        // name guard.  The match would then collapse the independent
        // outer and inner evaluations into a single below-window
        // predicate, silently replacing the inner aggregate filter with
        // the outer UDF.
        //
        // Both UDFs are registered as IMMUTABLE so the volatile guard
        // does not mask the bug.
        createFunction("CREATE FUNCTION outer_bool_udf(INT) RETURNS BOOLEAN "
                + "PROPERTIES ("
                + "  'type'='PYTHON_UDF',"
                + "  'symbol'='evaluate',"
                + "  'runtime_version'='3.10.2',"
                + "  'volatility'='immutable'"
                + ")");
        createFunction("CREATE FUNCTION inner_bool_udf(INT) RETURNS BOOLEAN "
                + "PROPERTIES ("
                + "  'type'='PYTHON_UDF',"
                + "  'symbol'='evaluate',"
                + "  'runtime_version'='3.10.2',"
                + "  'volatility'='immutable'"
                + ")");

        createTable("CREATE TABLE fact_udf_name (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_udf_name (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_udf_name add constraint uq_dim_udf_name_k unique (k)");

        // outer_bool_udf(f.v) in outer WHERE, inner_bool_udf(f2.v) in inner
        // WHERE.  After slot replacement, inner_bool_udf(f2.v) →
        // inner_bool_udf(f.v).  Both are PythonUdf with identical children
        // but different function names — ExpressionIdenticalChecker must
        // NOT match them.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_udf_name f, dim_udf_name d "
                + "WHERE f.k = d.k "
                + "  AND outer_bool_udf(f.v)"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_udf_name f2 "
                + "    WHERE f2.k = d.k"
                + "      AND inner_bool_udf(f2.v)"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — the two UDFs have different names and
        // are different functions, even though they share the same
        // PythonUdf wrapper class and have identical children after
        // slot replacement.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when inner UDF has a different "
                + "name than the structurally identical outer UDF — "
                + "BoundFunction name must be compared");
    }

    @Test
    public void testMatchWithDifferentAnalyzerRejected() throws Exception {
        // ExpressionIdenticalChecker must not match two MATCH_ANY
        // predicates with different USING ANALYZER clauses.  The
        // analyzer determines which rows are included, so 'english'
        // and 'chinese' produce different row sets.  Matching them
        // would collapse the independent outer and inner filters
        // and compute the window aggregate under the wrong analyzer.
        createTable("CREATE TABLE fact_match_analyzer (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT,\n"
                + "  txt STRING,\n"
                + "  INDEX idx_txt (`txt`) USING INVERTED\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_match_analyzer (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_match_analyzer add constraint uq_dim_match_analyzer_k unique (k)");

        // Outer MATCH_ANY uses analyzer 'english', inner uses 'chinese'.
        // After slot replacement, both are MatchAny with the same
        // children but different analyzers — must NOT be matched.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_match_analyzer f, dim_match_analyzer d "
                + "WHERE f.k = d.k "
                + "  AND f.txt MATCH_ANY 'foo' USING ANALYZER 'english'"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_match_analyzer f2 "
                + "    WHERE f2.k = d.k"
                + "      AND f2.txt MATCH_ANY 'foo' USING ANALYZER 'chinese'"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — MATCH with different analyzers are
        // different predicates even though they share the same
        // MatchAny class and children after slot replacement.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when MATCH predicates have "
                + "different USING ANALYZER clauses");
    }

    @Test
    public void testCommutativePredicateMatchBreaksAfterFirstHit() throws Exception {
        // When the outer filter contains both ordered forms of the same
        // equi-join condition (f.k = d.k and d.k = f.k),
        // visitComparisonPredicate accepts both via cp.commute().
        // Without the break in checkFilter(), the inner loop continues
        // after a successful match and calls innerIterator.remove()
        // twice without a next(), throwing IllegalStateException.
        createTable("CREATE TABLE fact_commute (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_commute (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_commute add constraint uq_dim_commute_k unique (k)");

        // Outer filter has both f.k = d.k and d.k = f.k.
        // Inner filter has f2.k = d.k → after slot replacement → f.k = d.k.
        // visitComparisonPredicate matches both forms.  The break after
        // the first match prevents IllegalStateException.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_commute f, dim_commute d "
                + "WHERE f.k = d.k "
                + "  AND d.k = f.k"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_commute f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should succeed — both ordered forms are the same join
        // condition and the break prevents a double-remove crash.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite should succeed when outer filter has both "
                + "ordered forms of the same equi-join condition");
    }

    @Test
    public void testCastWithDifferentTargetTypeRejected() throws Exception {
        // ExpressionIdenticalChecker must not match two TRY_CAST
        // predicates with different target types.  The old generic
        // visit() only checked class + children, ignoring the target
        // type that Cast.equals() compares.  TRY_CAST(f.s AS INT) and
        // TRY_CAST(f2.s AS DATE) share the same TryCast class and the
        // same slot child after replacement, but target INT ≠ DATE.
        createTable("CREATE TABLE fact_cast (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT,\n"
                + "  s STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_cast (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_cast add constraint uq_dim_cast_k unique (k)");

        // Outer: TRY_CAST(f.s AS INT) IS NULL
        // Inner: TRY_CAST(f2.s AS DATE) IS NULL → after slot replacement
        //        TRY_CAST(f.s AS DATE) IS NULL
        // Same TryCast class, same child, different target types.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_cast f, dim_cast d "
                + "WHERE f.k = d.k "
                + "  AND TRY_CAST(f.s AS INT) IS NULL"
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_cast f2 "
                + "    WHERE f2.k = d.k"
                + "      AND TRY_CAST(f2.s AS DATE) IS NULL"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — CAST/TRY_CAST with different target
        // types are semantically different predicates.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when CAST/TRY_CAST predicates "
                + "have different target types");
    }

    @Test
    public void testUnsafeFilterPreservesSubtreeFilters() throws Exception {
        // stripOuterFilters must not strip deterministic filters from
        // below a retained unsafe (volatile/NoneMovableFunction) filter.
        // If it recurses into the child of an unsafe filter and hoists
        // a safe predicate, the unsafe predicate evaluates over a
        // different row set — its input domain silently changes.
        //
        // Plan shape inside apply.left():
        //   SubQueryAlias sf
        //     Filter(assert_true(tag > 0, 'bad'))   ← unsafe, must stay
        //       Project(tag, k, ...)                ← slot-only
        //         Filter(tag > 0)                   ← safe, must stay below
        //           Scan dim_unique
        createTable("CREATE TABLE fact_unsafe_barrier (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // dim_unsafe_sf: outer-only table with nested assert_true + tag > 0
        createTable("CREATE TABLE dim_unsafe_sf (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_unsafe_sf add constraint uq_dim_unsafe_sf_k unique (k)");

        // Double-nested subquery on dim_unsafe_sf (outer-only): inner
        // WHERE tag > 0, outer WHERE assert_true(tag > 0, 'bad').
        // After analysis this produces:
        //   SubQueryAlias sf
        //     Filter(assert_true)          ← unsafe, must stay
        //       Project(tag, k, ...)       ← slot-only
        //         Filter(tag > 0)          ← safe, must stay below unsafe
        //           Scan dim_unsafe_sf
        // fact_unsafe_barrier is the shared table.
        String sql = "SELECT sf.did, sf.k, f.v "
                + "FROM ("
                + "  SELECT * FROM ("
                + "    SELECT * FROM dim_unsafe_sf WHERE tag > 0"
                + "  ) inner_sf"
                + "  WHERE assert_true(tag > 0, 'bad')"
                + ") sf, "
                + "fact_unsafe_barrier f "
                + "WHERE sf.k = f.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_unsafe_barrier f2 "
                + "    WHERE f2.k = sf.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match — the unsafe filter is on outer-only table
        // and the subtree filter is preserved below it.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite should succeed when unsafe filter on outer-only "
                + "table has a subtree filter preserved below it");

        // The safe filter (tag > 0) must remain as a descendant of the
        // unsafe filter (assert_true) in the plan — stripOuterFilters
        // must not hoist it from under a retained unsafe filter.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);

        // Find the assert_true filter below the window — it must still
        // have Filter(tag > 0) as a descendant.
        List<LogicalFilter<Plan>> allBelowFilters = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        boolean safeBelowUnsafe = false;
        for (LogicalFilter<Plan> f : allBelowFilters) {
            boolean hasNoneMovableFunction = f.getConjuncts().stream()
                    .anyMatch(c -> c.containsType(
                            org.apache.doris.nereids.trees.expressions.functions
                                    .NoneMovableFunction.class));
            if (!hasNoneMovableFunction) {
                continue;
            }

            // This filter has assert_true — verify it has Filter(tag > 0)
            // somewhere in its subtree (not just below the window).
            List<LogicalFilter<Plan>> subtreeFilters = f.child(0)
                    .collectToList(LogicalFilter.class::isInstance);
            for (LogicalFilter<Plan> sf : subtreeFilters) {
                for (Expression conj : sf.getConjuncts()) {
                    if (conj.toSql().contains("tag > 0")
                            && !conj.containsType(
                                    org.apache.doris.nereids.trees.expressions.functions
                                            .NoneMovableFunction.class)) {
                        safeBelowUnsafe = true;
                    }
                }
            }
        }
        Assertions.assertTrue(safeBelowUnsafe,
                "Deterministic filter (tag > 0) must remain in the subtree "
                + "below the assert_true filter — stripOuterFilters must "
                + "not hoist it from under a retained unsafe filter");
    }

    @Test
    public void testNoAggregateOutputConjunctRejected() throws Exception {
        // When the Filter directly contains an Apply (Filter→Apply shape)
        // but none of its conjuncts reference the aggregate output ExprId,
        // conjuncts.get(false) is null and rewrite() must not NPE.
        //
        // This shape cannot be produced naturally through SQL analysis:
        // every scalar subquery comparison (e.g. f.v > sum_alias) is always
        // a conjunct that references the aggregate output.  We therefore
        // construct the plan from a valid query and then strip the
        // agg-output conjunct from the top-level Filter so the null guard
        // in rewrite() is exercised.
        createTable("CREATE TABLE fact_no_agg_conj (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_no_agg_conj (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_no_agg_conj add constraint uq_dim_no_agg_conj_k unique (k)");

        // Build a valid Filter→Apply shape with the subquery comparison.
        // We will then strip the agg-output conjunct before handing it
        // to the rule.
        String sql = "SELECT d.did, f.id, f.k, f.v "
                + "FROM fact_no_agg_conj f, dim_no_agg_conj d "
                + "WHERE f.k = d.k "
                + "  AND f.v > ("
                + "    SELECT SUM(f2.v) FROM fact_no_agg_conj f2 WHERE f2.k = d.k"
                + "  )";

        Plan preRule = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .getPlan();

        // Precondition: a correlated Apply exists, so the rule will
        // reach check() / rewrite().
        Assertions.assertTrue(preRule.anyMatch(p -> p instanceof LogicalApply
                && ((LogicalApply<?, ?>) p).isCorrelated()),
                "Pre-rule plan must have a correlated Apply");

        // Locate the aggregate output ExprId so we can strip the
        // conjunct that references it.
        List<LogicalAggregate<Plan>> aggs = preRule
                .collectToList(LogicalAggregate.class::isInstance);
        Assertions.assertEquals(1, aggs.size());
        Set<ExprId> aggOutputExprIds = aggs.get(0).getOutputExprIdSet();

        // Walk all LogicalFilter nodes in the plan to find the one
        // sitting directly above the Apply (possibly through a Project).
        // The preRule root may be a Project wrapping the Filter→Apply
        // chain after PullUpProjectUnderApply.
        List<LogicalFilter<Plan>> allFilters = preRule
                .collectToList(LogicalFilter.class::isInstance);
        LogicalFilter<?> targetFilter = null;
        LogicalApply<?, ?> targetApply = null;
        for (LogicalFilter<Plan> f : allFilters) {
            Plan child = f.child(0);
            Plan maybeApply = child instanceof LogicalProject
                    ? child.child(0) : child;
            if (maybeApply instanceof LogicalApply) {
                targetFilter = f;
                targetApply = (LogicalApply<?, ?>) maybeApply;
                break;
            }
        }
        Assertions.assertNotNull(targetFilter,
                "Must find a LogicalFilter directly above the Apply");
        Assertions.assertNotNull(targetApply,
                "Must find a LogicalApply below the Filter");

        // Remove every conjunct from the target filter that references
        // the aggregate output, leaving only table-column predicates
        // (f.k = d.k).  This makes conjuncts.get(false) null in rewrite().
        Set<Expression> remainingConjuncts = targetFilter.getConjuncts().stream()
                .filter(c -> Sets.intersection(
                        c.getInputSlotExprIds(), aggOutputExprIds).isEmpty())
                .collect(Collectors.toSet());
        Assertions.assertTrue(
                remainingConjuncts.size() < targetFilter.getConjuncts().size(),
                "At least one agg-output conjunct must have been stripped");

        // Build a new plan whose filter above the Apply has only the
        // non-agg-output conjuncts, and pass it to the rule.
        Plan strippedChild = new LogicalFilter<>(
                remainingConjuncts, (Plan) targetFilter.child(0));

        Plan plan = new AggScalarSubQueryToWindowFunction()
                .rewriteRoot(strippedChild, null);

        // Rule must NOT match — no aggregate-output conjunct in Filter
        // means correlatedConjuncts is null, and rewrite() must return
        // the plan unchanged.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite must be rejected when no outer conjunct "
                + "references the aggregate output");
    }

    @Test
    public void testNestedFilterBelowUnsafeNotExtracted() throws Exception {
        // The nested-filter extraction loop in rewrite() must stop at
        // unsafe filter barriers, matching stripOuterFilters() semantics.
        // Without the barrier, a deterministic filter below assert_true
        // is collected and reinserted above the join while the original
        // remains in place — the predicate evaluates twice per joined row.
        createTable("CREATE TABLE fact_unsafe_nested (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_unsafe_nested (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT,\n"
                + "  keep INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_unsafe_nested add constraint uq_dim_unsafe_nested_k unique (k)");

        // Double-nested subquery on dim_unsafe_nested (outer-only):
        // inner WHERE keep > 0, outer WHERE assert_true(tag > 0, 'bad').
        // Plan shape:
        //   SubQueryAlias sf
        //     Filter(assert_true)        ← unsafe barrier
        //       Project                  ← slot-only
        //         Filter(keep > 0)       ← must NOT be extracted
        //           Scan dim_unsafe_nested
        // Without the barrier in collectStrippableFilters, Filter(keep>0)
        // is added to belowWindowConjuncts while remaining below assert_true.
        String sql = "SELECT sf.did, sf.k, f.v "
                + "FROM ("
                + "  SELECT * FROM ("
                + "    SELECT * FROM dim_unsafe_nested WHERE keep > 0"
                + "  ) inner_sf"
                + "  WHERE assert_true(tag > 0, 'bad')"
                + ") sf, "
                + "fact_unsafe_nested f "
                + "WHERE sf.k = f.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_unsafe_nested f2 "
                + "    WHERE f2.k = sf.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule should match — the unsafe filter is on outer-only table.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rewrite should succeed when unsafe filter on outer-only "
                + "table has a deterministic filter in its subtree");

        // The keep > 0 predicate must appear exactly once below the window
        // (in its original subtree below assert_true), not duplicated.
        List<LogicalWindow<Plan>> windows = plan.collectToList(LogicalWindow.class::isInstance);
        LogicalWindow<Plan> window = windows.get(0);
        Plan belowWindow = window.child(0);
        List<LogicalFilter<Plan>> allBelow = belowWindow
                .collectToList(LogicalFilter.class::isInstance);
        int keepGt0Count = 0;
        for (LogicalFilter<Plan> f : allBelow) {
            for (Expression conj : f.getConjuncts()) {
                if (conj.toSql().contains("keep > 0")
                        && !conj.containsType(
                                org.apache.doris.nereids.trees.expressions.functions
                                        .NoneMovableFunction.class)) {
                    keepGt0Count++;
                }
            }
        }
        Assertions.assertEquals(1, keepGt0Count,
                "Deterministic filter (keep > 0) must appear exactly once "
                + "below the window — not duplicated by the extraction loop");
    }

    @Test
    public void testStackedPruningProjectsExpanded() throws Exception {
        // When the shared table is behind multiple stacked pruning
        // projects — e.g. SubQueryAlias sf → Project(k) → Project(k)
        // → Scan(k,v) — ensureProjectOutput() must recurse into the
        // child before computing each project's childOutput.  If it
        // reads childOutput from the unexpanded child first, the outer
        // project cannot pull v through because its immediate child
        // (the inner project) only outputs k before expansion.
        //
        // Plan shape:
        //   Filter(sf.k = d.k, sf.k * 2 > sum_alias)
        //     Apply(correlation: d.k)
        //       CrossJoin
        //         SubQueryAlias sf
        //           Project(k)                     ← outer prune
        //             Project(k)                   ← inner prune
        //               Scan fact(k, v)
        //         Scan dim_unique(k)
        //       Aggregate(SUM(f2.v) AS sum_alias)
        //         Filter(f2.k = d.k)
        //           Scan fact f2
        createTable("CREATE TABLE fact_stacked (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_stacked (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_stacked add constraint uq_dim_stacked_k unique (k)");

        // Two nested SELECT subqueries produce stacked pruning projects.
        // Inner: SELECT id, k → Project(id, k) — slot-only, survives
        //        elimination (outputs {id,k} ≠ child's {id,k,v}).
        // Outer: SELECT k → Project(k) — slot-only, survives
        //        elimination (outputs {k} ≠ child's {id,k}).
        // Both are slot-only so checkProject() passes.  v is pruned at
        // both levels and only available at the Scan.
        // Without the depth-first fix, ensureProjectOutput reads
        // childOutput from the unexpanded inner project (only {id,k}),
        // fails to add v to the outer project, and the window's
        // SUM(sf.v) references a slot not produced by its child.
        String sql = "SELECT sf.k, d.did "
                + "FROM (SELECT k FROM "
                + "        (SELECT id, k FROM fact_stacked) t"
                + "     ) sf, "
                + "    dim_stacked d "
                + "WHERE sf.k = d.k "
                + "  AND sf.k * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_stacked f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must match and produce a window.
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance),
                "Rule must produce a window when shared table is behind "
                + "stacked pruning projects");

        // Walk every filter; verify every conjunct slot is produced by
        // the filter's child.  Also walk every window and verify its
        // input slots are produced by the window's child.
        List<LogicalFilter<Plan>> allFilters = plan
                .collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<Plan> f : allFilters) {
            Set<ExprId> childOutput = f.child().getOutputExprIdSet();
            for (Expression conj : f.getConjuncts()) {
                for (ExprId id : conj.getInputSlotExprIds()) {
                    Assertions.assertTrue(childOutput.contains(id),
                            "Filter conjunct slot " + id
                            + " not produced by child. Conjunct: "
                            + conj.toSql());
                }
            }
        }
        List<LogicalWindow<Plan>> windows = plan
                .collectToList(LogicalWindow.class::isInstance);
        for (LogicalWindow<Plan> w : windows) {
            Set<ExprId> childOutput = w.child().getOutputExprIdSet();
            for (NamedExpression ne : w.getWindowExpressions()) {
                for (ExprId id : ne.getInputSlotExprIds()) {
                    Assertions.assertTrue(childOutput.contains(id),
                            "Window expression slot " + id
                            + " not produced by child. Expression: "
                            + ne.toSql());
                }
            }
        }
    }

    @Test
    public void testSkipStorageEngineMergeRejected() throws Exception {
        // skip_storage_engine_merge = true tells BE to skip merging
        // multiple versions of the same key.  The scan may return
        // duplicate key rows, so DataTrait must not advertise uniqueness
        // even for tables that are otherwise unique (declared constraints,
        // OLAP key metadata).  The WinMagic rewrite must be suppressed.
        createTable("CREATE TABLE fact_sse (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_sse (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_sse add constraint uq_dim_sse_k unique (k)");

        boolean saved = connectContext.getSessionVariable().skipStorageEngineMerge;
        connectContext.getSessionVariable().skipStorageEngineMerge = true;
        try {
            String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                    + "FROM fact_sse f, dim_sse d "
                    + "WHERE f.k = d.k "
                    + "  AND f.v * 2 > ("
                    + "    SELECT SUM(f2.v) "
                    + "    FROM fact_sse f2 "
                    + "    WHERE f2.k = d.k"
                    + "  )";

            Plan plan = PlanChecker.from(createCascadesContext(sql))
                    .analyze(sql)
                    .applyBottomUp(new PullUpProjectUnderApply())
                    .applyTopDown(new PushDownFilterThroughProject())
                    .customRewrite(new EliminateUnnecessaryProject())
                    .customRewrite(new AggScalarSubQueryToWindowFunction())
                    .getPlan();

            Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                    "skipStorageEngineMerge exposes unmerged versions — "
                    + "uniqueness is not guaranteed, rewrite must be rejected");
        } finally {
            connectContext.getSessionVariable().skipStorageEngineMerge = saved;
        }
    }

    @Test
    public void testSkipDeleteBitmapRejected() throws Exception {
        // skip_delete_bitmap = true on a UNIQUE_KEYS table makes deleted
        // (replaced) rows visible alongside their replacements.  Two rows
        // with the same unique key coexist, so DataTrait must not
        // advertise uniqueness.  The WinMagic rewrite must be suppressed.
        createTable("CREATE TABLE fact_sdb (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // UNIQUE_KEYS table — the UNIQUE KEY model would normally make
        // DataTrait advertise uniqueness on the key column k.
        // Key columns must be declared first in the schema.
        createTable("CREATE TABLE dim_sdb (\n"
                + "  k INT NOT NULL,\n"
                + "  did INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");

        boolean saved = connectContext.getSessionVariable().skipDeleteBitmap;
        connectContext.getSessionVariable().skipDeleteBitmap = true;
        try {
            String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                    + "FROM fact_sdb f, dim_sdb d "
                    + "WHERE f.k = d.k "
                    + "  AND f.v * 2 > ("
                    + "    SELECT SUM(f2.v) "
                    + "    FROM fact_sdb f2 "
                    + "    WHERE f2.k = d.k"
                    + "  )";

            Plan plan = PlanChecker.from(createCascadesContext(sql))
                    .analyze(sql)
                    .applyBottomUp(new PullUpProjectUnderApply())
                    .applyTopDown(new PushDownFilterThroughProject())
                    .customRewrite(new EliminateUnnecessaryProject())
                    .customRewrite(new AggScalarSubQueryToWindowFunction())
                    .getPlan();

            Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                    "skipDeleteBitmap exposes replaced rows with same key — "
                    + "uniqueness is not guaranteed, rewrite must be rejected");
        } finally {
            connectContext.getSessionVariable().skipDeleteBitmap = saved;
        }
    }

    @Test
    public void testReadMorAsDupRejected() throws Exception {
        // read_mor_as_dup_tables = '*' on a MOR table makes the scan read
        // it as a DUPLICATE table, so the declared unique key cannot be
        // trusted.  DataTrait must not advertise uniqueness and the
        // WinMagic rewrite must be suppressed.
        createTable("CREATE TABLE fact_rmd (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        // MOR table: UNIQUE_KEYS with enable_unique_key_merge_on_write = false.
        // Key columns must be declared first in the schema.
        createTable("CREATE TABLE dim_rmd (\n"
                + "  k INT NOT NULL,\n"
                + "  did INT,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', "
                + "'enable_unique_key_merge_on_write' = 'false')");

        String saved = connectContext.getSessionVariable().readMorAsDupTables;
        connectContext.getSessionVariable().readMorAsDupTables = "*";
        try {
            String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                    + "FROM fact_rmd f, dim_rmd d "
                    + "WHERE f.k = d.k "
                    + "  AND f.v * 2 > ("
                    + "    SELECT SUM(f2.v) "
                    + "    FROM fact_rmd f2 "
                    + "    WHERE f2.k = d.k"
                    + "  )";

            Plan plan = PlanChecker.from(createCascadesContext(sql))
                    .analyze(sql)
                    .applyBottomUp(new PullUpProjectUnderApply())
                    .applyTopDown(new PushDownFilterThroughProject())
                    .customRewrite(new EliminateUnnecessaryProject())
                    .customRewrite(new AggScalarSubQueryToWindowFunction())
                    .getPlan();

            Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                    "readMorAsDup reads MOR table as DUPLICATE — "
                    + "uniqueness is not guaranteed, rewrite must be rejected");
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = saved;
        }
    }

    @Test
    public void testIncrementalStreamScanRejected() throws Exception {
        // An incremental scan (LogicalOlapTableStreamScan(INCREMENTAL) or
        // TableScanParams.INCREMENTAL_READ) can return multiple row versions
        // with the same key.  WinMagic runs before stream normalization, so
        // the window rewrite would group those duplicate-key rows under a
        // single PARTITION BY and multiply the aggregate — producing wrong
        // results.
        //
        // isDuplicateProducingScanMode() must suppress all uniqueness sources
        // for such scans:
        //   1. LogicalOlapTableStreamScan with isIncremental() == true
        //   2. Regular LogicalOlapScan with scanParams.incrementalRead() == true
        //
        // This test covers path 2 by attaching an INCREMENTAL_READ
        // TableScanParams to the correlated table's scan node.
        createTable("CREATE TABLE fact_incr (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_incr (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_incr add constraint uq_dim_incr_k unique (k)");

        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_incr f, dim_incr d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_incr f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan preRule = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .getPlan();

        // Precondition: the rule would normally match.
        Plan normalResult = new AggScalarSubQueryToWindowFunction()
                .rewriteRoot(preRule, null);
        Assertions.assertTrue(normalResult.anyMatch(LogicalWindow.class::isInstance),
                "With unique constraint the rule should normally produce a window");

        // Replace the correlated table's LogicalOlapScan with one that has
        // TableScanParams.INCREMENTAL_READ so isDuplicateProducingScanMode()
        // returns true and uniqueness is suppressed.
        org.apache.doris.analysis.TableScanParams incrParams =
                new org.apache.doris.analysis.TableScanParams(
                        org.apache.doris.analysis.TableScanParams.INCREMENTAL_READ,
                        null, null);
        Plan incrPlan = preRule.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalOlapScan(LogicalOlapScan scan, Object ctx) {
                if (scan.getTable().getName().equals("dim_incr")) {
                    return scan.withTableScanParams(incrParams);
                }
                return scan;
            }
        }, null);

        Plan plan = new AggScalarSubQueryToWindowFunction()
                .rewriteRoot(incrPlan, null);

        // Rule must NOT match — incremental scan exposes duplicate key
        // versions, so uniqueness is not guaranteed.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Incremental scan must be rejected because it can "
                + "return duplicate key rows that would multiply the "
                + "window aggregate");
    }

    @Test
    public void testMismatchedScanDomainsRejected() throws Exception {
        // A shared table scan with different row-domain semantics
        // (e.g. @incr on the outer fact scan vs. a normal base-table
        // inner fact2 scan) must be rejected.  checkRelation() pairs
        // scans by Table.getId(), but a wrapper/scan-param mismatch
        // means the two scans read different rows — the window would
        // compute over a different row set than the original scalar
        // subquery.
        createTable("CREATE TABLE fact_msd (\n"
                + "  id INT,\n"
                + "  k INT,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE dim_msd (\n"
                + "  did INT,\n"
                + "  k INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(did)\n"
                + "DISTRIBUTED BY HASH(did) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_msd add constraint uq_dim_msd_k unique (k)");

        String sql = "SELECT d.did, d.k, d.tag, f.id, f.v "
                + "FROM fact_msd f, dim_msd d "
                + "WHERE f.k = d.k "
                + "  AND f.v * 2 > ("
                + "    SELECT SUM(f2.v) "
                + "    FROM fact_msd f2 "
                + "    WHERE f2.k = d.k"
                + "  )";

        Plan preRule = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .getPlan();

        // Precondition: the rule would normally match.
        Plan normalResult = new AggScalarSubQueryToWindowFunction()
                .rewriteRoot(preRule, null);
        Assertions.assertTrue(normalResult.anyMatch(LogicalWindow.class::isInstance),
                "With unique constraint the rule should normally produce a window");

        // Attach INCREMENTAL_READ scan params ONLY to the outer shared-table
        // scan (fact f), not the inner scan (fact f2).  The DefaultPlanRewriter
        // visits left child before right child, so the first fact_msd scan
        // encountered is the outer one.
        final boolean[] modified = {false};
        org.apache.doris.analysis.TableScanParams incrParams =
                new org.apache.doris.analysis.TableScanParams(
                        org.apache.doris.analysis.TableScanParams.INCREMENTAL_READ,
                        null, null);
        Plan incrPlan = preRule.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalOlapScan(LogicalOlapScan scan, Object ctx) {
                if (!modified[0] && scan.getTable().getName().equals("fact_msd")) {
                    modified[0] = true;
                    return scan.withTableScanParams(incrParams);
                }
                return scan;
            }
        }, null);

        Plan plan = new AggScalarSubQueryToWindowFunction()
                .rewriteRoot(incrPlan, null);

        // Rule must NOT match — the outer and inner shared-table scans
        // have different row domains.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "Mismatched scan domains on shared table must be rejected: "
                + "the outer scan has @incr params but the inner scan "
                + "reads the full base table");
    }

    @Test
    public void testTvfRelationRejected() throws Exception {
        // TVF (table-valued function) relations like numbers() are
        // LogicalTVFRelation, not CatalogRelation.  checkRelation() and
        // isSameScanDomain() only inspect CatalogRelation, so TVF relations
        // are invisible to the relation accounting.  The rewrite would
        // incorrectly substitute the outer TVF rows for the inner TVF rows
        // when they produce different row counts (e.g. numbers(2) vs 3).
        // checkPlanType() must reject non-catalog relations.
        createTable("CREATE TABLE dim_tvf (\n"
                + "  id INT NOT NULL,\n"
                + "  tag INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        addConstraint("alter table dim_tvf add constraint uq_dim_tvf_id unique (id)");

        // Outer: numbers("number"="2") produces 2 rows per dim row.
        // Inner: numbers("number"="3") produces 3 rows.
        // Post-rewrite COUNT(*) OVER (PARTITION BY d.id) sees only 2 rows.
        String sql = "SELECT d.id, n.number "
                + "FROM numbers(\"number\"=\"2\") n, dim_tvf d "
                + "WHERE d.id > 0 "
                + "  AND 3 = ("
                + "    SELECT COUNT(*) "
                + "    FROM numbers(\"number\"=\"3\") n2 "
                + "    WHERE d.id > 0"
                + "  )";

        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        // Rule must NOT match — TVF relations are not CatalogRelation
        // and cannot be proven row-domain equivalent.
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance),
                "TVF relations must be rejected because they are not "
                + "CatalogRelation and their row-domain equivalence "
                + "cannot be proven");
    }

    private void check(String sql) {
        System.out.printf("Test:\n%s\n\n", sql);
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance));
    }

    private void checkNot(String sql) {
        System.out.printf("Test:\n%s\n\n", sql);
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance));
    }
}
