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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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

    private void check(String sql) {
        System.out.printf("Test:\n%s\n\n", sql);
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyTopDown(new PushDownFilterThroughProject())
                .customRewrite(new EliminateUnnecessaryProject())
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .rewrite()
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance));
    }

    private void checkNot(String sql) {
        System.out.printf("Test:\n%s\n\n", sql);
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .customRewrite(new AggScalarSubQueryToWindowFunction())
                .rewrite()
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance));
    }
}
