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

package org.apache.doris.nereids.rules.rewrite.logical;

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
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem);",
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem, part\n"
                        + "            where l_partkey = p_partkey);",
                "select sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "    from lineitem, part\n"
                        + "    where p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#23'\n"
                        + "        and p_container = 'MED BOX'\n"
                        + "        and l_quantity < (\n"
                        + "            select 0.2 * avg(l_quantity)\n"
                        + "            from lineitem, partsupp\n"
                        + "            where l_partkey = p_partkey);",
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
                        + "            and l_partkey = 10);"
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
                .applyTopDown(new AggScalarSubQueryToWindowFunction())
                .rewrite()
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan.anyMatch(LogicalWindow.class::isInstance));
    }

    private void checkNot(String sql) {
        System.out.printf("Test:\n%s\n\n", sql);
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyTopDown(new AggScalarSubQueryToWindowFunction())
                .rewrite()
                .getPlan();
        System.out.println(plan.treeString());
        Assertions.assertFalse(plan.anyMatch(LogicalWindow.class::isInstance));
    }
}
