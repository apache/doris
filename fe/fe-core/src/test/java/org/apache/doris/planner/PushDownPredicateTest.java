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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushDownPredicateTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl1\n" + "(k1 int, k2 int, v1 int)\n" + "distributed by hash(k1)\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    @Test
    public void testPushDownPredicateOnGroupingSetAggregate() throws Exception {
        String sql = "explain select k1, k2, count(distinct v1) from test.tbl1"
                + " group by grouping sets((k1), (k1, k2)) having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        String plan = planner.getExplainString(fragments, new ExplainOptions(false, false));
        Assertions.assertTrue(plan.contains("PREDICATES: `k1` = 1\n"));
    }

    @Test
    public void testPushDownPredicateOnRollupAggregate() throws Exception {
        String sql = "explain select k1, k2, count(distinct v1) from test.tbl1"
                + " group by rollup(k1, k2) having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        String plan = planner.getExplainString(fragments, new ExplainOptions(false, false));
        Assertions.assertFalse(plan.contains("PREDICATES:"));
    }

    @Test
    public void testPushDownPredicateOnNormalAggregate() throws Exception {
        String sql = "explain select k1, k2, count(distinct v1) from test.tbl1"
                + " group by k1, k2 having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        String plan = planner.getExplainString(fragments, new ExplainOptions(false, false));
        Assertions.assertTrue(plan.contains("PREDICATES: `k1` = 1, `k2` = 1\n"));
    }

    @Test
    public void testPushDownPredicateOnWindowFunction() throws Exception {
        String sql = "explain select v1, k1,"
                + " sum(v1) over (partition by k1 order by v1 rows between 1 preceding and 1 following)"
                + " as 'moving total' from test.tbl1 where k1 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        String plan = planner.getExplainString(fragments, new ExplainOptions(false, false));
        Assertions.assertTrue(plan.contains("PREDICATES: `k1` = 1\n"));
    }
}
