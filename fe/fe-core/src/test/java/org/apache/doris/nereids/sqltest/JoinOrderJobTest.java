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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JoinOrderJobTest extends SqlTestBase {
    @Test
    protected void testSimpleSQL() {
        String sql = "select * from T1, T2, T3, T4 "
                + "where "
                + "T1.id = T2.id and "
                + "T2.score = T3.score and "
                + "T3.id = T4.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .dpHypOptimize();
    }

    @Test
    protected void testSimpleSQLWithProject() {
        String sql = "select T1.id from T1, T2, T3, T4 "
                + "where "
                + "T1.id = T2.id and "
                + "T2.score = T3.score and "
                + "T3.id = T4.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .dpHypOptimize();
    }

    @Test
    protected void testComplexProject() {
        String sql = "select count(*) \n"
                + "from \n"
                + "T1, \n"
                + "(\n"
                + "select (T2.score + T3.score) as score from T2 join T3 on T2.id = T3.id"
                + ") subTable, \n"
                + "( \n"
                + "select (T4.id*2) as id from T4"
                + ") doubleT4 \n"
                + "where \n"
                + "T1.id = doubleT4.id and \n"
                + "T1.score = subTable.score;\n";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .dpHypOptimize();
    }

    @Test
    protected void testConstantComplex() {
        String sql = "select count(*) \n"
                + "from \n"
                + "T1 \n"
                + " join (\n"
                + "select (1) from T2"
                + ") subTable; \n";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .dpHypOptimize();
    }

    @Test
    protected void testConstantJoin() {
        String sql = "select count(*) \n"
                + "from \n"
                + "T1 \n"
                + " join (\n"
                + "select * , now() as t from T2 \n"
                + ") subTable on T1.id = t; \n";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .dpHypOptimize();
    }

    @Test
    protected void testCountJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select count(*) \n"
                + "from \n"
                + "T1, \n"
                + "(\n"
                + "select sum(T2.score + T3.score) as score from T2 join T3 on T2.id = T3.id"
                + ") subTable, \n"
                + "( \n"
                + "select sum(T4.id*2) as id from T4"
                + ") doubleT4 \n"
                + "where \n"
                + "T1.id = doubleT4.id and \n"
                + "T1.score = subTable.score;\n";
        Memo memo = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getCascadesContext()
                .getMemo();
        Assertions.assertEquals(memo.countMaxContinuousJoin(), 2);
    }

    @Test
    protected void test64TableJoin() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        Plan plan = hyperGraphBuilder
                .randomBuildPlanWith(65, 65);
        plan = new LogicalProject(plan.getOutput(), plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        Assertions.assertEquals(cascadesContext.getMemo().countMaxContinuousJoin(), 64);
        hyperGraphBuilder.initStats("test", cascadesContext);
        PlanChecker.from(cascadesContext)
                .optimize()
                .getBestPlanTree();
    }

    @Disabled
    @Test
    void test64CliqueJoin() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder(Sets.newHashSet(JoinType.INNER_JOIN));
        Plan plan = hyperGraphBuilder
                .randomBuildPlanWith(64, 64 * 63 / 2);
        plan = new LogicalProject(plan.getOutput(), plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        hyperGraphBuilder.initStats("test", cascadesContext);
        PlanChecker.from(cascadesContext)
                .rewrite()
                .dpHypOptimize()
                .getBestPlanTree();
    }
}
