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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SelectReplaceTest extends AnalyzeCheckTestBase implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        String t0 = "create table t1("
                + "id int, \n"
                + "k1 int, \n"
                + "k2 int, \n"
                + "v1 int, \n"
                + "v2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');";

        String t1 = "create table t2("
                + "id int, \n"
                + "k1 int, \n"
                + "v2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');";

        createTables(t0, t1);
    }

    @Test
    void testReplace() {
        String sql1 = "select * replace (2 as t1k1, t2k1 * 2 as t2k1) from "
                + "(select 1, t1.k1 as t1k1, t2.k1 as t2k1 from t1 join t2 on t1.id = t2.id) t";
        PlanChecker.from(connectContext).checkPlannerResult(sql1);

        String sql2 = "select * replace (1 as k1) except (k2) from t1";
        PlanChecker.from(connectContext).checkPlannerResult(sql2, planner -> {
            Assertions.assertEquals(4, planner.getPhysicalPlan().getOutput().size());
        });

        // k1 in both except and replace
        String sql3 = "select * replace (1 as k1, 2 as k2) except (k1) from t1";
        Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql3));

        // duplicate replace column
        String sql4 = "select * replace (1 as k1, 2 as k1) from t1";
        Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql4));

        String sql5 = "select * replace (1 as k1) from t1, t1 t1_copy";
        Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql5));

        // Unknown column
        String sql6 = "select * replace (1 as fake) from t1";
        Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql6));

        // agg not support replace
        String sql7 = "select * replace (v2 + 1 as v2) from t2 group by id, k1, v2";
        Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql7));
    }

    @Test
    public void testReplace2() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(
                        new UnboundStar(ImmutableList.of("db", "t1"),
                                ImmutableList.of(),
                                ImmutableList.of(new UnboundAlias(new UnboundSlot("name"), "id"))
                        )), false, olapScan);
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(project)
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(proj -> proj.getProjects().get(0).getName().equals("id"))
                );
    }

    @Test
    public void testParse() {
        String sql1 = "select * replace('new' as v1) from t1";
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql1, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                ));

        String sql2 = "select * replace('new' as v1, abs(v1 + v2) * 3 as v2) from t1";
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql2, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                ));

        // need select *
        String sql3 = "seelct k1, k2, v1, v2 replace(k1 / 2 as k1) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql3, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                )));

        // no replace expression
        String sql4 = "select * replace() from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql4, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                )));

        // no from clause
        String sql5 = "select * replace('new' as v1)";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql5, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                )));

        // with except and replace
        String sql6 = "select * except (v1) replace('new' as v2) from t1";
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql6, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                ));
    }
}
