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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SelectExceptTest extends AnalyzeCheckTestBase implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        String t = "create table t1("
                + "id int, \n"
                + "value int)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');";
        createTables(t);
    }

    @Test
    void testExcept() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(
                        new UnboundStar(ImmutableList.of("db", "t1"),
                                ImmutableList.of(new UnboundSlot("db", "t1", "id")),
                                ImmutableList.of()
                        )), false, olapScan);
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(project)
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(proj -> proj.getProjects().size() == 1 && proj.getProjects().get(0).getName().equals("name"))
                );
    }

    @Test
    void testParse() {
        String sql1 = "select * except(v1, v2) from t1";
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql1, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        ).when(project -> project.getProjects().size() == 1
                                && project.getProjects().get(0) instanceof UnboundStar
                                && ((UnboundStar) project.getProjects().get(0)).getExceptedSlots().size() == 2)
                ));

        String sql2 = "select k1, k2, v1, v2 except(v1, v2) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .parse(sql2));

        String sql3 = "select * except(v1, v2)";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .parse(sql3));

        String sql4 = "select * except() from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .parse(sql4));

        String sql5 = "select * except(v1 + v2, v3 as k3) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .parse(sql5));

        String sql6 = "select * except(id name) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .parse(sql6));
    }

    @Test
    public void testExceptAnalyze() {
        PatternDescriptor expected = logicalAggregate(
                logicalProject(
                        logicalOlapScan()
                ).when(proj -> proj.getProjects().size() == 2)
        ).when(agg -> agg.getOutputExpressions().size() == 2
                && agg.getOutputExpression(0).getName().equals("id")
                && agg.getOutputExpression(1).getName().equals("value")
                && agg.getGroupByExpressions().size() == 1
        );
        String sql1 = "select * except(value), sum(value) as value from t1 group by id";
        PlanChecker.from(connectContext)
                .parse(sql1).analyze().matches(expected);

        String sql2 = "select * except(value), sum(value) as value from t1 group by 1";
        PlanChecker.from(connectContext)
                .parse(sql2).analyze().matches(expected);

        String sql3 = "select * except(id, value) from t1"; // All slots in * EXCEPT clause are excepted
        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext).parse(sql3).analyze());
    }
}
