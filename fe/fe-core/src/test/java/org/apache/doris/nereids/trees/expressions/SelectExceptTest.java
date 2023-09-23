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

class SelectExceptTest implements MemoPatternMatchSupported {
    @Test
    void testExcept() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(new UnboundStar(ImmutableList.of("db", "t1"))),
                ImmutableList.of(new UnboundSlot("db", "t1", "id")),
                false,
                olapScan);
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(project)
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(proj -> proj.getExcepts().size() == 1 && proj.getProjects().size() == 1)
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
                        ).when(project -> project.getExcepts().size() == 2
                                && project.getProjects().get(0) instanceof UnboundStar)
                ));

        String sql2 = "select k1, k2, v1, v2 except(v1, v2) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql2, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        ).when(project -> project.getExcepts().size() == 2
                                && project.getProjects().get(0) instanceof UnboundStar)
                )));

        String sql3 = "select * except(v1, v2)";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql3, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        ).when(project -> project.getExcepts().size() == 2
                                && project.getProjects().get(0) instanceof UnboundStar)
                )));

        String sql4 = "select * except() from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql4, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        ).when(project -> project.getExcepts().size() == 2
                                && project.getProjects().get(0) instanceof UnboundStar)
                )));

        String sql5 = "select * except(v1 + v2, v3 as k3) from t1";
        Assertions.assertThrows(ParseException.class, () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .checkParse(sql5, (checker) -> checker.matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        ).when(project -> project.getExcepts().size() == 2
                                && project.getProjects().get(0) instanceof UnboundStar)
                )));
    }
}
