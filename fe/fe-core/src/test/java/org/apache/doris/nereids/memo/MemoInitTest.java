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

package org.apache.doris.nereids.memo;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Objects;

public class MemoInitTest implements PatternMatchSupported {
    private ConnectContext connectContext = MemoTestUtils.createConnectContext();

    @Test
    public void initByOneLevelPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(table);

        PlanChecker.from(connectContext, scan)
                .checkGroupNum(1)
                .matches(
                    logicalOlapScan().when(scan::equals)
                );
    }

    @Test
    public void initByTwoLevelChainPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(table);

        LogicalProject<LogicalOlapScan> topProject = new LogicalProject<>(
                ImmutableList.of(scan.computeOutput().get(0)), scan);

        PlanChecker.from(connectContext, topProject)
                .checkGroupNum(2)
                .matches(
                        logicalProject(
                                any().when(child -> Objects.equals(child, scan))
                        ).when(root -> Objects.equals(root, topProject))
                );
    }

    @Test
    public void initByJoinSameUnboundTable() {
        UnboundRelation scanA = new UnboundRelation(ImmutableList.of("a"));

        LogicalJoin<UnboundRelation, UnboundRelation> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanA);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanA))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByJoinSameLogicalTable() {
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(tableA);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanA);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanA))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByTwoLevelJoinPlan() {
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        OlapTable tableB = PlanConstructor.newOlapTable(0, "b", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(tableA);
        LogicalOlapScan scanB = new LogicalOlapScan(tableB);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanB);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanB))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByThreeLevelChainPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(table);

        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(scan.computeOutput().get(0)), scan);
        LogicalFilter<LogicalProject<LogicalOlapScan>> filter = new LogicalFilter<>(
                new EqualTo(scan.computeOutput().get(0), new IntegerLiteral(1)), project);

        PlanChecker.from(connectContext, filter)
                .checkGroupNum(3)
                .matches(
                        logicalFilter(
                            logicalProject(
                                    any().when(child -> Objects.equals(child, scan))
                            ).when(root -> Objects.equals(root, project))
                        ).when(root -> Objects.equals(root, filter))
                );
    }

    @Test
    public void initByThreeLevelBushyPlan() {
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        OlapTable tableB = PlanConstructor.newOlapTable(0, "b", 1);
        OlapTable tableC = PlanConstructor.newOlapTable(0, "c", 1);
        OlapTable tableD = PlanConstructor.newOlapTable(0, "d", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(tableA);
        LogicalOlapScan scanB = new LogicalOlapScan(tableB);
        LogicalOlapScan scanC = new LogicalOlapScan(tableC);
        LogicalOlapScan scanD = new LogicalOlapScan(tableD);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> leftJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, scanA, scanB);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> rightJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, scanC, scanD);
        LogicalJoin topJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, leftJoin, rightJoin);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(7)
                .matches(
                        logicalJoin(
                                logicalJoin(
                                        any().when(child -> Objects.equals(child, scanA)),
                                        any().when(child -> Objects.equals(child, scanB))
                                ).when(left -> Objects.equals(left, leftJoin)),

                                logicalJoin(
                                        any().when(child -> Objects.equals(child, scanC)),
                                        any().when(child -> Objects.equals(child, scanD))
                                ).when(right -> Objects.equals(right, rightJoin))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }
}
