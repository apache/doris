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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class IntersectReorderTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void test() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .intersect(ImmutableList.of(scan2, scan1))
                .build();

        CascadesContext ctx = MemoTestUtils.createCascadesContext(plan);
        for (Group group : ctx.getMemo().getGroups()) {
            if (group.getLogicalExpression().getPlan() instanceof LogicalOlapScan) {
                LogicalOlapScan scan = (LogicalOlapScan) group.getLogicalExpression().getPlan();
                if (scan.equals(scan1)) {
                    group.setStatistics(new Statistics(1, new HashMap<>()));
                }
                if (scan.equals(scan2)) {
                    group.setStatistics(new Statistics(2, new HashMap<>()));
                }
            }
        }
        PlanChecker.from(ctx)
                .applyExploration(IntersectReorder.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalIntersect(
                                logicalOlapScan().when(scan -> scan.equals(scan1)),
                                logicalOlapScan().when(scan -> scan.equals(scan2))
                        )
                );
    }
}
