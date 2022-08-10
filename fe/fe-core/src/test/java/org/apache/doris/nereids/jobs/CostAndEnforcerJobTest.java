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

package org.apache.doris.nereids.jobs;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CostAndEnforcerJobTest {
    /*
     *   topJoin
     *   /     \
     *  C   bottomJoin
     *        /    \
     *       A      B
     */

    private static List<LogicalOlapScan> scans = Lists.newArrayList();
    private static List<List<SlotReference>> outputs = Lists.newArrayList();

    @BeforeAll
    public static void init() {
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "b", 1);
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "c", 0);

        scans.add(scan1);
        scans.add(scan2);
        scans.add(scan3);

        List<SlotReference> t1Output = scan1.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> t2Output = scan2.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> t3Output = scan3.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        outputs.add(t1Output);
        outputs.add(t2Output);
        outputs.add(t3Output);
    }

    @Test
    public void testExecute() {
        new MockUp<CostCalculator>() {
            @Mock
            public double calculateCost(GroupExpression groupExpression) {
                return 0;
            }
        };

        /*
         *   bottomJoin
         *    /    \
         *   A      B
         */
        Expression bottomJoinOnCondition = new EqualTo(outputs.get(0).get(0), outputs.get(1).get(0));

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> bottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                Optional.of(bottomJoinOnCondition), scans.get(0), scans.get(1));

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(bottomJoin);
        cascadesContext.pushJob(
                new OptimizeGroupJob(
                        cascadesContext.getMemo().getRoot(),
                        cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }
}
