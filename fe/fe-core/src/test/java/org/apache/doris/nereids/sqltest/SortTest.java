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

import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SortTest extends SqlTestBase {
    @Test
    public void testTwoPhaseSort() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from\n"
                + "(select score from T1 order by id) as t order by score\n";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan.anyMatch(e -> e instanceof PhysicalQuickSort
                && ((PhysicalQuickSort<?>) e).getSortPhase().isMerge() && e.child(0) instanceof PhysicalDistribute));
    }
}
