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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.nereids.processor.post.ColumnPruningPostProcessor;
import org.apache.doris.nereids.rules.rewrite.InferFilterNotNull;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ColumnPruningPostProcessorTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void test() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, ImmutableList.of())
                .project(ImmutableList.of(0, 2))
                .build();

        PhysicalPlan physicalPlan = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .implement()
                .getPhysicalPlan();

        ColumnPruningPostProcessor processor = new ColumnPruningPostProcessor();
        PhysicalPlan newPlan = (PhysicalPlan) physicalPlan.accept(processor, null);

        Assertions.assertTrue(newPlan instanceof PhysicalProject);
        Assertions.assertTrue(newPlan.child(0) instanceof PhysicalNestedLoopJoin);
        Assertions.assertTrue(newPlan.child(0).child(0) instanceof PhysicalProject);
        Assertions.assertTrue(newPlan.child(0).child(1) instanceof PhysicalProject);
    }
}
