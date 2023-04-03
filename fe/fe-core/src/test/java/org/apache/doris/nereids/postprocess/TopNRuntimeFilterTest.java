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

import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopNRuntimeFilterTest extends SSBTestBase {
    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
    }

    @Test
    public void testUseTopNRf() {
        String sql = "select * from customer order by c_custkey limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        Assertions.assertTrue(plan.children().get(0) instanceof PhysicalTopN);
        PhysicalTopN localTopN = (PhysicalTopN) plan.children().get(0);
        Assertions.assertTrue(localTopN.getMutableState(PhysicalTopN.TOPN_RUNTIME_FILTER).isPresent());
    }

    // topn rf do not apply on string-like and float column
    @Test
    public void testNotUseTopNRf() {
        String sql = "select * from customer order by c_name limit 5";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        Assertions.assertTrue(plan.children().get(0) instanceof PhysicalTopN);
        PhysicalTopN localTopN = (PhysicalTopN) plan.children().get(0);
        Assertions.assertFalse(localTopN.getMutableState(PhysicalTopN.TOPN_RUNTIME_FILTER).isPresent());
    }
}
