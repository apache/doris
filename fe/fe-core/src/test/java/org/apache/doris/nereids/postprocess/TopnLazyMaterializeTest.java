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
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;

import org.junit.jupiter.api.Test;

public class TopnLazyMaterializeTest extends SSBTestBase {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setRuntimeFilterMode("Global");
        connectContext.getSessionVariable().setRuntimeFilterType(8);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void test() {
        String sql = "select lineorder.*, dates.* from dates, lineorder where d_datekey > 19980101 and lo_orderdate = d_datekey order by d_date limit 10;";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanFragment fragment = new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        // MaterializationNode materializationNode = (MaterializationNode) fragment.getPlanRoot();
        System.out.println(fragment);
    }
}
