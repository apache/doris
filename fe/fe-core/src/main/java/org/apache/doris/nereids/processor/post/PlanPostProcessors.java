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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TRuntimeFilterMode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;

/**
 * PlanPostprocessors: after copy out the plan from the memo, we use this rewriter to rewrite plan by visitor.
 */
public class PlanPostProcessors {
    private final CascadesContext cascadesContext;

    public PlanPostProcessors(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not be null");
    }

    /**
     * post process
     *
     * @param physicalPlan input plan
     * @return physical plan
     */
    public PhysicalPlan process(PhysicalPlan physicalPlan) {
        PhysicalPlan resultPlan = physicalPlan;
        for (PlanPostProcessor processor : getProcessors()) {
            resultPlan = (PhysicalPlan) processor.processRoot(resultPlan, cascadesContext);
        }
        return resultPlan;
    }

    /**
     * get processors
     */
    public List<PlanPostProcessor> getProcessors() {
        // add processor if we need
        Builder<PlanPostProcessor> builder = ImmutableList.builder();
        builder.add(new PushDownFilterThroughProject());
        builder.add(new RemoveUselessProjectPostProcessor());
        builder.add(new MergeProjectPostProcessor());
        builder.add(new RecomputeLogicalPropertiesProcessor());
        builder.add(new AddOffsetIntoDistribute());
        if (cascadesContext.getConnectContext().getSessionVariable().enableAggregateCse) {
            builder.add(new ProjectAggregateExpressionsForCse());
        }
        builder.add(new CommonSubExpressionOpt());
        // DO NOT replace PLAN NODE from here
        if (cascadesContext.getConnectContext().getSessionVariable().pushTopnToAgg) {
            builder.add(new PushTopnToAgg());
        }
        builder.add(new TopNScanOpt());
        builder.add(new FragmentProcessor());
        if (!cascadesContext.getConnectContext().getSessionVariable().getRuntimeFilterMode()
                        .toUpperCase().equals(TRuntimeFilterMode.OFF.name())) {
            builder.add(new RegisterParent());
            builder.add(new RuntimeFilterGenerator());
            if (ConnectContext.get().getSessionVariable().enableRuntimeFilterPrune) {
                builder.add(new RuntimeFilterPruner());
                if (ConnectContext.get().getSessionVariable().runtimeFilterPruneForExternal) {
                    builder.add(new RuntimeFilterPrunerForExternalTable());
                }
            }
        }
        builder.add(new Validator());
        return builder.build();
    }
}
