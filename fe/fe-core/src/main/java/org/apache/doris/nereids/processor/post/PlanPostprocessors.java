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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * PlanPostprocessors: after copy out the plan from the memo, we use this rewriter to rewrite plan by visitor.
 */
public class PlanPostprocessors {
    private final CascadesContext cascadesContext;

    public PlanPostprocessors(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not be null");
    }

    public PhysicalPlan process(PhysicalPlan physicalPlan) {
        PhysicalPlan resultPlan = physicalPlan;
        for (PlanPostprocessor processor : getProcessors()) {
            resultPlan = (PhysicalPlan) physicalPlan.accept(processor, cascadesContext);
        }
        return resultPlan;
    }

    public List<PlanPostprocessor> getProcessors() {
        // add processor if we need
        return ImmutableList.of();
    }
}
