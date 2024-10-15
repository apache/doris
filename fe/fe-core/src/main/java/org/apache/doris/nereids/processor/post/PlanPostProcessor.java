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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

/**
 * PlanPostprocessor: a PlanVisitor to rewrite PhysicalPlan to new PhysicalPlan.
 */
public class PlanPostProcessor extends DefaultPlanRewriter<CascadesContext> {
    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        AbstractPhysicalPlan newPlan = (AbstractPhysicalPlan) super.visit(plan, context);
        return newPlan == plan ? plan : newPlan.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) plan);
    }

    public Plan processRoot(Plan plan, CascadesContext ctx) {
        return plan.accept(this, ctx);
    }
}
