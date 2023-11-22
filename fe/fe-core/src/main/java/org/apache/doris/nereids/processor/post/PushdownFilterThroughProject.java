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
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

/**
 * merge consecutive projects
 */
public class PushdownFilterThroughProject extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        Plan child = filter.child();
        if (!(child instanceof PhysicalProject)) {
            Plan newChild = child.accept(this, context);
            if (newChild == child) {
                return filter;
            } else {
                return ((AbstractPhysicalPlan) filter.withChildren(child.accept(this, context)))
                        .copyStatsAndGroupIdFrom(filter);
            }
        }

        PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) child;
        PhysicalFilter<? extends Plan> newFilter = filter.withConjunctsAndChild(
                ExpressionUtils.replace(filter.getConjuncts(), project.getAliasToProducer()),
                project.child());
        return ((PhysicalProject) project.withChildren(newFilter.accept(this, context)))
                .copyStatsAndGroupIdFrom(project);
    }
}
