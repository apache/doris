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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.Map;
import java.util.Objects;

/**
 * merge consecutive projects
 */
public class PushDownFilterThroughProject extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter = (PhysicalFilter<? extends Plan>) super.visit(filter, context);
        Plan child = filter.child();
        // don't push down filter if child project contains NoneMovableFunction
        if (!(child instanceof PhysicalProject) || ((PhysicalProject) child).containsNoneMovableFunction()) {
            return filter;
        }

        PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) child;
        Map<Slot, Expression> childAlias = project.getAliasToProducer();
        if (filter.getInputSlots().stream().map(childAlias::get).filter(Objects::nonNull)
                .anyMatch(Expression::containsNonfoldable)) {
            return filter;
        }
        PhysicalFilter<? extends Plan> newFilter = filter.withConjunctsAndChild(
                ExpressionUtils.replace(filter.getConjuncts(), childAlias), project.child());
        return ((AbstractPhysicalPlan) project.withChildren(newFilter.accept(this, context)))
                .copyStatsAndGroupIdFrom(project);
    }
}
