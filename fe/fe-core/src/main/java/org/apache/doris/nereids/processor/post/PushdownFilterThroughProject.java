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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

/**
 * merge consecutive projects
 */
public class PushdownFilterThroughProject extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        PhysicalProperties properties = filter.getPhysicalProperties();
        Statistics stats = filter.getStats();
        Plan child = filter.child();
        if (!(child instanceof PhysicalProject)) {
            PhysicalPlan newChild = (PhysicalPlan) child.accept(this, context);
            if (child != newChild) {
                newChild = newChild.withPhysicalPropertiesAndStats(
                        ((PhysicalPlan) child).getPhysicalProperties(),
                        ((AbstractPlan) child).getStats());
                return ((PhysicalPlan) (filter.withChildren(newChild)))
                        .withPhysicalPropertiesAndStats(properties, stats);
            }
            return filter;
        }

        PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) child;
        PhysicalFilter<? extends Plan> newFilter = filter.withConjunctsAndChild(
                ExpressionUtils.replace(filter.getConjuncts(), project.getAliasToProducer()),
                project.child()).withPhysicalPropertiesAndStats(properties, stats);
        PhysicalPlan newChild = (PhysicalPlan) newFilter.accept(this, context);
        newChild = newChild.withPhysicalPropertiesAndStats(properties, stats);
        return ((PhysicalPlan) (project.withChildren(newChild)))
                .withPhysicalPropertiesAndStats(properties, stats);
    }
}
