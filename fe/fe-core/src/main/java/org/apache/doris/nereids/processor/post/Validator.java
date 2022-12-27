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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * validator plan.
 */
public class Validator extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        Plan child = project.child();
        // Forbidden project-project, we must merge project.
        Preconditions.checkArgument(!(child instanceof PhysicalProject));

        // TODO: Check projects is from child output.
        // List<NamedExpression> projects = project.getProjects();
        // Set<Slot> childOutputSet = child.getOutputSet();

        child.accept(this, context);
        return project;
    }

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        Preconditions.checkArgument(!filter.getConjuncts().isEmpty()
                && filter.getPredicate() != BooleanLiteral.TRUE);

        Plan child = filter.child();
        // Forbidden filter-project, we must make filter-project -> project-filter.
        Preconditions.checkState(!(child instanceof PhysicalProject));
        // Forbidden filter-cross join, because we put all filter on cross join into its other join condition.
        Preconditions.checkState(!(child instanceof PhysicalNestedLoopJoin));

        // Check filter is from child output.
        Set<Slot> childOutputSet = child.getOutputSet();
        Set<Slot> slotsUsedByFilter = filter.getConjuncts().stream()
                .<Set<Slot>>map(expr -> expr.collect(Slot.class::isInstance))
                .flatMap(Collection::stream).collect(Collectors.toSet());
        for (Slot slot : slotsUsedByFilter) {
            Preconditions.checkState(childOutputSet.contains(slot));
        }

        child.accept(this, context);
        return filter;
    }
}
