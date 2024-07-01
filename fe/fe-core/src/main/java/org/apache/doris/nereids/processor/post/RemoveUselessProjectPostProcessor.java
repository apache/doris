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
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Prune column for Join-Cluster
 */
@DependsRules({
        MergeProjectPostProcessor.class
})
public class RemoveUselessProjectPostProcessor extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project = (PhysicalProject<? extends Plan>) super.visit(project, ctx);
        Plan child = project.child();
        if (project.isAllSlots()) {
            Set<Slot> projects = project.getProjects().stream().map(Slot.class::cast).collect(Collectors.toSet());
            Set<Slot> outputSet = child.getOutputSet();
            if (outputSet.equals(projects)) {
                return child;
            }
        }
        return project;
    }
}

