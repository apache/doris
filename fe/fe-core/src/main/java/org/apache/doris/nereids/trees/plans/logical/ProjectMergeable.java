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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ProjectMergeable: merge top projection to current
 *
 * for example:
 * 1. project(oneRowRelation) -> oneRowRelation
 * 2. project(project) -> project
 * 3. project(emptyRelation) -> emptyRelation
 */
public interface ProjectMergeable extends ProjectProcessor, OutputPrunable, Plan {
    @Override
    default Optional<Plan> processProject(List<NamedExpression> parentProjects) {
        return mergeContinuedProjects(parentProjects, this);
    }

    /** merge project until can not merge */
    static Optional<Plan> mergeContinuedProjects(List<NamedExpression> parentProject, Plan plan) {
        if (!(plan instanceof ProjectMergeable)
                || !((ProjectMergeable) plan).canProcessProject(parentProject)) {
            return Optional.empty();
        }
        List<NamedExpression> mergedProjects = parentProject;
        ProjectMergeable child = (ProjectMergeable) plan;
        while (true) {
            mergedProjects = new ArrayList<>(PlanUtils.mergeProjections(child.getProjects(), mergedProjects));
            for (NamedExpression expression : child.getProjects()) {
                // keep NoneMovableFunction for later use
                if (expression.containsType(NoneMovableFunction.class)) {
                    mergedProjects.add(expression);
                }
            }
            if (child.arity() == 1 && child.child(0) instanceof ProjectMergeable
                    && ((ProjectMergeable) child.child(0)).canProcessProject(mergedProjects)) {
                child = (ProjectMergeable) child.child(0);
                continue;
            }
            return Optional.of(child.withProjects(mergedProjects));
        }
    }

    List<NamedExpression> getProjects();

    Plan withProjects(List<NamedExpression> projects);
}
