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
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Optional;

/**
 * ProjectProcessor, this class is used to push project/ eliminate project,
 * e.g. LogicalProject(LogicalUnion(child1, child2)) -> LogicalUnion(logicalProject(child1), logicalProject(child2))
 */
public interface ProjectProcessor {
    boolean canProcessProject(List<NamedExpression> parentProjects);

    Optional<Plan> processProject(List<NamedExpression> parentProjects);

    /** tryProcessProject */
    static Optional<Plan> tryProcessProject(List<NamedExpression> parentProjects, Plan plan) {
        if (plan instanceof ProjectProcessor && ((ProjectProcessor) plan).canProcessProject(parentProjects)) {
            return ((ProjectProcessor) plan).processProject(parentProjects);
        } else {
            return Optional.empty();
        }
    }
}
