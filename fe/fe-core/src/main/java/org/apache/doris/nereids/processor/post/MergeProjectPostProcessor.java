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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * merge consecutive projects
 */
public class MergeProjectPostProcessor extends PlanPostProcessor {

    @Override
    public PhysicalProject visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        Plan child = project.child();
        Plan newChild = child.accept(this, ctx);
        if (newChild instanceof PhysicalProject) {
            List<NamedExpression> projections = project.mergeProjections((PhysicalProject) newChild);
            return (PhysicalProject) project
                    .withProjectionsAndChild(projections, newChild.child(0))
                    .copyStatsAndGroupIdFrom(project);
        }
        return child != newChild
                ? (PhysicalProject) project.withChildren(Lists.newArrayList(newChild)).copyStatsAndGroupIdFrom(project)
                : project;
    }
}
