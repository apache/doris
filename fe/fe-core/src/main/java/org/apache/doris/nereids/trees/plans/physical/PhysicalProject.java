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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Physical project plan node.
 */
public class PhysicalProject<CHILD_TYPE extends Plan<CHILD_TYPE>>
        extends PhysicalUnary<PhysicalProject<CHILD_TYPE>, CHILD_TYPE> {

    private final List<? extends NamedExpression> projects;

    public PhysicalProject(List<? extends NamedExpression> projects, CHILD_TYPE child) {
        super(NodeType.PHYSICAL_PROJECT, child);
        this.projects = projects;
    }

    public List<? extends NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public String toString() {
        return "Project (" + StringUtils.join(projects, ", ") + ")";
    }
}
