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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Logical project plan node.
 */
public class LogicalProject extends LogicalUnary {
    private final List<? extends NamedExpression> projects;

    /**
     * Constructor for LogicalProjectPlan.
     *
     * @param projects project list
     * @param child child plan node
     */
    public LogicalProject(List<? extends NamedExpression> projects, LogicalPlan child) {
        super(NodeType.LOGICAL_PROJECT, child);
        this.projects = projects;
        updateOutput();
    }

    /**
     * Get project list.
     *
     * @return all project of this node.
     */
    public List<? extends NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public List<Slot> getOutput() {
        return output;
    }

    private void updateOutput() {
        output = Lists.newArrayListWithCapacity(projects.size());
        for (NamedExpression projection : projects) {
            try {
                output.add(projection.toAttribute());
            } catch (UnboundException e) {
                output.clear();
                break;
            }
        }
    }

    @Override
    public String toString() {
        return "Project (" + StringUtils.join(projects, ", ") + ")";
    }
}
