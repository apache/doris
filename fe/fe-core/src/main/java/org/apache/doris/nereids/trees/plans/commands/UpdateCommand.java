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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.List;

/**
 * update command
 */
public class UpdateCommand extends Command implements ForwardWithSync {
    private List<Pair<List<String>, Expression>> assignments;
    private String tableName;
    private LogicalPlan logicalQuery;

    public UpdateCommand(String tableName, List<Pair<List<String>, Expression>> assignments, LogicalPlan logicalQuery) {
        super(PlanType.UPDATE_COMMAND);
        this.tableName = tableName;
        this.assignments = assignments;
        this.logicalQuery = logicalQuery;

    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUpdateCommand(this, context);
    }
}
