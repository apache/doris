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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * All DDL and DML commands' super class.
 */
public abstract class Command extends AbstractPlan implements LogicalPlan, BlockFuncDepsPropagation {

    protected Command(PlanType type) {
        super(type, Optional.empty(), Optional.empty(), null, ImmutableList.of());
    }

    public abstract void run(ConnectContext ctx, StmtExecutor executor) throws Exception;

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        throw new RuntimeException("Command do not implement getGroupExpression");
    }

    @Override
    public List<Plan> children() {
        throw new RuntimeException("Command do not implement children");
    }

    @Override
    public Plan child(int index) {
        throw new RuntimeException("Command do not implement child");
    }

    @Override
    public int arity() {
        throw new RuntimeException("Command do not implement arity");
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        throw new RuntimeException("Command do not implement withChildren");
    }

    @Override
    public PlanType getType() {
        throw new RuntimeException("Command do not implement getType");
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new RuntimeException("Command do not implement getExpressions");
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        throw new RuntimeException("Command do not implement getLogicalProperties");
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        throw new RuntimeException("Command do not implement withGroupExprLogicalPropChildren");
    }

    @Override
    public boolean canBind() {
        throw new RuntimeException("Command do not implement canResolve");
    }

    @Override
    public List<Slot> getOutput() {
        throw new RuntimeException("Command do not implement getOutput");
    }

    @Override
    public Set<Slot> getOutputSet() {
        throw new RuntimeException("Command do not implement getOutputSet");
    }

    @Override
    public String treeString() {
        throw new RuntimeException("Command do not implement treeString");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new RuntimeException("Command do not implement withGroupExpression");
    }
}
