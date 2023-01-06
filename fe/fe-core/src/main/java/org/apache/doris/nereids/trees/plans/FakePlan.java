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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Used for unit test only.
 */
public class FakePlan implements Plan {

    @Override
    public List<Plan> children() {
        return null;
    }

    @Override
    public Plan child(int index) {
        return null;
    }

    @Override
    public int arity() {
        return 0;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return null;
    }

    @Override
    public PlanType getType() {
        return null;
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ArrayList<>();
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        return new LogicalProperties(ArrayList::new);
    }

    @Override
    public boolean canBind() {
        return false;
    }

    @Override
    public List<Slot> getOutput() {
        return new ArrayList<>();
    }

    @Override
    public List<Slot> getNonUserVisibleOutput() {
        return ImmutableList.of();
    }

    @Override
    public String treeString() {
        return "DUMMY";
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return this;
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return this;
    }
}
