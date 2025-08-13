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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DummyPlan implements Plan {
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
        return null;
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        return null;
    }

    @Override
    public boolean canBind() {
        return false;
    }

    @Override
    public List<Slot> getOutput() {
        return null;
    }

    @Override
    public Set<Slot> getOutputSet() {
        return null;
    }

    @Override
    public String treeString(boolean printStates) {
        return "";
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return null;
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return null;
    }

    @Override
    public void computeUnique(Builder builder) {

    }

    @Override
    public void computeUniform(Builder builder) {

    }

    @Override
    public void computeEqualSet(Builder builder) {

    }

    @Override
    public void computeFd(Builder builder) {

    }

    @Override
    public List<Plan> children() {
        return ImmutableList.of();
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
    public <T> Optional<T> getMutableState(String key) {
        return Optional.empty();
    }

    @Override
    public void setMutableState(String key, Object value) {

    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return null;
    }
}
