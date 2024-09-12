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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * select hint plan.
 * e.g. LogicalSelectHint (set_var(query_timeout='1800', exec_mem_limit='2147483648'))
 */
public class LogicalSelectHint<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements BlockFuncDepsPropagation {

    private final ImmutableList<SelectHint> hints;

    public LogicalSelectHint(ImmutableList<SelectHint> hints, CHILD_TYPE child) {
        this(hints, Optional.empty(), Optional.empty(), child);
    }

    /**
     * LogicalSelectHint's full parameter constructor.
     * @param hints hint maps, key is hint name, e.g. 'SET_VAR', and value is parameter pairs, e.g. query_time=100
     * @param groupExpression groupExpression exists when this plan is copy out from memo.
     * @param logicalProperties logicalProperties is use for compute output
     * @param child child plan
     */
    public LogicalSelectHint(ImmutableList<SelectHint> hints,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SELECT_HINT, groupExpression, logicalProperties, child);
        this.hints = ImmutableList.copyOf(Objects.requireNonNull(hints, "hints can not be null"));
    }

    public List<SelectHint> getHints() {
        return hints;
    }

    /**
     * check if current select hint include some hint
     * @param hintName hint name
     * @return boolean which indicate have hint
     */
    public boolean isIncludeHint(String hintName) {
        for (SelectHint hint : hints) {
            if (hint.getHintName().equalsIgnoreCase(hintName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public LogicalSelectHint<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSelectHint<>(hints, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSelectHint(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public LogicalSelectHint<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSelectHint<>(hints, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSelectHint<>(hints, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        String hintStr = this.hints
                .stream()
                .map(hint -> hint.toString())
                .collect(Collectors.joining(", "));
        return "LogicalSelectHint (" + hintStr + ")";
    }
}
