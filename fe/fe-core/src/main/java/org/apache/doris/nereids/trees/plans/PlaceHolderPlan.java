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
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalLeafOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A virtual node that represents a fixed plan.
 * Used in {@link org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator},
 * as a place-holder when do match root.
 */
public class PlaceHolderPlan extends LogicalLeafPlan<PlaceHolderPlan.PlaceHolderOperator> {
    /** PlaceHolderOperator. */
    public static class PlaceHolderOperator extends LogicalLeafOperator {
        private final LogicalProperties logicalProperties;

        public PlaceHolderOperator(LogicalProperties logicalProperties) {
            super(OperatorType.PLACE_HOLDER);
            this.logicalProperties = Objects.requireNonNull(logicalProperties, "logicalProperties can not be null");
        }

        @Override
        public List<Slot> computeOutput() {
            throw new IllegalStateException("PlaceholderOperator can not compute output");
        }

        @Override
        public LogicalProperties computeLogicalProperties(Plan... inputs) {
            throw new IllegalStateException("PlaceholderOperator can not compute logical properties");
        }
    }

    public PlaceHolderPlan(LogicalProperties logicalProperties) {
        super(new PlaceHolderOperator(logicalProperties), Optional.empty(), Optional.of(logicalProperties));
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    @Override
    public NodeType getType() {
        return NodeType.FIXED;
    }

    @Override
    public PlaceHolderPlan withOutput(List<Slot> output) {
        throw new RuntimeException();
    }

    @Override
    public PlaceHolderPlan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 0);
        return new PlaceHolderPlan(logicalProperties);
    }
}
