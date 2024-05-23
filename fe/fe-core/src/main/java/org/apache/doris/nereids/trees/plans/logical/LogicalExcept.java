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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.ExprFdItem;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Except.
 */
public class LogicalExcept extends LogicalSetOperation {

    public LogicalExcept(Qualifier qualifier, List<Plan> inputs) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, inputs);
    }

    public LogicalExcept(Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs, List<Plan> children) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, outputs, childrenOutputs, children);
    }

    public LogicalExcept(Qualifier qualifier, List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalExcept",
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExcept(this, context);
    }

    @Override
    public LogicalExcept withChildren(List<Plan> children) {
        return new LogicalExcept(qualifier, outputs, regularChildrenOutputs, children);
    }

    @Override
    public LogicalExcept withChildrenAndTheirOutputs(List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        Preconditions.checkArgument(children.size() == childrenOutputs.size(),
                "children size %s is not equals with children outputs size %s",
                children.size(), childrenOutputs.size());
        return new LogicalExcept(qualifier, outputs, childrenOutputs, children);
    }

    @Override
    public LogicalExcept withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalExcept(qualifier, outputs, regularChildrenOutputs, groupExpression,
                Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalExcept(qualifier, outputs, regularChildrenOutputs,
                groupExpression, logicalProperties, children);
    }

    @Override
    public LogicalExcept withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalExcept(qualifier, newOutputs, regularChildrenOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        Set<NamedExpression> output = ImmutableSet.copyOf(getOutput());
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<SlotReference> exprs = output.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .collect(ImmutableSet.toImmutableSet());

        if (qualifier == Qualifier.DISTINCT) {
            ExprFdItem fdItem = FdFactory.INSTANCE.createExprFdItem(exprs, true, exprs);
            builder.add(fdItem);

            // only inherit from left side
            ImmutableSet<FdItem> leftFdItems = child(0).getLogicalProperties()
                    .getTrait().getFdItems();

            builder.addAll(leftFdItems);
        }

        return builder.build();
    }

    @Override
    public void computeUnique(Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
        if (qualifier == Qualifier.DISTINCT) {
            builder.addUniqueSlot(ImmutableSet.copyOf(getOutput()));
        }
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> output = getOutput();
        List<? extends Slot> originalOutputs = regularChildrenOutputs.isEmpty()
                ? child(0).getOutput()
                : regularChildrenOutputs.get(0);
        for (int i = 0; i < output.size(); i++) {
            replaceMap.put(originalOutputs.get(i), output.get(i));
        }
        builder.replace(replaceMap);
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> output = getOutput();
        List<? extends Slot> originalOutputs = regularChildrenOutputs.isEmpty()
                ? child(0).getOutput()
                : regularChildrenOutputs.get(0);
        for (int i = 0; i < output.size(); i++) {
            replaceMap.put(originalOutputs.get(i), output.get(i));
        }
        builder.replace(replaceMap);
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> output = getOutput();
        List<? extends Slot> originalOutputs = regularChildrenOutputs.isEmpty()
                ? child(0).getOutput()
                : regularChildrenOutputs.get(0);
        for (int i = 0; i < output.size(); i++) {
            replaceMap.put(originalOutputs.get(i), output.get(i));
        }
        builder.replace(replaceMap);
    }

    @Override
    public void computeUniform(Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
        Map<Slot, Slot> replaceMap = new HashMap<>();
        List<Slot> output = getOutput();
        List<? extends Slot> originalOutputs = regularChildrenOutputs.isEmpty()
                ? child(0).getOutput()
                : regularChildrenOutputs.get(0);
        for (int i = 0; i < output.size(); i++) {
            replaceMap.put(originalOutputs.get(i), output.get(i));
        }
        builder.replace(replaceMap);
    }
}
