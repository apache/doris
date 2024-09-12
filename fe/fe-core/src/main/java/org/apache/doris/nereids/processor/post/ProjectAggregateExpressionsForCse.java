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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * create project under aggregate to enable CSE
 */
public class ProjectAggregateExpressionsForCse extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate, CascadesContext ctx) {
        aggregate = (PhysicalHashAggregate<? extends Plan>) super.visit(aggregate, ctx);

        // for multi-phases aggregate, only process the 1st phase aggregate
        if (aggregate.child() instanceof PhysicalDistribute || aggregate.child() instanceof Aggregate) {
            return aggregate;
        }

        // select sum(A+B), ...
        // "A+B" is a cse candidate
        // cseCandidates: A+B -> alias(A+B)
        Map<Expression, Alias> cseCandidates = new HashMap<>();
        Set<Slot> inputSlots = new HashSet<>();

        for (Expression expr : aggregate.getExpressions()) {
            getCseCandidatesFromAggregateFunction(expr, cseCandidates);
            inputSlots.addAll(expr.getInputSlots());
        }
        if (cseCandidates.isEmpty()) {
            // no opportunity to generate cse
            return aggregate;
        }

        if (aggregate.child() instanceof PhysicalProject) {
            List<NamedExpression> projections = ((PhysicalProject) aggregate.child()).getProjects();
            Map<Slot, Expression> replaceMap = new HashMap<>();
            for (NamedExpression ne : projections) {
                if (ne instanceof Alias) {
                    replaceMap.put(ne.toSlot(), ((Alias) ne).child());
                }
            }
            for (Expression key : cseCandidates.keySet()) {
                cseCandidates.put(key,
                        (Alias) ExpressionUtils.replace(cseCandidates.get(key), replaceMap));
            }
        }

        // select sum(A+B),...
        // slotMap: A+B -> alias(A+B) to slot#3
        // sum(A+B) is replaced by sum(slot#3)
        Map<Expression, Slot> slotMap = new HashMap<>();
        for (Expression key : cseCandidates.keySet()) {
            slotMap.put(key, cseCandidates.get(key).toSlot());
        }
        List<NamedExpression> aggOutputReplaced = new ArrayList<>();
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            aggOutputReplaced.add((NamedExpression) ExpressionUtils.replace(expr, slotMap));
        }

        if (aggregate.child() instanceof PhysicalProject) {
            PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) aggregate.child();
            List<NamedExpression> newProjections = Lists.newArrayList(project.getProjects());
            newProjections.addAll(cseCandidates.values());
            project = project.withProjectionsAndChild(newProjections, (Plan) project.child());
            aggregate = (PhysicalHashAggregate<? extends Plan>) aggregate
                    .withAggOutput(aggOutputReplaced)
                    .withChildren(project);
        } else {
            List<NamedExpression> projections = new ArrayList<>();
            projections.addAll(inputSlots);
            projections.addAll(cseCandidates.values());
            List<Slot> projectOutput = new ImmutableList.Builder<Slot>()
                    .addAll(inputSlots)
                    .addAll(slotMap.values())
                    .build();
            LogicalProperties projectLogicalProperties = new LogicalProperties(
                    () -> projectOutput,
                    () -> DataTrait.EMPTY_TRAIT
            );
            PhysicalProject<? extends Plan> project = new PhysicalProject<>(projections,
                    projectLogicalProperties,
                    aggregate.child());
            aggregate = (PhysicalHashAggregate<? extends Plan>) aggregate
                    .withAggOutput(aggOutputReplaced)
                    .withChildren(project);
        }
        return aggregate;
    }

    private void getCseCandidatesFromAggregateFunction(Expression expr, Map<Expression, Alias> result) {
        if (expr instanceof AggregateFunction) {
            for (Expression child : expr.children()) {
                if (!(child instanceof SlotReference) && !child.isConstant() && !(child instanceof OrderExpression)) {
                    if (child instanceof Alias) {
                        result.put(child, (Alias) child);
                    } else {
                        result.put(child, new Alias(child));
                    }
                }
            }
        } else {
            for (Expression child : expr.children()) {
                if (!(child instanceof SlotReference) && !child.isConstant()) {
                    getCseCandidatesFromAggregateFunction(child, result);
                }
            }
        }
    }
}
