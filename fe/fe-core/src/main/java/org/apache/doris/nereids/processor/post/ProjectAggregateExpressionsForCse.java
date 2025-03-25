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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
            List<NamedExpression> newProjections = Lists.newArrayList();
            // do column prune
            // case 1:
            // original plan
            //   agg(groupKey[C+1, abs(C+1)]
            //     -->project(A+B as C)
            //
            //  "A+B as C" should be reserved
            //  new plan
            //  agg(groupKey=[D, abs(D)])
            //    -->project(A+B as C, C+1 as D)
            //  case 2:
            // original plan
            //  agg(groupKey[A+1, abs(A+1)], output[sum(B)])
            //    --> project(A, B)
            // "A+1" is extracted, we have
            //  plan1:
            //    agg(groupKey[X, abs(X)], output[sum(B)])
            //         --> project(A, B, A+1 as X)
            // then column prune(A should be pruned, because it is not used directly by AGG)
            // we have plan2:
            //     agg(groupKey[X, abs(X)], output[sum(B)])
            //          -->project(B, A+1 as X)
            PhysicalProject<? extends Plan> project = (PhysicalProject<? extends Plan>) aggregate.child();
            Set<Slot> newInputSlots = aggOutputReplaced.stream()
                    .flatMap(expr -> expr.getInputSlots().stream())
                    .collect(Collectors.toSet());
            for (NamedExpression expr : project.getProjects()) {
                if (!(expr instanceof SlotReference) || newInputSlots.contains(expr)) {
                    newProjections.add(expr);
                }
            }
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
            AbstractPhysicalPlan child = ((AbstractPhysicalPlan) aggregate.child());
            PhysicalProperties projectPhysicalProperties = new PhysicalProperties(
                    child.getPhysicalProperties().getDistributionSpec(),
                    child.getPhysicalProperties().getOrderSpec());
            PhysicalProject<? extends Plan> project = new PhysicalProject<>(projections, Optional.empty(),
                    projectLogicalProperties,
                    projectPhysicalProperties,
                    child.getStats(),
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
