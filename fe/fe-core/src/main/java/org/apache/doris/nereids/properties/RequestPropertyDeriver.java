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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for parent property drive.
 */
public class RequestPropertyDeriver extends PlanVisitor<Void, PlanContext> {
    /*
     * requestPropertyFromParent
     *             │
     *             ▼
     *          curNode (current plan node in current CostAndEnforcerJob)
     *             │
     *             ▼
     * requestPropertyToChildren
     */
    private final PhysicalProperties requestPropertyFromParent;
    private List<List<PhysicalProperties>> requestPropertyToChildren;

    public RequestPropertyDeriver(JobContext context) {
        this.requestPropertyFromParent = context.getRequiredProperties();
    }

    /**
     * get request children property list
     */
    public List<List<PhysicalProperties>> getRequestChildrenPropertyList(GroupExpression groupExpression) {
        requestPropertyToChildren = Lists.newArrayList();
        groupExpression.getPlan().accept(this, new PlanContext(groupExpression));
        return requestPropertyToChildren;
    }

    @Override
    public Void visit(Plan plan, PlanContext context) {
        List<PhysicalProperties> requiredPropertyList =
                Lists.newArrayListWithCapacity(context.getGroupExpression().arity());
        for (int i = context.getGroupExpression().arity(); i > 0; --i) {
            requiredPropertyList.add(PhysicalProperties.ANY);
        }
        requestPropertyToChildren.add(requiredPropertyList);
        return null;
    }

    @Override
    public Void visitPhysicalAggregate(PhysicalAggregate<? extends Plan> agg, PlanContext context) {
        // 1. first phase agg just return any
        if (agg.getAggPhase().isLocal() && !agg.isFinalPhase()) {
            addRequestPropertyToChildren(PhysicalProperties.ANY);
            return null;
        }
        if (agg.getAggPhase() == AggPhase.GLOBAL && !agg.isFinalPhase()) {
            addRequestPropertyToChildren(requestPropertyFromParent);
            return null;
        }
        // 2. second phase agg, need to return shuffle with partition key
        List<Expression> partitionExpressions = agg.getPartitionExpressions();
        if (partitionExpressions.isEmpty() && agg.getAggPhase() != AggPhase.DISTINCT_LOCAL) {
            addRequestPropertyToChildren(PhysicalProperties.GATHER);
            return null;
        }
        if (agg.getAggPhase() == AggPhase.DISTINCT_LOCAL) {
            // use slots in distinct agg as shuffle slots
            List<ExprId> shuffleSlots = extractFromDistinctFunction(agg.getOutputExpressions());
            Preconditions.checkState(!shuffleSlots.isEmpty());
            addRequestPropertyToChildren(
                    PhysicalProperties.createHash(new DistributionSpecHash(shuffleSlots, ShuffleType.AGGREGATE)));
            return null;
        }
        // TODO: when parent is a join node,
        //    use requestPropertyFromParent to keep column order as join to avoid shuffle again.
        if (partitionExpressions.stream().allMatch(SlotReference.class::isInstance)) {
            List<ExprId> partitionedSlots = partitionExpressions.stream()
                    .map(SlotReference.class::cast)
                    .map(SlotReference::getExprId)
                    .collect(Collectors.toList());
            addRequestPropertyToChildren(
                    PhysicalProperties.createHash(new DistributionSpecHash(partitionedSlots, ShuffleType.AGGREGATE)));
            return null;
        }

        throw new RuntimeException("Need to add a rule to split aggregate to aggregate(project),"
                + " see more in AggregateDisassemble");

        // TODO: add other phase logical when we support distinct aggregate
    }

    @Override
    public Void visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.ANY);
        return null;
    }

    @Override
    public Void visitPhysicalLocalQuickSort(PhysicalLocalQuickSort<? extends Plan> sort, PlanContext context) {
        // TODO: rethink here, should we throw exception directly?
        addRequestPropertyToChildren(PhysicalProperties.ANY);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, PlanContext context) {
        // for shuffle join
        if (JoinUtils.couldShuffle(hashJoin)) {
            Pair<List<ExprId>, List<ExprId>> onClauseUsedSlots = JoinUtils.getOnClauseUsedSlots(hashJoin);
            // shuffle join
            addRequestPropertyToChildren(
                    PhysicalProperties.createHash(
                            new DistributionSpecHash(onClauseUsedSlots.first, ShuffleType.JOIN)),
                    PhysicalProperties.createHash(
                            new DistributionSpecHash(onClauseUsedSlots.second, ShuffleType.JOIN)));
        }
        // for broadcast join
        if (JoinUtils.couldBroadcast(hashJoin)) {
            addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
        }

        return null;
    }

    @Override
    public Void visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, PlanContext context) {
        // TODO: currently doris only use NLJ to do cross join, update this if we use NLJ to do other joins.
        addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
        return null;
    }

    @Override
    public Void visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.GATHER);
        return null;
    }

    /**
     * helper function to assemble request children physical properties
     * @param physicalProperties one set request properties for children
     */
    private void addRequestPropertyToChildren(PhysicalProperties... physicalProperties) {
        requestPropertyToChildren.add(Lists.newArrayList(physicalProperties));
    }

    private List<ExprId> extractFromDistinctFunction(List<NamedExpression> outputExpression) {
        List<ExprId> exprIds = Lists.newArrayList();
        for (NamedExpression originOutputExpr : outputExpression) {
            Set<AggregateFunction> aggregateFunctions
                    = originOutputExpr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                if (aggregateFunction.isDistinct()) {
                    for (Expression expr : aggregateFunction.children()) {
                        Preconditions.checkState(expr instanceof SlotReference, "normalize aggregate failed to"
                                + " normalize aggregate function " + aggregateFunction.toSql());
                        exprIds.add(((SlotReference) expr).getExprId());
                    }
                }
            }
        }
        return exprIds;
    }
}

