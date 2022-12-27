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
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
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
        if (plan instanceof RequirePropertiesSupplier) {
            RequireProperties requireProperties = ((RequirePropertiesSupplier) plan).getRequireProperties();
            List<PhysicalProperties> requestPhysicalProperties =
                    requireProperties.computeRequirePhysicalProperties(plan, requestPropertyFromParent);
            addRequestPropertyToChildren(requestPhysicalProperties);
            return null;
        }

        List<PhysicalProperties> requiredPropertyList =
                Lists.newArrayListWithCapacity(context.getGroupExpression().arity());
        for (int i = context.getGroupExpression().arity(); i > 0; --i) {
            requiredPropertyList.add(PhysicalProperties.ANY);
        }
        requestPropertyToChildren.add(requiredPropertyList);
        return null;
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
        JoinHint hint = hashJoin.getHint();
        switch (hint) {
            case BROADCAST_RIGHT:
                addBroadcastJoinRequestProperty();
                break;
            case SHUFFLE_RIGHT:
                addShuffleJoinRequestProperty(hashJoin);
                break;
            case NONE:
            default:
                // for shuffle join
                if (JoinUtils.couldShuffle(hashJoin)) {
                    addShuffleJoinRequestProperty(hashJoin);
                }
                // for broadcast join
                if (JoinUtils.couldBroadcast(hashJoin)) {
                    addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
                }

        }
        return null;
    }

    private void addBroadcastJoinRequestProperty() {
        addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
    }

    private void addShuffleJoinRequestProperty(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin) {
        Pair<List<ExprId>, List<ExprId>> onClauseUsedSlots = JoinUtils.getOnClauseUsedSlots(hashJoin);
        // shuffle join
        addRequestPropertyToChildren(
                PhysicalProperties.createHash(
                        new DistributionSpecHash(onClauseUsedSlots.first, ShuffleType.JOIN)),
                PhysicalProperties.createHash(
                        new DistributionSpecHash(onClauseUsedSlots.second, ShuffleType.JOIN)));
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

    @Override
    public Void visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.ANY);
        return null;
    }

    /**
     * helper function to assemble request children physical properties
     * @param physicalProperties one set request properties for children
     */
    private void addRequestPropertyToChildren(PhysicalProperties... physicalProperties) {
        requestPropertyToChildren.add(Lists.newArrayList(physicalProperties));
    }

    private void addRequestPropertyToChildren(List<PhysicalProperties> physicalProperties) {
        requestPropertyToChildren.add(physicalProperties);
    }

    private List<ExprId> extractExprIdFromDistinctFunction(List<NamedExpression> outputExpression) {
        Set<AggregateFunction> distinctAggregateFunctions = ExpressionUtils.collect(outputExpression, expr ->
                expr instanceof AggregateFunction && ((AggregateFunction) expr).isDistinct()
        );
        List<ExprId> exprIds = Lists.newArrayList();
        for (AggregateFunction aggregateFunction : distinctAggregateFunctions) {
            for (Expression expr : aggregateFunction.children()) {
                Preconditions.checkState(expr instanceof SlotReference, "normalize aggregate failed to"
                        + " normalize aggregate function " + aggregateFunction.toSql());
                exprIds.add(((SlotReference) expr).getExprId());
            }
        }
        return exprIds;
    }

    private void addRequestHashDistribution(List<Expression> hashColumns, ShuffleType shuffleType) {
        List<ExprId> partitionedSlots = hashColumns.stream()
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toList());
        addRequestPropertyToChildren(
                PhysicalProperties.createHash(new DistributionSpecHash(partitionedSlots, shuffleType)));
    }
}

