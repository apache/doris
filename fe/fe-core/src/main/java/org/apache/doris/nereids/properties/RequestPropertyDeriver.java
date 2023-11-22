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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
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
    private final ConnectContext connectContext;
    private final PhysicalProperties requestPropertyFromParent;
    private List<List<PhysicalProperties>> requestPropertyToChildren;

    public RequestPropertyDeriver(ConnectContext connectContext, JobContext context) {
        this.connectContext = connectContext;
        this.requestPropertyFromParent = context.getRequiredProperties();
    }

    public RequestPropertyDeriver(ConnectContext connectContext, PhysicalProperties requestPropertyFromParent) {
        this.connectContext = connectContext;
        this.requestPropertyFromParent = requestPropertyFromParent;
    }

    /**
     * get request children property list
     */
    public List<List<PhysicalProperties>> getRequestChildrenPropertyList(GroupExpression groupExpression) {
        requestPropertyToChildren = Lists.newArrayList();
        groupExpression.getPlan().accept(this, new PlanContext(connectContext, groupExpression));
        return requestPropertyToChildren;
    }

    @Override
    public Void visit(Plan plan, PlanContext context) {
        if (plan instanceof RequirePropertiesSupplier) {
            RequireProperties requireProperties = ((RequirePropertiesSupplier<?>) plan).getRequireProperties();
            List<PhysicalProperties> requestPhysicalProperties =
                    requireProperties.computeRequirePhysicalProperties(plan, requestPropertyFromParent);
            addRequestPropertyToChildren(requestPhysicalProperties);
            return null;
        }

        List<PhysicalProperties> requiredPropertyList =
                Lists.newArrayListWithCapacity(context.arity());
        for (int i = context.arity(); i > 0; --i) {
            requiredPropertyList.add(PhysicalProperties.ANY);
        }
        addRequestPropertyToChildren(requiredPropertyList);
        return null;
    }

    /* ********************************************************************************************
     * sink Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public Void visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> olapTableSink, PlanContext context) {
        if (connectContext != null && !connectContext.getSessionVariable().enableStrictConsistencyDml) {
            addRequestPropertyToChildren(PhysicalProperties.ANY);
        } else {
            addRequestPropertyToChildren(olapTableSink.getRequirePhysicalProperties());
        }
        return null;
    }

    @Override
    public Void visitPhysicalResultSink(PhysicalResultSink<? extends Plan> physicalResultSink, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.GATHER);
        return null;
    }

    @Override
    public Void visitPhysicalDeferMaterializeResultSink(
            PhysicalDeferMaterializeResultSink<? extends Plan> sink,
            PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.GATHER);
        return null;
    }

    /* ********************************************************************************************
     * Other Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public Void visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.GATHER);
        return null;
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.ANY, requestPropertyFromParent);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, PlanContext context) {
        JoinHint hint = hashJoin.getHint();
        if (hint == JoinHint.BROADCAST_RIGHT && JoinUtils.couldBroadcast(hashJoin)) {
            addBroadcastJoinRequestProperty();
            return null;
        }
        if (hint == JoinHint.SHUFFLE_RIGHT && JoinUtils.couldShuffle(hashJoin)) {
            addShuffleJoinRequestProperty(hashJoin);
            return null;
        }
        // for shuffle join
        if (JoinUtils.couldShuffle(hashJoin)) {
            addShuffleJoinRequestProperty(hashJoin);
        }

        // for broadcast join
        if (JoinUtils.couldBroadcast(hashJoin)
                && (JoinUtils.checkBroadcastJoinStats(hashJoin) || requestPropertyToChildren.isEmpty())) {
            addBroadcastJoinRequestProperty();
        }
        return null;
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PlanContext context) {
        if (limit.isGlobal()) {
            addRequestPropertyToChildren(PhysicalProperties.GATHER);
        } else {
            addRequestPropertyToChildren(PhysicalProperties.ANY);
        }
        return null;
    }

    @Override
    public Void visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, PlanContext context) {
        // see canParallelize() in NestedLoopJoinNode
        if (nestedLoopJoin.getJoinType().isCrossJoin() || nestedLoopJoin.getJoinType().isInnerJoin()
                || nestedLoopJoin.getJoinType().isLeftJoin()) {
            addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
        } else {
            addRequestPropertyToChildren(PhysicalProperties.GATHER, PhysicalProperties.GATHER);
        }
        return null;
    }

    @Override
    public Void visitPhysicalSetOperation(PhysicalSetOperation setOperation, PlanContext context) {
        // intersect and except need do distinct, so we must do distribution on it.
        DistributionSpec distributionRequestFromParent = requestPropertyFromParent.getDistributionSpec();
        if (distributionRequestFromParent instanceof DistributionSpecHash) {
            // shuffle according to parent require
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) distributionRequestFromParent;
            addRequestPropertyToChildren(createHashRequestAccordingToParent(
                    setOperation, distributionSpecHash, context));
        } else {
            // shuffle all column
            // TODO: for wide table, may be we should add a upper limit of shuffle columns
            addRequestPropertyToChildren(setOperation.getRegularChildrenOutputs().stream()
                    .map(childOutputs -> childOutputs.stream()
                            .map(SlotReference::getExprId)
                            .collect(ImmutableList.toImmutableList()))
                    .map(l -> PhysicalProperties.createHash(l, ShuffleType.EXECUTION_BUCKETED))
                    .collect(Collectors.toList()));
        }
        return null;
    }

    @Override
    public Void visitPhysicalUnion(PhysicalUnion union, PlanContext context) {
        // TODO: we do not generate gather union until we could do better cost computation on set operation
        List<PhysicalProperties> requiredPropertyList =
                Lists.newArrayListWithCapacity(context.arity());
        if (union.getConstantExprsList().isEmpty()) {
            // translate requestPropertyFromParent to other children's request.
            DistributionSpec distributionRequestFromParent = requestPropertyFromParent.getDistributionSpec();
            if (distributionRequestFromParent instanceof DistributionSpecHash) {
                DistributionSpecHash distributionSpecHash = (DistributionSpecHash) distributionRequestFromParent;
                requiredPropertyList = createHashRequestAccordingToParent(union, distributionSpecHash, context);
            } else {
                for (int i = context.arity(); i > 0; --i) {
                    requiredPropertyList.add(PhysicalProperties.ANY);
                }
            }

        } else {
            // current be could not run const expr on appropriate node,
            // so if we have constant exprs on union, the output of union always any
            // then any other request on children is useless.
            for (int i = context.arity(); i > 0; --i) {
                requiredPropertyList.add(PhysicalProperties.ANY);
            }
        }
        addRequestPropertyToChildren(requiredPropertyList);
        return null;
    }

    @Override
    public Void visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, PlanContext context) {
        if (!sort.getSortPhase().isLocal()) {
            addRequestPropertyToChildren(PhysicalProperties.GATHER);
        } else {
            addRequestPropertyToChildren(PhysicalProperties.ANY);
        }
        return null;
    }

    @Override
    public Void visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PlanContext context) {
        if (partitionTopN.getPhase().isTwoPhaseLocal()) {
            addRequestPropertyToChildren(PhysicalProperties.ANY);
        } else {
            Preconditions.checkState(partitionTopN.getPhase().isTwoPhaseGlobal()
                            || partitionTopN.getPhase().isOnePhaseGlobal(),
                    "partition topn phase is not two phase global or one phase global");
            PhysicalProperties properties = PhysicalProperties.createHash(partitionTopN.getPartitionKeys(),
                    ShuffleType.REQUIRE);
            addRequestPropertyToChildren(properties);
        }
        return null;
    }

    @Override
    public Void visitPhysicalFileSink(PhysicalFileSink<? extends Plan> fileSink, PlanContext context) {
        addRequestPropertyToChildren(PhysicalProperties.GATHER);
        return null;
    }

    private List<PhysicalProperties> createHashRequestAccordingToParent(
            SetOperation setOperation, DistributionSpecHash distributionRequestFromParent, PlanContext context) {
        List<PhysicalProperties> requiredPropertyList =
                Lists.newArrayListWithCapacity(context.arity());
        int[] outputOffsets = new int[distributionRequestFromParent.getOrderedShuffledColumns().size()];
        List<NamedExpression> setOperationOutputs = setOperation.getOutputs();
        // get the offset of bucketed columns of set operation
        for (int i = 0; i < setOperationOutputs.size(); i++) {
            int offset = distributionRequestFromParent.getExprIdToEquivalenceSet()
                    .getOrDefault(setOperationOutputs.get(i).getExprId(), -1);
            if (offset >= 0) {
                outputOffsets[offset] = i;
            }
        }
        // use the offset to generate children's request
        for (int i = 0; i < context.arity(); i++) {
            List<SlotReference> childOutput = setOperation.getRegularChildOutput(i);
            ImmutableList.Builder<ExprId> childRequest = ImmutableList.builder();
            for (int offset : outputOffsets) {
                childRequest.add(childOutput.get(offset).getExprId());
            }
            requiredPropertyList.add(PhysicalProperties.createHash(
                    childRequest.build(), distributionRequestFromParent.getShuffleType()));
        }
        return requiredPropertyList;
    }

    private void addBroadcastJoinRequestProperty() {
        addRequestPropertyToChildren(PhysicalProperties.ANY, PhysicalProperties.REPLICATED);
    }

    private void addShuffleJoinRequestProperty(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin) {
        Pair<List<ExprId>, List<ExprId>> onClauseUsedSlots = hashJoin.getHashConjunctsExprIds();
        // shuffle join
        addRequestPropertyToChildren(
                PhysicalProperties.createHash(
                        new DistributionSpecHash(onClauseUsedSlots.first, ShuffleType.REQUIRE)),
                PhysicalProperties.createHash(
                        new DistributionSpecHash(onClauseUsedSlots.second, ShuffleType.REQUIRE)));
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
}

