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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for property drive.
 */
public class ChildOutputPropertyDeriver extends PlanVisitor<PhysicalProperties, PlanContext> {
    /*
     *   parentPlanNode
     *         ▲
     *         │
     * childOutputProperty
     */
    PhysicalProperties requestProperty;
    List<PhysicalProperties> childrenOutputProperties;

    public ChildOutputPropertyDeriver(PhysicalProperties requestProperty,
            List<PhysicalProperties> childrenOutputProperties) {
        this.childrenOutputProperties = childrenOutputProperties;
        this.requestProperty = requestProperty;
    }

    public static PhysicalProperties getOutputProperties(
            PhysicalProperties requirements,
            List<PhysicalProperties> childrenOutputProperties,
            GroupExpression groupExpression) {

        ChildOutputPropertyDeriver childOutputPropertyDeriver = new ChildOutputPropertyDeriver(requirements,
                childrenOutputProperties);

        return groupExpression.getPlan().accept(childOutputPropertyDeriver, new PlanContext(groupExpression));
    }

    @Override
    public PhysicalProperties visit(Plan plan, PlanContext context) {
        return PhysicalProperties.ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalAggregate(PhysicalAggregate<Plan> agg, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        // TODO: add distinct phase output properties
        switch (agg.getAggPhase()) {
            case LOCAL:
                return new PhysicalProperties(childOutputProperty.getDistributionSpec());
            case GLOBAL:
                List<ExprId> columns = agg.getPartitionExpressions().stream()
                        .map(SlotReference.class::cast)
                        .map(SlotReference::getExprId)
                        .collect(Collectors.toList());
                return PhysicalProperties.createHash(new DistributionSpecHash(columns, ShuffleType.AGGREGATE));
            case DISTINCT_LOCAL:
            case DISTINCT_GLOBAL:
            default:
                throw new RuntimeException("Could not derive output properties for agg phase: " + agg.getAggPhase());
        }
    }

    @Override
    public PhysicalProperties visitPhysicalHeapSort(PhysicalHeapSort<Plan> sort, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(DistributionSpecGather.INSTANCE, new OrderSpec(sort.getOrderKeys()));
    }

    @Override
    public PhysicalProperties visitPhysicalLimit(PhysicalLimit<Plan> limit, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(DistributionSpecGather.INSTANCE, childOutputProperty.getOrderSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalProject(PhysicalProject<Plan> project, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalFilter(PhysicalFilter<Plan> filter, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalDistribution(PhysicalDistribute<Plan> distribution, PlanContext context) {
        return distribution.getPhysicalProperties();
    }

    @Override
    public PhysicalProperties visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        PhysicalProperties rightOutputProperty = childrenOutputProperties.get(1);

        // broadcast
        if (rightOutputProperty.getDistributionSpec() instanceof DistributionSpecReplicated) {
            return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
        }

        // shuffle
        if (leftOutputProperty.getDistributionSpec() instanceof DistributionSpecHash
                && rightOutputProperty.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash leftHashSpec = (DistributionSpecHash) leftOutputProperty.getDistributionSpec();
            DistributionSpecHash rightHashSpec = (DistributionSpecHash) rightOutputProperty.getDistributionSpec();

            // colocate join
            if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                    && rightHashSpec.getShuffleType() == ShuffleType.NATURAL) {
                final long leftTableId = leftHashSpec.getTableId();
                final long rightTableId = rightHashSpec.getTableId();
                final Set<Long> leftTablePartitions = leftHashSpec.getPartitionIds();
                final Set<Long> rightTablePartitions = rightHashSpec.getPartitionIds();
                boolean noNeedCheckColocateGroup = (leftTableId == rightTableId)
                        && (leftTablePartitions.equals(rightTablePartitions)) && (leftTablePartitions.size() <= 1);
                ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
                if (noNeedCheckColocateGroup
                        || (colocateIndex.isSameGroup(leftTableId, rightTableId)
                        && colocateIndex.isGroupUnstable(colocateIndex.getGroup(leftTableId)))) {
                    // TODO: need to merge right bucket column to output property
                    return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
                }
            }

            // bucket shuffle
            if (rightHashSpec.getShuffleType() == ShuffleType.BUCKET) {
                return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
            }

            // shuffle
            // TODO: need to merge right bucket column to output property
            DistributionSpecHash outputHash
                    = new DistributionSpecHash(leftHashSpec.getShuffledColumns(), ShuffleType.JOIN);
            return new PhysicalProperties(outputHash);
        }

        throw new RuntimeException("Could not derive hash join's output properties. join: " + hashJoin);
    }

    @Override
    public PhysicalProperties visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<Plan, Plan> nestedLoopJoin,
            PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanContext context) {
        if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            return PhysicalProperties.createHash((DistributionSpecHash) olapScan.getDistributionSpec());
        } else {
            return PhysicalProperties.ANY;
        }
    }
}
