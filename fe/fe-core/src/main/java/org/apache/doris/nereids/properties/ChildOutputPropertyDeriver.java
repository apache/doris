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

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final List<PhysicalProperties> childrenOutputProperties;

    public ChildOutputPropertyDeriver(List<PhysicalProperties> childrenOutputProperties) {
        this.childrenOutputProperties = Objects.requireNonNull(childrenOutputProperties);
    }

    public PhysicalProperties getOutputProperties(GroupExpression groupExpression) {
        return groupExpression.getPlan().accept(this, new PlanContext(groupExpression));
    }

    @Override
    public PhysicalProperties visit(Plan plan, PlanContext context) {
        return PhysicalProperties.ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> agg, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        switch (agg.getAggPhase()) {
            case LOCAL:
            case GLOBAL:
            case DISTINCT_LOCAL:
            case DISTINCT_GLOBAL:
                DistributionSpec childSpec = childOutputProperty.getDistributionSpec();
                // If child's property is enforced, change it to bucketed
                if (childSpec instanceof DistributionSpecHash
                        && ((DistributionSpecHash) childSpec).getShuffleType().equals(ShuffleType.ENFORCED)) {
                    DistributionSpecHash distributionSpecHash = (DistributionSpecHash) childSpec;
                    return new PhysicalProperties(distributionSpecHash.withShuffleType(ShuffleType.BUCKETED));
                }
                return new PhysicalProperties(childOutputProperty.getDistributionSpec());
            default:
                throw new RuntimeException("Could not derive output properties for agg phase: " + agg.getAggPhase());
        }
    }

    @Override
    public PhysicalProperties visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort,
            PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        if (sort.getSortPhase().isLocal()) {
            return new PhysicalProperties(
                    childrenOutputProperties.get(0).getDistributionSpec(),
                    new OrderSpec(sort.getOrderKeys()));
        }
        return new PhysicalProperties(DistributionSpecGather.INSTANCE, new OrderSpec(sort.getOrderKeys()));
    }

    @Override
    public PhysicalProperties visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(DistributionSpecGather.INSTANCE, childOutputProperty.getOrderSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalProject(PhysicalProject<? extends Plan> project, PlanContext context) {
        // TODO: order spec do not process since we do not use it.
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childProperties = childrenOutputProperties.get(0);
        DistributionSpec childDistributionSpec = childProperties.getDistributionSpec();
        OrderSpec childOrderSpec = childProperties.getOrderSpec();
        DistributionSpec outputDistributionSpec;
        if (childDistributionSpec instanceof DistributionSpecHash) {
            Map<ExprId, ExprId> projections = Maps.newHashMap();
            Set<ExprId> obstructions = Sets.newHashSet();
            for (NamedExpression namedExpression : project.getProjects()) {
                if (namedExpression instanceof Alias) {
                    Alias alias = (Alias) namedExpression;
                    Expression child = alias.child();
                    if (child instanceof SlotReference) {
                        projections.put(((SlotReference) child).getExprId(), alias.getExprId());
                    } else if (child instanceof Cast && child.child(0) instanceof Slot
                            && isSameHashValue(child.child(0).getDataType(), child.getDataType())) {
                        // cast(slot as varchar(10)) can do projection if slot is varchar(3)
                        projections.put(((Slot) child.child(0)).getExprId(), alias.getExprId());
                    } else {
                        obstructions.addAll(
                                child.getInputSlots().stream()
                                        .map(NamedExpression::getExprId)
                                        .collect(Collectors.toSet()));
                    }
                }
            }
            if (projections.entrySet().stream().allMatch(kv -> kv.getKey().equals(kv.getValue()))) {
                return childrenOutputProperties.get(0);
            }
            outputDistributionSpec = ((DistributionSpecHash) childDistributionSpec).project(projections, obstructions);
            return new PhysicalProperties(outputDistributionSpec, childOrderSpec);
        } else {
            return childrenOutputProperties.get(0);
        }

    }

    @Override
    public PhysicalProperties visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalDistribute(
            PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        return distribute.getPhysicalProperties();
    }

    @Override
    public PhysicalProperties visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        PhysicalProperties rightOutputProperty = childrenOutputProperties.get(1);

        // broadcast
        if (rightOutputProperty.getDistributionSpec() instanceof DistributionSpecReplicated) {
            DistributionSpec parentDistributionSpec = leftOutputProperty.getDistributionSpec();
            return new PhysicalProperties(parentDistributionSpec);
        }

        // shuffle
        if (leftOutputProperty.getDistributionSpec() instanceof DistributionSpecHash
                && rightOutputProperty.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash leftHashSpec = (DistributionSpecHash) leftOutputProperty.getDistributionSpec();
            DistributionSpecHash rightHashSpec = (DistributionSpecHash) rightOutputProperty.getDistributionSpec();

            // colocate join
            if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                    && rightHashSpec.getShuffleType() == ShuffleType.NATURAL) {
                if (JoinUtils.couldColocateJoin(leftHashSpec, rightHashSpec)) {
                    return new PhysicalProperties(DistributionSpecHash.merge(leftHashSpec, rightHashSpec));
                }
            }

            // shuffle, if left child is natural mean current join is bucket shuffle join
            // and remain natural for colocate join on upper join.
            return new PhysicalProperties(DistributionSpecHash.merge(leftHashSpec, rightHashSpec,
                    leftHashSpec.getShuffleType() == ShuffleType.NATURAL ? ShuffleType.NATURAL : ShuffleType.BUCKETED));
        }

        throw new RuntimeException("Could not derive hash join's output properties. join: " + hashJoin);
    }

    @Override
    public PhysicalProperties visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        // TODO: currently, only support cross join in BE
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanContext context) {
        // TODO: find a better way to handle both tablet num == 1 and colocate table together in future
        if (!olapScan.getTable().isColocateTable() && olapScan.getScanTabletNum() == 1
                && (!ConnectContext.get().getSessionVariable().enablePipelineEngine()
                        || ConnectContext.get().getSessionVariable().getParallelExecInstanceNum() == 1)) {
            return PhysicalProperties.GATHER;
        } else if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            return PhysicalProperties.createHash((DistributionSpecHash) olapScan.getDistributionSpec());
        } else {
            return PhysicalProperties.ANY;
        }
    }

    @Override
    public PhysicalProperties visitPhysicalFileScan(PhysicalFileScan fileScan, PlanContext context) {
        return PhysicalProperties.ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanContext context) {
        return storageLayerAggregate.getRelation().accept(this, context);
    }

    @Override
    public PhysicalProperties visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, PlanContext context) {
        return PhysicalProperties.ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalEsScan(PhysicalEsScan esScan, PlanContext context) {
        return PhysicalProperties.ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanContext context) {
        TableValuedFunction function = tvfRelation.getFunction();
        return function.getPhysicalProperties();
    }

    @Override
    public PhysicalProperties visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanContext context) {
        return PhysicalProperties.GATHER;
    }

    @Override
    public PhysicalProperties visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    private boolean isSameHashValue(DataType originType, DataType castType) {
        if (originType.isStringLikeType() && (castType.isVarcharType() || castType.isStringType())
                && (castType.width() >= originType.width() || castType.width() < 0)) {
            return true;
        } else {
            return false;
        }
    }
}
