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
import org.apache.doris.nereids.exceptions.AnalysisException;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Arrays;
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

    public PhysicalProperties getOutputProperties(ConnectContext connectContext, GroupExpression groupExpression) {
        return groupExpression.getPlan().accept(this, new PlanContext(connectContext, groupExpression));
    }

    @Override
    public PhysicalProperties visit(Plan plan, PlanContext context) {
        if (childrenOutputProperties.isEmpty()) {
            return PhysicalProperties.ANY;
        } else {
            DistributionSpec firstChildSpec = childrenOutputProperties.get(0).getDistributionSpec();
            if (firstChildSpec instanceof DistributionSpecHash) {
                return PhysicalProperties.createAnyFromHash((DistributionSpecHash) firstChildSpec);
            } else {
                return PhysicalProperties.ANY;
            }
        }
    }

    /* ********************************************************************************************
     * sink Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PhysicalProperties visitPhysicalSink(PhysicalSink<? extends Plan> physicalSink, PlanContext context) {
        return PhysicalProperties.GATHER;
    }

    /* ********************************************************************************************
     * Leaf Plan Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PhysicalProperties visitPhysicalCTEConsumer(
            PhysicalCTEConsumer cteConsumer, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.isEmpty(), "cte consumer should be leaf node");
        return PhysicalProperties.MUST_SHUFFLE;
    }

    @Override
    public PhysicalProperties visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, PlanContext context) {
        return PhysicalProperties.GATHER;
    }

    @Override
    public PhysicalProperties visitPhysicalEsScan(PhysicalEsScan esScan, PlanContext context) {
        return PhysicalProperties.STORAGE_ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalFileScan(PhysicalFileScan fileScan, PlanContext context) {
        return PhysicalProperties.STORAGE_ANY;
    }

    /**
     * TODO return ANY after refactor coordinator
     * return STORAGE_ANY not ANY, in order to generate distribute on jdbc scan.
     * select * from (select * from external.T) as A union all (select * from external.T)
     * if visitPhysicalJdbcScan returns ANY, the plan is
     * union
     *  |--- JDBCSCAN
     *  +--- JDBCSCAN
     *  this breaks coordinator assumption that one fragment has at most only one scan.
     */
    @Override
    public PhysicalProperties visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, PlanContext context) {
        return PhysicalProperties.STORAGE_ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalOdbcScan(PhysicalOdbcScan odbcScan, PlanContext context) {
        return PhysicalProperties.STORAGE_ANY;
    }

    @Override
    public PhysicalProperties visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanContext context) {
        return new PhysicalProperties(olapScan.getDistributionSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalDeferMaterializeOlapScan(
            PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan, PlanContext context) {
        return visitPhysicalOlapScan(deferMaterializeOlapScan.getPhysicalOlapScan(), context);
    }

    @Override
    public PhysicalProperties visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, PlanContext context) {
        return PhysicalProperties.GATHER;
    }

    @Override
    public PhysicalProperties visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanContext context) {
        TableValuedFunction function = tvfRelation.getFunction();
        return function.getPhysicalProperties();
    }

    /* ********************************************************************************************
     * Other Node, in lexicographical order
     * ******************************************************************************************** */

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
                return new PhysicalProperties(childOutputProperty.getDistributionSpec());
            default:
                throw new RuntimeException("Could not derive output properties for agg phase: " + agg.getAggPhase());
        }
    }

    @Override
    public PhysicalProperties visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(childOutputProperty.getDistributionSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalCTEAnchor(
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        // return properties inherited from consumer side which may further be used at upper layer
        return childrenOutputProperties.get(1);
    }

    @Override
    public PhysicalProperties visitPhysicalCTEProducer(
            PhysicalCTEProducer<? extends Plan> cteProducer, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalDistribute(
            PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        return distribute.getPhysicalProperties();
    }

    @Override
    public PhysicalProperties visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        PhysicalProperties rightOutputProperty = childrenOutputProperties.get(1);

        // broadcast
        if (rightOutputProperty.getDistributionSpec() instanceof DistributionSpecReplicated) {
            DistributionSpec leftDistributionSpec = leftOutputProperty.getDistributionSpec();
            // if left side is hash distribute and the key can satisfy the join keys, then mock
            // a right side hash spec with the corresponding join keys, to filling the returning spec
            // with refined EquivalenceExprIds.
            if (leftDistributionSpec instanceof DistributionSpecHash
                    && !(hashJoin.isMarkJoin() && hashJoin.getHashJoinConjuncts().isEmpty())
                    && !hashJoin.getHashConjunctsExprIds().first.isEmpty()
                    && !hashJoin.getHashConjunctsExprIds().second.isEmpty()
                    && hashJoin.getHashConjunctsExprIds().first.size()
                        == hashJoin.getHashConjunctsExprIds().second.size()
                    && leftDistributionSpec.satisfy(
                            new DistributionSpecHash(hashJoin.getHashConjunctsExprIds().first, ShuffleType.REQUIRE))) {
                DistributionSpecHash mockedRightHashSpec = mockAnotherSideSpecFromConjuncts(
                        hashJoin, (DistributionSpecHash) leftDistributionSpec);
                if (SessionVariable.canUseNereidsDistributePlanner()) {
                    return computeShuffleJoinOutputProperties(hashJoin,
                            (DistributionSpecHash) leftDistributionSpec, mockedRightHashSpec);
                } else {
                    return legacyComputeShuffleJoinOutputProperties(hashJoin,
                            (DistributionSpecHash) leftDistributionSpec, mockedRightHashSpec);
                }
            } else {
                return new PhysicalProperties(leftDistributionSpec);
            }
        }

        // shuffle
        if (leftOutputProperty.getDistributionSpec() instanceof DistributionSpecHash
                && rightOutputProperty.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash leftHashSpec = (DistributionSpecHash) leftOutputProperty.getDistributionSpec();
            DistributionSpecHash rightHashSpec = (DistributionSpecHash) rightOutputProperty.getDistributionSpec();
            if (SessionVariable.canUseNereidsDistributePlanner()) {
                return computeShuffleJoinOutputProperties(hashJoin, leftHashSpec, rightHashSpec);
            } else {
                return legacyComputeShuffleJoinOutputProperties(hashJoin, leftHashSpec, rightHashSpec);
            }
        }

        throw new RuntimeException("Could not derive hash join's output properties. join: " + hashJoin);
    }

    @Override
    public PhysicalProperties visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    @Override
    public PhysicalProperties visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 2);
        PhysicalProperties leftOutputProperty = childrenOutputProperties.get(0);
        return new PhysicalProperties(leftOutputProperty.getDistributionSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalProject(PhysicalProject<? extends Plan> project, PlanContext context) {
        // TODO: order spec do not process since we do not use it.
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        PhysicalProperties childProperties = childrenOutputProperties.get(0);
        DistributionSpec childDistributionSpec = childProperties.getDistributionSpec();
        OrderSpec childOrderSpec = childProperties.getOrderSpec();
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
            DistributionSpecHash childDistributionSpecHash = (DistributionSpecHash) childDistributionSpec;
            DistributionSpec defaultAnySpec = childDistributionSpecHash.getShuffleType() == ShuffleType.NATURAL
                    ? DistributionSpecStorageAny.INSTANCE : DistributionSpecAny.INSTANCE;
            DistributionSpec outputDistributionSpec = childDistributionSpecHash.project(
                    projections, obstructions, defaultAnySpec);
            return new PhysicalProperties(outputDistributionSpec, childOrderSpec);
        } else {
            return childrenOutputProperties.get(0);
        }
    }

    @Override
    public PhysicalProperties visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        DistributionSpec childDistributionSpec = childrenOutputProperties.get(0).getDistributionSpec();
        PhysicalProperties output = childrenOutputProperties.get(0);
        if (childDistributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) childDistributionSpec;
            List<List<Expression>> groupingSets = repeat.getGroupingSets();
            if (!groupingSets.isEmpty()) {
                Set<Expression> intersectGroupingKeys = Utils.fastToImmutableSet(groupingSets.get(0));
                for (int i = 1; i < groupingSets.size() && !intersectGroupingKeys.isEmpty(); i++) {
                    intersectGroupingKeys = Sets.intersection(
                            intersectGroupingKeys, Utils.fastToImmutableSet(groupingSets.get(i))
                    );
                }
                if (!intersectGroupingKeys.isEmpty()) {
                    List<ExprId> orderedShuffledColumns = distributionSpecHash.getOrderedShuffledColumns();
                    boolean hashColumnsChanged = false;
                    for (Expression intersectGroupingKey : intersectGroupingKeys) {
                        if (!(intersectGroupingKey instanceof SlotReference)) {
                            hashColumnsChanged = true;
                            break;
                        }
                        if (!(orderedShuffledColumns.contains(((SlotReference) intersectGroupingKey).getExprId()))) {
                            hashColumnsChanged = true;
                            break;
                        }
                    }
                    if (!hashColumnsChanged) {
                        return childrenOutputProperties.get(0);
                    }
                }
            }
            output = PhysicalProperties.createAnyFromHash((DistributionSpecHash) childDistributionSpec);
        }
        return output.withOrderSpec(childrenOutputProperties.get(0).getOrderSpec());
    }

    @Override
    public PhysicalProperties visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN,
            PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        DistributionSpec childDistSpec = childrenOutputProperties.get(0).getDistributionSpec();

        if (partitionTopN.getPhase().isTwoPhaseLocal() || partitionTopN.getPhase().isOnePhaseGlobal()) {
            return new PhysicalProperties(childDistSpec);
        } else {
            Preconditions.checkState(partitionTopN.getPhase().isTwoPhaseGlobal(),
                    "partition topn phase is not two phase global");
            Preconditions.checkState(childDistSpec instanceof DistributionSpecHash,
                    "child dist spec is not hash spec");

            return new PhysicalProperties(childDistSpec, new OrderSpec(partitionTopN.getOrderKeys()));
        }
    }

    @Override
    public PhysicalProperties visitPhysicalSetOperation(PhysicalSetOperation setOperation, PlanContext context) {
        int[] offsetsOfFirstChild = null;
        ShuffleType firstType = null;
        List<DistributionSpec> childrenDistribution = childrenOutputProperties.stream()
                .map(PhysicalProperties::getDistributionSpec)
                .collect(Collectors.toList());
        if (childrenDistribution.isEmpty()) {
            // no child, mean it only has some one-row-relations
            return PhysicalProperties.GATHER;
        }
        if (childrenDistribution.stream().allMatch(DistributionSpecGather.class::isInstance)) {
            return PhysicalProperties.GATHER;
        }
        for (int i = 0; i < childrenDistribution.size(); i++) {
            DistributionSpec childDistribution = childrenDistribution.get(i);
            if (!(childDistribution instanceof DistributionSpecHash)) {
                if (i != 0) {
                    // NOTICE: if come here, the first child output must be DistributionSpecHash
                    return PhysicalProperties.createAnyFromHash((DistributionSpecHash) childrenDistribution.get(0));
                } else {
                    return new PhysicalProperties(childDistribution);
                }
            }
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) childDistribution;
            int[] offsetsOfCurrentChild = new int[distributionSpecHash.getOrderedShuffledColumns().size()];
            for (int j = 0; j < setOperation.getRegularChildOutput(i).size(); j++) {
                int offset = distributionSpecHash.getExprIdToEquivalenceSet()
                        .getOrDefault(setOperation.getRegularChildOutput(i).get(j).getExprId(), -1);
                if (offset >= 0) {
                    offsetsOfCurrentChild[offset] = j;
                } else {
                    // NOTICE: if come here, the first child output must be DistributionSpecHash
                    return PhysicalProperties.createAnyFromHash((DistributionSpecHash) childrenDistribution.get(0));
                }
            }
            if (offsetsOfFirstChild == null) {
                firstType = ((DistributionSpecHash) childDistribution).getShuffleType();
                offsetsOfFirstChild = offsetsOfCurrentChild;
            } else if (!Arrays.equals(offsetsOfFirstChild, offsetsOfCurrentChild)
                    || firstType != ((DistributionSpecHash) childDistribution).getShuffleType()) {
                // NOTICE: if come here, the first child output must be DistributionSpecHash
                return PhysicalProperties.createAnyFromHash((DistributionSpecHash) childrenDistribution.get(0));
            }
        }
        // bucket
        List<ExprId> request = Lists.newArrayList();
        for (int offset : offsetsOfFirstChild) {
            request.add(setOperation.getOutput().get(offset).getExprId());
        }
        return PhysicalProperties.createHash(request, firstType);
    }

    @Override
    public PhysicalProperties visitPhysicalUnion(PhysicalUnion union, PlanContext context) {
        if (union.getConstantExprsList().isEmpty()) {
            return visitPhysicalSetOperation(union, context);
        } else {
            // current be could not run const expr on appropriate node,
            // so if we have constant exprs on union, the output of union always any
            return visit(union, context);
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
    public PhysicalProperties visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PlanContext context) {
        Preconditions.checkState(childrenOutputProperties.size() == 1);
        return childrenOutputProperties.get(0);
    }

    private PhysicalProperties computeShuffleJoinOutputProperties(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            DistributionSpecHash leftHashSpec, DistributionSpecHash rightHashSpec) {

        ShuffleSide shuffleSide = computeShuffleSide(leftHashSpec, rightHashSpec);

        ShuffleType outputShuffleType = shuffleSide == ShuffleSide.LEFT
                ? rightHashSpec.getShuffleType() : leftHashSpec.getShuffleType();

        switch (hashJoin.getJoinType()) {
            case INNER_JOIN:
                if (shuffleSide == ShuffleSide.LEFT) {
                    return new PhysicalProperties(DistributionSpecHash.merge(
                            rightHashSpec, leftHashSpec, outputShuffleType));
                } else {
                    return new PhysicalProperties(DistributionSpecHash.merge(
                            leftHashSpec, rightHashSpec, outputShuffleType));
                }
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
            case LEFT_OUTER_JOIN:
                if (shuffleSide == ShuffleSide.LEFT) {
                    return new PhysicalProperties(
                            leftHashSpec.withShuffleTypeAndForbidColocateJoin(outputShuffleType)
                    );
                } else {
                    return new PhysicalProperties(leftHashSpec);
                }
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
            case RIGHT_OUTER_JOIN:
                if (shuffleSide == ShuffleSide.RIGHT) {
                    return new PhysicalProperties(
                            rightHashSpec.withShuffleTypeAndForbidColocateJoin(outputShuffleType)
                    );
                } else {
                    return new PhysicalProperties(rightHashSpec);
                }
            case FULL_OUTER_JOIN:
                return PhysicalProperties.createAnyFromHash(leftHashSpec, rightHashSpec);
            default:
                throw new AnalysisException("unknown join type " + hashJoin.getJoinType());
        }
    }

    private ShuffleSide computeShuffleSide(DistributionSpecHash leftHashSpec, DistributionSpecHash rightHashSpec) {
        ShuffleType leftShuffleType = leftHashSpec.getShuffleType();
        ShuffleType rightShuffleType = rightHashSpec.getShuffleType();
        switch (leftShuffleType) {
            case EXECUTION_BUCKETED:
                if (rightShuffleType == ShuffleType.EXECUTION_BUCKETED) {
                    return ShuffleSide.BOTH;
                }
                break;
            case STORAGE_BUCKETED:
                if (rightShuffleType == ShuffleType.NATURAL) {
                    // use storage hash to shuffle left to right to do bucket shuffle join
                    return ShuffleSide.LEFT;
                }
                break;
            case NATURAL:
                switch (rightShuffleType) {
                    case NATURAL:
                        // colocate join
                        return ShuffleSide.NONE;
                    case STORAGE_BUCKETED:
                        // use storage hash to shuffle right to left to do bucket shuffle join
                        return ShuffleSide.RIGHT;
                    default:
                }
                break;
            default:
        }
        throw new IllegalStateException(
                "Illegal join with wrong distribution, left: " + leftShuffleType + ", right: " + rightShuffleType);
    }

    private PhysicalProperties legacyComputeShuffleJoinOutputProperties(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            DistributionSpecHash leftHashSpec, DistributionSpecHash rightHashSpec) {
        switch (hashJoin.getJoinType()) {
            case INNER_JOIN:
            case CROSS_JOIN:
                return new PhysicalProperties(DistributionSpecHash.merge(
                        leftHashSpec, rightHashSpec, leftHashSpec.getShuffleType()));
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
            case LEFT_OUTER_JOIN:
                return new PhysicalProperties(leftHashSpec);
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
            case RIGHT_OUTER_JOIN:
                if (JoinUtils.couldColocateJoin(leftHashSpec, rightHashSpec, hashJoin.getHashJoinConjuncts())) {
                    return new PhysicalProperties(rightHashSpec);
                } else {
                    // retain left shuffle type, since coordinator use left most node to schedule fragment
                    // forbid colocate join, since right table already shuffle
                    return new PhysicalProperties(rightHashSpec.withShuffleTypeAndForbidColocateJoin(
                            leftHashSpec.getShuffleType()));
                }
            case FULL_OUTER_JOIN:
                return PhysicalProperties.createAnyFromHash(leftHashSpec);
            default:
                throw new AnalysisException("unknown join type " + hashJoin.getJoinType());
        }
    }

    private DistributionSpecHash mockAnotherSideSpecFromConjuncts(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, DistributionSpecHash oneSideSpec) {
        List<ExprId> leftExprIds = hashJoin.getHashConjunctsExprIds().first;
        List<ExprId> rightExprIds = hashJoin.getHashConjunctsExprIds().second;
        Preconditions.checkState(!leftExprIds.isEmpty() && !rightExprIds.isEmpty()
                && leftExprIds.size() == rightExprIds.size(), "invalid hash join conjuncts");
        List<ExprId> anotherSideOrderedExprIds = Lists.newArrayList();
        for (ExprId exprId : oneSideSpec.getOrderedShuffledColumns()) {
            int index = leftExprIds.indexOf(exprId);
            if (index == -1) {
                Set<ExprId> equivalentExprIds = oneSideSpec.getEquivalenceExprIdsOf(exprId);
                for (ExprId id : equivalentExprIds) {
                    index = leftExprIds.indexOf(id);
                    if (index >= 0) {
                        break;
                    }
                }
                Preconditions.checkState(index >= 0, "can't find exprId in equivalence set");
            }
            anotherSideOrderedExprIds.add(rightExprIds.get(index));
        }
        return new DistributionSpecHash(anotherSideOrderedExprIds, oneSideSpec.getShuffleType());
    }

    private boolean isSameHashValue(DataType originType, DataType castType) {
        if (originType.isStringLikeType() && (castType.isVarcharType() || castType.isStringType())
                && (castType.width() >= originType.width() || castType.width() < 0)) {
            return true;
        } else {
            return false;
        }
    }

    private enum ShuffleSide {
        LEFT, RIGHT, BOTH, NONE
    }
}
