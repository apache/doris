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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.CrossJoinNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to translate to physical plan generated by new optimizer to the plan fragments.
 * <STRONG>
 * ATTENTION:
 * Must always visit plan's children first when you implement a method to translate from PhysicalPlan to PlanNode.
 * </STRONG>
 */
public class PhysicalPlanTranslator extends DefaultPlanVisitor<PlanFragment, PlanTranslatorContext> {
    /**
     * Translate Nereids Physical Plan tree to Stale Planner PlanFragment tree.
     *
     * @param physicalPlan Nereids Physical Plan tree
     * @param context context to help translate
     * @return Stale Planner PlanFragment tree
     */
    public PlanFragment translatePlan(PhysicalPlan physicalPlan, PlanTranslatorContext context) {
        PlanFragment rootFragment = physicalPlan.accept(this, context);
        if (rootFragment.isPartitioned() && rootFragment.getPlanRoot().getNumInstances() > 1) {
            rootFragment = exchangeToMergeFragment(rootFragment, context);
        }
        List<Expr> outputExprs = Lists.newArrayList();
        physicalPlan.getOutput().stream().map(Slot::getExprId)
                .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
        rootFragment.setOutputExprs(outputExprs);
        rootFragment.getPlanRoot().convertToVectoriezd();
        for (PlanFragment fragment : context.getPlanFragments()) {
            fragment.finalize(null);
        }
        Collections.reverse(context.getPlanFragments());
        context.getDescTable().computeMemLayout();
        return rootFragment;
    }


    /**
     * Translate Agg.
     * todo: support DISTINCT
     */
    @Override
    public PlanFragment visitPhysicalAggregate(
            PhysicalAggregate<? extends Plan> aggregate,
            PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = aggregate.child(0).accept(this, context);

        // TODO: stale planner generate aggregate tuple in a special way. tuple include 2 parts:
        //    1. group by expressions: removing duplicate expressions add to tuple
        //    2. agg functions: only removing duplicate agg functions in output expression should appear in tuple.
        //       e.g. select sum(v1) + 1, sum(v1) + 2 from t1 should only generate one sum(v1) in tuple
        //    We need:
        //    1. add a project after agg, if agg function is not the top output expression.(Done)
        //    2. introduce canonicalized, semanticEquals and deterministic in Expression
        //       for removing duplicate.
        List<Expression> groupByExpressionList = aggregate.getGroupByExpressions();
        List<NamedExpression> outputExpressionList = aggregate.getOutputExpressions();

        // 1. generate slot reference for each group expression
        List<SlotReference> groupSlotList = Lists.newArrayList();
        for (Expression e : groupByExpressionList) {
            if (e instanceof SlotReference && outputExpressionList.stream().anyMatch(o -> o.anyMatch(e::equals))) {
                groupSlotList.add((SlotReference) e);
            } else {
                groupSlotList.add(new SlotReference(e.toSql(), e.getDataType(), e.nullable(), Collections.emptyList()));
            }
        }
        ArrayList<Expr> execGroupingExpressions = groupByExpressionList.stream()
                .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toCollection(ArrayList::new));
        // 2. collect agg functions and generate agg function to slot reference map
        List<Slot> aggFunctionOutput = Lists.newArrayList();
        List<AggregateFunction> aggregateFunctionList = outputExpressionList.stream()
                .filter(o -> o.anyMatch(AggregateFunction.class::isInstance))
                .peek(o -> aggFunctionOutput.add(o.toSlot()))
                .map(o -> o.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toList());
        ArrayList<FunctionCallExpr> execAggregateFunctions = aggregateFunctionList.stream()
                .map(x -> (FunctionCallExpr) ExpressionTranslator.translate(x, context))
                .collect(Collectors.toCollection(ArrayList::new));

        // process partition list
        List<Expression> partitionExpressionList = aggregate.getPartitionExpressions();
        List<Expr> execPartitionExpressions = partitionExpressionList.stream()
                .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());
        DataPartition mergePartition = DataPartition.UNPARTITIONED;
        if (CollectionUtils.isNotEmpty(execPartitionExpressions)) {
            mergePartition = DataPartition.hashPartitioned(execPartitionExpressions);
        }

        // 3. generate output tuple
        List<Slot> slotList = Lists.newArrayList();
        TupleDescriptor outputTupleDesc;
        if (aggregate.getAggPhase() == AggPhase.LOCAL
                || (aggregate.getAggPhase() == AggPhase.GLOBAL && aggregate.isFinalPhase())
                || aggregate.getAggPhase() == AggPhase.DISTINCT_LOCAL) {
            slotList.addAll(groupSlotList);
            slotList.addAll(aggFunctionOutput);
            outputTupleDesc = generateTupleDesc(slotList, null, context);
        } else {
            // In the distinct agg scenario, global shares local's desc
            AggregationNode localAggNode = (AggregationNode) inputPlanFragment.getPlanRoot().getChild(0);
            outputTupleDesc = localAggNode.getAggInfo().getOutputTupleDesc();
        }

        if (aggregate.getAggPhase() == AggPhase.GLOBAL) {
            for (FunctionCallExpr execAggregateFunction : execAggregateFunctions) {
                execAggregateFunction.setMergeForNereids(true);
            }
        }
        if (aggregate.getAggPhase() == AggPhase.DISTINCT_LOCAL) {
            for (FunctionCallExpr execAggregateFunction : execAggregateFunctions) {
                if (!execAggregateFunction.isDistinct()) {
                    execAggregateFunction.setMergeForNereids(true);
                }
            }
        }
        AggregateInfo aggInfo = AggregateInfo.create(execGroupingExpressions, execAggregateFunctions, outputTupleDesc,
                outputTupleDesc, aggregate.getAggPhase().toExec());
        AggregationNode aggregationNode = new AggregationNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), aggInfo);
        if (!aggregate.isFinalPhase()) {
            aggregationNode.unsetNeedsFinalize();
        }
        PlanFragment currentFragment = inputPlanFragment;
        switch (aggregate.getAggPhase()) {
            case LOCAL:
                aggregationNode.setUseStreamingPreagg(aggregate.isUsingStream());
                aggregationNode.setIntermediateTuple();
                break;
            case GLOBAL:
            case DISTINCT_LOCAL:
                if (currentFragment.getPlanRoot() instanceof ExchangeNode) {
                    ExchangeNode exchangeNode = (ExchangeNode) currentFragment.getPlanRoot();
                    currentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, mergePartition);
                    inputPlanFragment.setOutputPartition(mergePartition);
                    inputPlanFragment.setPlanRoot(exchangeNode.getChild(0));
                    inputPlanFragment.setDestination(exchangeNode);
                    context.addPlanFragment(currentFragment);
                }
                currentFragment.updateDataPartition(mergePartition);
                break;
            default:
                throw new RuntimeException("Unsupported yet");
        }
        currentFragment.setPlanRoot(aggregationNode);
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, PlanTranslatorContext context) {
        List<Slot> output = emptyRelation.getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(output, null, context);
        for (int i = 0; i < output.size(); i++) {
            SlotDescriptor slotDescriptor = tupleDescriptor.getSlots().get(i);
            slotDescriptor.setIsNullable(true); // we should set to nullable, or else BE would core

            Slot slot = output.get(i);
            SlotRef slotRef = context.findSlotRef(slot.getExprId());
            slotRef.setLabel(slot.getName());
        }

        ArrayList<TupleId> tupleIds = new ArrayList();
        tupleIds.add(tupleDescriptor.getId());
        EmptySetNode emptySetNode = new EmptySetNode(context.nextPlanNodeId(), tupleIds);

        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), emptySetNode,
                DataPartition.UNPARTITIONED);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation,
            PlanTranslatorContext context) {
        List<Slot> slots = oneRowRelation.getLogicalProperties().getOutput();
        TupleDescriptor oneRowTuple = generateTupleDesc(slots, null, context);

        List<Expr> legacyExprs = oneRowRelation.getProjects()
                .stream()
                .map(expr -> ExpressionTranslator.translate(expr, context))
                .collect(Collectors.toList());

        for (int i = 0; i < legacyExprs.size(); i++) {
            SlotDescriptor slotDescriptor = oneRowTuple.getSlots().get(i);
            Expr expr = legacyExprs.get(i);
            slotDescriptor.setSourceExpr(expr);
            slotDescriptor.setIsNullable(true); // we should set to nullable, or else BE would core
        }

        UnionNode unionNode = new UnionNode(context.nextPlanNodeId(), oneRowTuple.getId());
        unionNode.addConstExprList(legacyExprs);
        unionNode.finalizeForNereids(oneRowTuple, oneRowTuple.getSlots());

        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), unionNode, DataPartition.UNPARTITIONED);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        // Create OlapScanNode
        List<Slot> slotList = olapScan.getOutput();
        OlapTable olapTable = olapScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, olapTable, context);
        tupleDescriptor.setTable(olapTable);
        OlapScanNode olapScanNode = new OlapScanNode(context.nextPlanNodeId(), tupleDescriptor, "OlapScanNode");
        // TODO: Do we really need tableName here?
        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, olapTable, tableName);
        tupleDescriptor.setRef(tableRef);
        olapScanNode.setSelectedPartitionIds(olapScan.getSelectedPartitionIds());

        // TODO: Unify the logic here for all the table types once aggregate/unique key types are fully supported.
        switch (olapScan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                // TODO: Improve complete info for aggregate and unique key types table.
                PreAggStatus preAgg = olapScan.getPreAggStatus();
                olapScanNode.setSelectedIndexInfo(olapScan.getSelectedIndexId(), preAgg.isOn(), preAgg.getOffReason());
                break;
            case DUP_KEYS:
                try {
                    olapScanNode.updateScanRangeInfoByNewMVSelector(olapScan.getSelectedIndexId(), true, "");
                    olapScanNode.setIsPreAggregation(true, "");
                } catch (Exception e) {
                    throw new AnalysisException(e.getMessage());
                }
                break;
            default:
                throw new RuntimeException("Not supported key type: " + olapScan.getTable().getKeysType());
        }

        Utils.execWithUncheckedException(olapScanNode::init);
        context.addScanNode(olapScanNode);
        // translate runtime filter
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(olapScan.getId()).forEach(
                        expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, olapScanNode, context)
                )
        );
        // Create PlanFragment
        DataPartition dataPartition = DataPartition.RANDOM;
        if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) olapScan.getDistributionSpec();
            List<Expr> partitionExprs = distributionSpecHash.getOrderedShuffledColumns().stream()
                    .map(context::findSlotRef).collect(Collectors.toList());
            dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
        }
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), olapScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    /*-
     * Physical sort:
     * 1. Build sortInfo
     *    There are two types of slotRef:
     *    one is generated by the previous node, collectively called old.
     *    the other is newly generated by the sort node, collectively called new.
     *    Filling of sortInfo related data structures,
     *    a. ordering use newSlotRef.
     *    b. sortTupleSlotExprs use oldSlotRef.
     * 2. Create sortNode
     * 3. Create mergeFragment
     * TODO: When the slotRef of sort is currently generated,
     *       it will be based on the expression in select and orderBy expression in to ensure the uniqueness of slotRef.
     *       But eg:
     *       select a+1 from table order by a+1;
     *       the expressions of the two are inconsistent.
     *       The former will perform an additional Alias.
     *       Currently we cannot test whether this will have any effect.
     *       After a+1 can be parsed , reprocessing.
     */
    @Override
    public PlanFragment visitLogicalSort(LogicalSort<? extends Plan> sort, PlanTranslatorContext context) {
        return super.visitLogicalSort(sort, context);
    }

    @Override
    public PlanFragment visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort,
            PlanTranslatorContext context) {
        PlanFragment childFragment = visitAbstractPhysicalSort(sort, context);
        SortNode sortNode = (SortNode) childFragment.getPlanRoot();
        // isPartitioned() == false means there is only one instance, so no merge phase
        if (!childFragment.isPartitioned()) {
            return childFragment;
        }
        PlanFragment mergeFragment = createParentFragment(childFragment, DataPartition.UNPARTITIONED, context);
        ExchangeNode exchangeNode = (ExchangeNode) mergeFragment.getPlanRoot();
        // exchangeNode.limit/offset will be set in when translating  PhysicalLimit
        exchangeNode.setMergeInfo(sortNode.getSortInfo());
        return mergeFragment;
    }

    @Override
    public PlanFragment visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanTranslatorContext context) {
        PlanFragment childFragment = visitAbstractPhysicalSort(topN, context);
        SortNode sortNode = (SortNode) childFragment.getPlanRoot();
        sortNode.setOffset(topN.getOffset());
        sortNode.setLimit(topN.getLimit());
        // isPartitioned() == false means there is only one instance, so no merge phase
        if (!childFragment.isPartitioned()) {
            return childFragment;
        }
        PlanFragment mergeFragment = createParentFragment(childFragment, DataPartition.UNPARTITIONED, context);
        ExchangeNode exchangeNode = (ExchangeNode) mergeFragment.getPlanRoot();
        exchangeNode.setMergeInfo(sortNode.getSortInfo());
        exchangeNode.setOffset(topN.getOffset());
        exchangeNode.setLimit(topN.getLimit());
        return mergeFragment;
    }

    @Override
    public PlanFragment visitAbstractPhysicalSort(
            AbstractPhysicalSort<? extends Plan> sort,
            PlanTranslatorContext context) {
        PlanFragment childFragment = sort.child(0).accept(this, context);
        List<Expr> oldOrderingExprList = Lists.newArrayList();
        List<Boolean> ascOrderList = Lists.newArrayList();
        List<Boolean> nullsFirstParamList = Lists.newArrayList();
        List<OrderKey> orderKeyList = sort.getOrderKeys();
        // 1.Get previous slotRef
        orderKeyList.forEach(k -> {
            oldOrderingExprList.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrderList.add(k.isAsc());
            nullsFirstParamList.add(k.isNullFirst());
        });
        List<Expr> sortTupleOutputList = new ArrayList<>();
        List<Slot> outputList = sort.getOutput();
        outputList.forEach(k -> {
            sortTupleOutputList.add(ExpressionTranslator.translate(k, context));
        });
        // 2. Generate new Tuple
        TupleDescriptor tupleDesc = generateTupleDesc(outputList, orderKeyList, context, null);
        // 3. Get current slotRef
        List<Expr> newOrderingExprList = Lists.newArrayList();
        orderKeyList.forEach(k -> {
            newOrderingExprList.add(ExpressionTranslator.translate(k.getExpr(), context));
        });
        // 4. fill in SortInfo members
        SortInfo sortInfo = new SortInfo(newOrderingExprList, ascOrderList, nullsFirstParamList, tupleDesc);
        PlanNode childNode = childFragment.getPlanRoot();
        SortNode sortNode = new SortNode(context.nextPlanNodeId(), childNode, sortInfo, true);
        sortNode.finalizeForNereids(tupleDesc, sortTupleOutputList, oldOrderingExprList);
        childFragment.addPlanRoot(sortNode);
        return childFragment;
    }

    /**
     * the contract of hash join node with BE
     * 1. hash join contains 3 types of predicates:
     *   a. equal join conjuncts
     *   b. other join conjuncts
     *   c. other predicates (denoted by filter conjuncts in the rest of comments)
     *
     * 2. hash join contains 3 tuple descriptors
     *   a. input tuple descriptors, corresponding to the left child output and right child output.
     *      If its column is selected, it will be displayed in explain by `tuple ids`.
     *      for example, select L.* from L join R on ..., because no column from R are selected, tuple ids only
     *      contains output tuple of L.
     *      equal join conjuncts is bound on input tuple descriptors.
     *
     *   b.intermediate tuple.
     *      This tuple describes schema of the output block after evaluating equal join conjuncts
     *      and other join conjuncts.
     *
     *      Other join conjuncts currently is bound on intermediate tuple. There are some historical reason, and it
     *      should be bound on input tuple in the future.
     *
     *      filter conjuncts will be evaluated on the intermediate tuple. That means the input block of filter is
     *      described by intermediate tuple, and hence filter conjuncts should be bound on intermediate tuple.
     *
     *      In order to be compatible with old version, intermediate tuple is not pruned. For example, intermediate
     *      tuple contains all slots from both sides of children. After probing hash-table, BE does not need to
     *      materialize all slots in intermediate tuple. The slots in HashJoinNode.hashOutputSlotIds will be
     *      materialized by BE. If `hashOutputSlotIds` is empty, all slots will be materialized.
     *
     *      In case of outer join, the slots in intermediate should be set nullable.
     *      For example,
     *      select L.*, R.* from L left outer join R on ...
     *      All slots from R in intermediate tuple should be nullable.
     *
     *   c. output tuple
     *      This describes the schema of hash join output block.
     * 3. Intermediate tuple
     *      for BE performance reason, the slots in intermediate tuple depends on the join type and other join conjucts.
     *      In general, intermediate tuple contains all slots of both children, except one case.
     *      For left-semi/left-ant (right-semi/right-semi) join without other join conjuncts, intermediate tuple
     *      only contains left (right) children output slots.
     *
     */
    // TODO: 1. support shuffle join / co-locate / bucket shuffle join later
    @Override
    public PlanFragment visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            PlanTranslatorContext context) {
        Preconditions.checkArgument(hashJoin.left() instanceof PhysicalPlan,
                "HashJoin's left child should be PhysicalPlan");
        Preconditions.checkArgument(hashJoin.right() instanceof PhysicalPlan,
                "HashJoin's left child should be PhysicalPlan");
        PhysicalHashJoin<PhysicalPlan, PhysicalPlan> physicalHashJoin
                = (PhysicalHashJoin<PhysicalPlan, PhysicalPlan>) hashJoin;
        // NOTICE: We must visit from right to left, to ensure the last fragment is root fragment
        PlanFragment rightFragment = hashJoin.child(1).accept(this, context);
        PlanFragment leftFragment = hashJoin.child(0).accept(this, context);

        if (JoinUtils.shouldNestedLoopJoin(hashJoin)) {
            throw new RuntimeException("Physical hash join could not execute without equal join condition.");
        }

        PlanNode leftPlanRoot = leftFragment.getPlanRoot();
        PlanNode rightPlanRoot = rightFragment.getPlanRoot();
        JoinType joinType = hashJoin.getJoinType();

        List<Expr> execEqConjuncts = hashJoin.getHashJoinConjuncts().stream()
                .map(EqualTo.class::cast)
                .map(e -> JoinUtils.swapEqualToForChildrenOrder(e, hashJoin.left().getOutputSet()))
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        HashJoinNode hashJoinNode = new HashJoinNode(context.nextPlanNodeId(), leftPlanRoot,
                rightPlanRoot, JoinType.toJoinOperator(joinType), execEqConjuncts, Lists.newArrayList(),
                null, null, null);

        PlanFragment currentFragment;
        if (JoinUtils.shouldColocateJoin(physicalHashJoin)) {
            currentFragment = constructColocateJoin(hashJoinNode, leftFragment, rightFragment, context);
        } else if (JoinUtils.shouldBucketShuffleJoin(physicalHashJoin)) {
            currentFragment = constructBucketShuffleJoin(
                    physicalHashJoin, hashJoinNode, leftFragment, rightFragment, context);
        } else if (JoinUtils.shouldBroadcastJoin(physicalHashJoin)) {
            currentFragment = constructBroadcastJoin(hashJoinNode, leftFragment, rightFragment, context);
        } else {
            currentFragment = constructShuffleJoin(
                    physicalHashJoin, hashJoinNode, leftFragment, rightFragment, context);
        }
        // Nereids does not care about output order of join,
        // but BE need left child's output must be before right child's output.
        // So we need to swap the output order of left and right child if necessary.
        // TODO: revert this after Nereids could ensure the output order is correct.
        List<TupleDescriptor> leftTuples = context.getTupleDesc(leftPlanRoot);
        List<SlotDescriptor> leftSlotDescriptors = leftTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<TupleDescriptor> rightTuples = context.getTupleDesc(rightPlanRoot);
        List<SlotDescriptor> rightSlotDescriptors = rightTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Map<ExprId, SlotReference> outputSlotReferenceMap = Maps.newHashMap();

        hashJoin.getOutput().stream()
                .map(SlotReference.class::cast)
                .forEach(s -> outputSlotReferenceMap.put(s.getExprId(), s));
        List<SlotReference> outputSlotReferences = Stream.concat(leftTuples.stream(), rightTuples.stream())
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .map(sd -> context.findExprId(sd.getId()))
                .map(outputSlotReferenceMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        Map<ExprId, SlotReference> hashOutputSlotReferenceMap = Maps.newHashMap(outputSlotReferenceMap);

        hashJoin.getOtherJoinConjuncts()
                .stream()
                .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                .flatMap(e -> e.getInputSlots().stream())
                .map(SlotReference.class::cast)
                .forEach(s -> hashOutputSlotReferenceMap.put(s.getExprId(), s));
        hashJoin.getFilterConjuncts().stream()
                .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                .flatMap(e -> e.getInputSlots().stream())
                .map(SlotReference.class::cast)
                .forEach(s -> hashOutputSlotReferenceMap.put(s.getExprId(), s));

        Map<ExprId, SlotReference> leftChildOutputMap = Maps.newHashMap();
        hashJoin.child(0).getOutput().stream()
                .map(SlotReference.class::cast)
                .forEach(s -> leftChildOutputMap.put(s.getExprId(), s));
        Map<ExprId, SlotReference> rightChildOutputMap = Maps.newHashMap();
        hashJoin.child(1).getOutput().stream()
                .map(SlotReference.class::cast)
                .forEach(s -> rightChildOutputMap.put(s.getExprId(), s));

        // make intermediate tuple
        List<SlotDescriptor> leftIntermediateSlotDescriptor = Lists.newArrayList();
        List<SlotDescriptor> rightIntermediateSlotDescriptor = Lists.newArrayList();
        TupleDescriptor intermediateDescriptor = context.generateTupleDesc();

        if (hashJoin.getOtherJoinConjuncts().isEmpty()
                && (joinType == JoinType.LEFT_ANTI_JOIN || joinType == JoinType.LEFT_SEMI_JOIN)) {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                leftIntermediateSlotDescriptor.add(sd);
            }
        } else if (hashJoin.getOtherJoinConjuncts().isEmpty()
                && (joinType == JoinType.RIGHT_ANTI_JOIN || joinType == JoinType.RIGHT_SEMI_JOIN)) {
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                rightIntermediateSlotDescriptor.add(sd);
            }
        } else {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                    hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                    hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        }

        // set slots as nullable for outer join
        if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            rightIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }
        if (joinType == JoinType.RIGHT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            leftIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }

        List<Expr> otherJoinConjuncts = hashJoin.getOtherJoinConjuncts()
                .stream()
                // TODO add constant expr will cause be crash, currently we only handle true literal.
                //  remove it after Nereids could ensure no constant expr in other join condition
                .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        hashJoin.getFilterConjuncts().stream()
                .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(hashJoinNode::addConjunct);
        // translate runtime filter
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> runtimeFilterTranslator
                .getRuntimeFilterOfHashJoinNode(physicalHashJoin)
                .forEach(filter -> runtimeFilterTranslator.createLegacyRuntimeFilter(filter, hashJoinNode, context)));

        hashJoinNode.setOtherJoinConjuncts(otherJoinConjuncts);

        hashJoinNode.setvIntermediateTupleDescList(Lists.newArrayList(intermediateDescriptor));

        if (hashJoin.isShouldTranslateOutput()) {
            // translate output expr on intermediate tuple
            List<Expr> srcToOutput = outputSlotReferences.stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .collect(Collectors.toList());

            TupleDescriptor outputDescriptor = context.generateTupleDesc();
            outputSlotReferences.forEach(s -> context.createSlotDesc(outputDescriptor, s));

            hashJoinNode.setvOutputTupleDesc(outputDescriptor);
            hashJoinNode.setvSrcToOutputSMap(srcToOutput);
        }
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanTranslatorContext context) {
        // NOTICE: We must visit from right to left, to ensure the last fragment is root fragment
        // TODO: we should add a helper method to wrap this logic.
        //   Maybe something like private List<PlanFragment> postOrderVisitChildren(
        //       PhysicalPlan plan, PlanVisitor visitor, Context context).
        PlanFragment rightFragment = nestedLoopJoin.child(1).accept(this, context);
        PlanFragment leftFragment = nestedLoopJoin.child(0).accept(this, context);
        PlanNode leftFragmentPlanRoot = leftFragment.getPlanRoot();
        PlanNode rightFragmentPlanRoot = rightFragment.getPlanRoot();
        if (JoinUtils.shouldNestedLoopJoin(nestedLoopJoin)) {
            List<TupleDescriptor> leftTuples = context.getTupleDesc(leftFragmentPlanRoot);
            List<TupleDescriptor> rightTuples = context.getTupleDesc(rightFragmentPlanRoot);
            List<TupleId> tupleIds = Stream.concat(leftTuples.stream(), rightTuples.stream())
                    .map(TupleDescriptor::getId)
                    .collect(Collectors.toList());

            CrossJoinNode crossJoinNode = new CrossJoinNode(context.nextPlanNodeId(),
                    leftFragmentPlanRoot, rightFragmentPlanRoot, tupleIds);
            rightFragment.getPlanRoot().setCompactData(false);
            crossJoinNode.setChild(0, leftFragment.getPlanRoot());
            connectChildFragment(crossJoinNode, 1, leftFragment, rightFragment, context);
            leftFragment.setPlanRoot(crossJoinNode);
            nestedLoopJoin.getOtherJoinConjuncts().stream()
                    .map(e -> ExpressionTranslator.translate(e, context)).forEach(crossJoinNode::addConjunct);

            return leftFragment;
        } else {
            throw new RuntimeException("Physical nested loop join could not execute with equal join condition.");
        }
    }

    // TODO: generate expression mapping when be project could do in ExecNode.
    @Override
    public PlanFragment visitPhysicalProject(PhysicalProject<? extends Plan> project, PlanTranslatorContext context) {
        if (project.child(0) instanceof PhysicalHashJoin) {
            ((PhysicalHashJoin<?, ?>) project.child(0)).setShouldTranslateOutput(false);
        }
        if (project.child(0) instanceof PhysicalFilter) {
            if (project.child(0).child(0) instanceof PhysicalHashJoin) {
                ((PhysicalHashJoin<?, ?>) project.child(0).child(0)).setShouldTranslateOutput(false);
            }
        }
        PlanFragment inputFragment = project.child(0).accept(this, context);

        List<Expr> execExprList = project.getProjects()
                .stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        // TODO: fix the project alias of an aliased relation.
        List<Slot> slotList = project.getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, null, context);
        PlanNode inputPlanNode = inputFragment.getPlanRoot();
        // For hash join node, use vSrcToOutputSMap to describe the expression calculation, use
        // vIntermediateTupleDescList as input, and set vOutputTupleDesc as the final output.
        // TODO: HashJoinNode's be implementation is not support projection yet, remove this after when supported.
        if (inputPlanNode instanceof HashJoinNode) {
            HashJoinNode hashJoinNode = (HashJoinNode) inputPlanNode;
            hashJoinNode.setvOutputTupleDesc(tupleDescriptor);
            hashJoinNode.setvSrcToOutputSMap(execExprList);
            return inputFragment;
        }
        inputPlanNode.setProjectList(execExprList);
        inputPlanNode.setOutputTupleDesc(tupleDescriptor);

        List<Expr> predicateList = inputPlanNode.getConjuncts();
        Set<Integer> requiredSlotIdList = new HashSet<>();
        for (Expr expr : predicateList) {
            extractExecSlot(expr, requiredSlotIdList);
        }
        for (Expr expr : execExprList) {
            extractExecSlot(expr, requiredSlotIdList);
        }
        if (inputPlanNode instanceof OlapScanNode) {
            updateChildSlotsMaterialization(inputPlanNode, requiredSlotIdList, context);
        }
        return inputFragment;
    }

    private void updateChildSlotsMaterialization(PlanNode execPlan,
            Set<Integer> requiredSlotIdList,
            PlanTranslatorContext context) {
        Set<SlotRef> slotRefSet = new HashSet<>();
        for (Expr expr : execPlan.getConjuncts()) {
            expr.collect(SlotRef.class, slotRefSet);
        }
        Set<Integer> slotIdSet = slotRefSet.stream()
                .map(SlotRef::getSlotId).map(SlotId::asInt).collect(Collectors.toSet());
        slotIdSet.addAll(requiredSlotIdList);
        boolean noneMaterialized = execPlan.getTupleIds().stream()
                .map(context::getTupleDesc)
                .map(TupleDescriptor::getSlots)
                .flatMap(List::stream)
                .peek(s -> s.setIsMaterialized(slotIdSet.contains(s.getId().asInt())))
                .filter(SlotDescriptor::isMaterialized)
                .count() == 0;
        if (noneMaterialized) {
            context.getDescTable()
                    .getTupleDesc(execPlan.getTupleIds().get(0)).getSlots().get(0).setIsMaterialized(true);
        }
    }

    @Override
    public PlanFragment visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanTranslatorContext context) {
        if (filter.child(0) instanceof PhysicalHashJoin) {
            PhysicalHashJoin join = (PhysicalHashJoin<?, ?>) filter.child(0);
            join.getFilterConjuncts().addAll(ExpressionUtils.extractConjunction(filter.getPredicates()));
        }
        PlanFragment inputFragment = filter.child(0).accept(this, context);
        PlanNode planNode = inputFragment.getPlanRoot();
        if (!(filter.child(0) instanceof PhysicalHashJoin)) {
            addConjunctsToPlanNode(filter, planNode, context);
        }
        return inputFragment;
    }

    private void addConjunctsToPlanNode(PhysicalFilter<? extends Plan> filter,
            PlanNode planNode,
            PlanTranslatorContext context) {
        Expression expression = filter.getPredicates();
        List<Expression> expressionList = ExpressionUtils.extractConjunction(expression);
        expressionList.stream().map(e -> ExpressionTranslator.translate(e, context)).forEach(planNode::addConjunct);
    }

    @Override
    public PlanFragment visitPhysicalLimit(PhysicalLimit<? extends Plan> physicalLimit, PlanTranslatorContext context) {
        PlanFragment inputFragment = physicalLimit.child(0).accept(this, context);
        PlanNode child = inputFragment.getPlanRoot();

        // physical plan:  limit --> sort
        // after translate, it could be:
        // 1. limit->sort => set (limit and offset) on sort
        // 2. limit->exchange->sort => set (limit and offset) on exchange, set sort.limit = limit+offset
        if (child instanceof SortNode) {
            SortNode sort = (SortNode) child;
            sort.setLimit(physicalLimit.getLimit());
            sort.setOffset(physicalLimit.getOffset());
            return inputFragment;
        }
        if (child instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) child;
            exchangeNode.setLimit(physicalLimit.getLimit());
            // we do not check if this is a merging exchange here,
            // since this guaranteed by translating logic plan to physical plan
            exchangeNode.setOffset(physicalLimit.getOffset());
            if (exchangeNode.getChild(0) instanceof SortNode) {
                SortNode sort = (SortNode) exchangeNode.getChild(0);
                sort.setLimit(physicalLimit.getLimit() + physicalLimit.getOffset());
                sort.setOffset(0);
            }
            return inputFragment;
        }
        // for other PlanNode, just set limit as limit+offset
        child.setLimit(physicalLimit.getLimit() + physicalLimit.getOffset());
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            PlanTranslatorContext context) {
        PlanFragment childFragment = distribute.child().accept(this, context);
        ExchangeNode exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot(), false);
        exchange.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.setPlanRoot(exchange);
        return childFragment;
    }

    @Override
    public PlanFragment visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = assertNumRows.child(0).accept(this, context);
        // create assertNode
        AssertNumRowsNode assertNumRowsNode = new AssertNumRowsNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(),
                ExpressionTranslator.translateAssert(assertNumRows.getAssertNumRowsElement()));
        if (currentFragment.getPlanRoot() instanceof ExchangeNode) {
            currentFragment.setPlanRoot(currentFragment.getPlanRoot().getChild(0));
            currentFragment = createParentFragment(currentFragment, DataPartition.UNPARTITIONED, context);
        }
        currentFragment.addPlanRoot(assertNumRowsNode);
        return currentFragment;
    }

    private void extractExecSlot(Expr root, Set<Integer> slotRefList) {
        if (root instanceof SlotRef) {
            slotRefList.add(((SlotRef) root).getDesc().getId().asInt());
            return;
        }
        for (Expr child : root.getChildren()) {
            extractExecSlot(child, slotRefList);
        }
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, Table table, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot);
        }
        return tupleDescriptor;
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, List<OrderKey> orderKeyList,
            PlanTranslatorContext context, Table table) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        Set<ExprId> alreadyExists = Sets.newHashSet();
        for (OrderKey orderKey : orderKeyList) {
            if (orderKey.getExpr() instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) orderKey.getExpr();
                // TODO: trick here, we need semanticEquals to remove redundant expression
                if (alreadyExists.contains(slotReference.getExprId())) {
                    continue;
                }
                context.createSlotDesc(tupleDescriptor, (SlotReference) orderKey.getExpr());
                alreadyExists.add(slotReference.getExprId());
            }
        }
        for (Slot slot : slotList) {
            if (alreadyExists.contains(slot.getExprId())) {
                continue;
            }
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot);
            alreadyExists.add(slot.getExprId());
        }

        return tupleDescriptor;
    }

    private PlanFragment createParentFragment(PlanFragment childFragment, DataPartition parentPartition,
            PlanTranslatorContext context) {
        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        PlanFragment parentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, parentPartition);
        childFragment.setDestination(exchangeNode);
        childFragment.setOutputPartition(parentPartition);
        context.addPlanFragment(parentFragment);
        return parentFragment;
    }

    private void connectChildFragment(PlanNode parent, int childIdx,
            PlanFragment parentFragment, PlanFragment childFragment,
            PlanTranslatorContext context) {
        PlanNode exchange = parent.getChild(childIdx);
        if (!(exchange instanceof ExchangeNode)) {
            exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot(), false);
            exchange.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        }
        childFragment.setPlanRoot(exchange.getChild(0));
        exchange.setFragment(parentFragment);
        parent.setChild(childIdx, exchange);
        childFragment.setDestination((ExchangeNode) exchange);
    }

    /**
     * Return unpartitioned fragment that merges the input fragment's output via
     * an ExchangeNode.
     * Requires that input fragment be partitioned.
     */
    private PlanFragment exchangeToMergeFragment(PlanFragment inputFragment, PlanTranslatorContext context) {
        Preconditions.checkState(inputFragment.isPartitioned());

        // exchange node clones the behavior of its input, aside from the conjuncts
        ExchangeNode mergePlan =
                new ExchangeNode(context.nextPlanNodeId(), inputFragment.getPlanRoot(), false);
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        PlanFragment fragment = new PlanFragment(context.nextFragmentId(), mergePlan, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(mergePlan);
        context.addPlanFragment(fragment);
        return fragment;
    }

    private PlanFragment constructColocateJoin(HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context) {
        // TODO: add reason
        hashJoinNode.setColocate(true, "");
        hashJoinNode.setChild(0, leftFragment.getPlanRoot());
        hashJoinNode.setChild(1, rightFragment.getPlanRoot());
        leftFragment.setPlanRoot(hashJoinNode);
        context.removePlanFragment(rightFragment);
        leftFragment.setHasColocatePlanNode(true);
        return leftFragment;
    }

    private PlanFragment constructBucketShuffleJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> physicalHashJoin,
            HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context) {
        // according to left partition to generate right partition expr list
        DistributionSpecHash leftDistributionSpec
                = (DistributionSpecHash) physicalHashJoin.left().getPhysicalProperties().getDistributionSpec();
        Pair<List<ExprId>, List<ExprId>> onClauseUsedSlots = JoinUtils.getOnClauseUsedSlots(physicalHashJoin);
        List<ExprId> rightPartitionExprIds = Lists.newArrayList(onClauseUsedSlots.second);
        for (int i = 0; i < onClauseUsedSlots.first.size(); i++) {
            int idx = leftDistributionSpec.getExprIdToEquivalenceSet().get(onClauseUsedSlots.first.get(i));
            rightPartitionExprIds.set(idx, onClauseUsedSlots.second.get(i));
        }
        // assemble fragment
        hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.BUCKET_SHUFFLE);
        if (leftDistributionSpec.getShuffleType() != ShuffleType.NATURAL) {
            hashJoinNode.setDistributionMode(DistributionMode.PARTITIONED);
        }
        connectChildFragment(hashJoinNode, 1, leftFragment, rightFragment, context);
        leftFragment.setPlanRoot(hashJoinNode);
        TPartitionType partitionType = TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED;
        if (leftDistributionSpec.getShuffleType() != ShuffleType.NATURAL) {
            partitionType = TPartitionType.HASH_PARTITIONED;
        }
        DataPartition rhsJoinPartition = new DataPartition(partitionType,
                rightPartitionExprIds.stream().map(context::findSlotRef).collect(Collectors.toList()));
        rightFragment.setOutputPartition(rhsJoinPartition);

        return leftFragment;
    }

    private PlanFragment constructBroadcastJoin(HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context) {
        hashJoinNode.setDistributionMode(DistributionMode.BROADCAST);
        leftFragment.setPlanRoot(hashJoinNode);
        connectChildFragment(hashJoinNode, 1, leftFragment, rightFragment, context);
        return leftFragment;
    }

    private PlanFragment constructShuffleJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> physicalHashJoin,
            HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context) {
        hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);
        // TODO should according nereids distribute indicate
        // first, extract join exprs
        List<BinaryPredicate> eqJoinConjuncts = hashJoinNode.getEqJoinConjuncts();
        List<Expr> lhsJoinExprs = Lists.newArrayList();
        List<Expr> rhsJoinExprs = Lists.newArrayList();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            // no remapping necessary
            lhsJoinExprs.add(eqJoinPredicate.getChild(0).clone(null));
            rhsJoinExprs.add(eqJoinPredicate.getChild(1).clone(null));
        }

        // create the parent fragment containing the HashJoin node
        DataPartition lhsJoinPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
                Expr.cloneList(lhsJoinExprs, null));
        DataPartition rhsJoinPartition =
                new DataPartition(TPartitionType.HASH_PARTITIONED, rhsJoinExprs);
        PlanFragment joinFragment = new PlanFragment(context.nextFragmentId(), hashJoinNode, lhsJoinPartition);
        context.addPlanFragment(joinFragment);

        connectChildFragment(hashJoinNode, 0, joinFragment, leftFragment, context);
        connectChildFragment(hashJoinNode, 1, joinFragment, rightFragment, context);

        leftFragment.setOutputPartition(lhsJoinPartition);
        rightFragment.setOutputPartition(rhsJoinPartition);

        return joinFragment;
    }
}
