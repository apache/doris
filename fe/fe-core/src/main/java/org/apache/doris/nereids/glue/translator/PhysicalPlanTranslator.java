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
import org.apache.doris.analysis.GroupByClause.GroupingType;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.NestedLoopJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RepeatNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.TableFunctionNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlanTranslator.class);

    /**
     * Translate Nereids Physical Plan tree to Stale Planner PlanFragment tree.
     *
     * @param physicalPlan Nereids Physical Plan tree
     * @param context context to help translate
     * @return Stale Planner PlanFragment tree
     */
    public PlanFragment translatePlan(PhysicalPlan physicalPlan, PlanTranslatorContext context) {
        PlanFragment rootFragment = physicalPlan.accept(this, context);
        if (physicalPlan instanceof PhysicalDistribute) {
            PhysicalDistribute distribute = (PhysicalDistribute) physicalPlan;
            DataPartition dataPartition;
            if (distribute.getPhysicalProperties().equals(PhysicalProperties.GATHER)) {
                dataPartition = DataPartition.UNPARTITIONED;
            } else {
                throw new AnalysisException("Unsupported PhysicalDistribute in the root plan: " + distribute);
            }

            ExchangeNode exchangeNode = (ExchangeNode) rootFragment.getPlanRoot();
            PlanFragment currentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, dataPartition);
            rootFragment.setOutputPartition(dataPartition);
            rootFragment.setPlanRoot(exchangeNode.getChild(0));
            rootFragment.setDestination(exchangeNode);
            context.addPlanFragment(currentFragment);
            rootFragment = currentFragment;
        }
        if (rootFragment.isPartitioned() && rootFragment.getPlanRoot().getNumInstances() > 1) {
            rootFragment = exchangeToMergeFragment(rootFragment, context);
        }
        List<Expr> outputExprs = Lists.newArrayList();
        physicalPlan.getOutput().stream().map(Slot::getExprId)
                .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
        rootFragment.setOutputExprs(outputExprs);
        rootFragment.getPlanRoot().convertToVectorized();
        for (PlanFragment fragment : context.getPlanFragments()) {
            fragment.finalize(null);
        }
        Collections.reverse(context.getPlanFragments());
        context.getDescTable().computeMemLayout();
        return rootFragment;
    }


    /**
     * Translate Agg.
     */
    @Override
    public PlanFragment visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate,
            PlanTranslatorContext context) {

        PlanFragment inputPlanFragment = aggregate.child(0).accept(this, context);

        List<Expression> groupByExpressionList = aggregate.getGroupByExpressions();
        List<NamedExpression> outputExpressionList = aggregate.getOutputExpressions();

        // 1. generate slot reference for each group expression
        List<SlotReference> groupSlotList = collectGroupBySlots(groupByExpressionList, outputExpressionList);
        ArrayList<Expr> execGroupingExpressions = groupByExpressionList.stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toCollection(ArrayList::new));
        // 2. collect agg expressions and generate agg function to slot reference map
        List<Slot> aggFunctionOutput = Lists.newArrayList();
        List<AggregateExpression> aggregateExpressionList = outputExpressionList.stream()
                .filter(o -> o.anyMatch(AggregateExpression.class::isInstance))
                .peek(o -> aggFunctionOutput.add(o.toSlot()))
                .map(o -> o.<Set<AggregateExpression>>collect(AggregateExpression.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toList());
        ArrayList<FunctionCallExpr> execAggregateFunctions = aggregateExpressionList.stream()
                .map(aggregateFunction -> (FunctionCallExpr) ExpressionTranslator.translate(aggregateFunction, context))
                .collect(Collectors.toCollection(ArrayList::new));

        PlanFragment currentFragment;
        if (inputPlanFragment.getPlanRoot() instanceof ExchangeNode
                && aggregate.child() instanceof PhysicalDistribute) {
            //the exchange node is generated in two cases:
            //  1. some nodes (e.g. sort node) need to gather data from multiple instances, and hence their gather phase
            //     need an exchange node. For this type of exchange, their data partition is un_partitioned, do not
            //     create a new plan fragment.
            //  2. PhysicalDistribute node is translated to exchange node. PhysicalDistribute node means we need to
            //     shuffle data, and we have to create a new plan fragment.
            ExchangeNode exchangeNode = (ExchangeNode) inputPlanFragment.getPlanRoot();
            Optional<List<Expression>> partitionExpressions = aggregate.getPartitionExpressions();
            PhysicalDistribute physicalDistribute = (PhysicalDistribute) aggregate.child();
            DataPartition dataPartition = toDataPartition(physicalDistribute, partitionExpressions, context).get();
            currentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, dataPartition);
            inputPlanFragment.setOutputPartition(dataPartition);
            inputPlanFragment.setPlanRoot(exchangeNode.getChild(0));
            inputPlanFragment.setDestination(exchangeNode);
            context.addPlanFragment(currentFragment);
        } else {
            currentFragment = inputPlanFragment;
        }

        // 3. generate output tuple
        List<Slot> slotList = Lists.newArrayList();
        TupleDescriptor outputTupleDesc;
        slotList.addAll(groupSlotList);
        slotList.addAll(aggFunctionOutput);
        outputTupleDesc = generateTupleDesc(slotList, null, context);

        List<Integer> aggFunOutputIds = ImmutableList.of();
        if (!aggFunctionOutput.isEmpty()) {
            aggFunOutputIds = outputTupleDesc
                    .getSlots()
                    .subList(groupSlotList.size(), outputTupleDesc.getSlots().size())
                    .stream()
                    .map(slot -> slot.getId().asInt())
                    .collect(ImmutableList.toImmutableList());
        }
        boolean isPartial = aggregate.getAggregateParam().aggMode.productAggregateBuffer;
        AggregateInfo aggInfo = AggregateInfo.create(execGroupingExpressions, execAggregateFunctions,
                aggFunOutputIds, isPartial, outputTupleDesc, outputTupleDesc, aggregate.getAggPhase().toExec());
        AggregationNode aggregationNode = new AggregationNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(), aggInfo);
        if (!aggregate.getAggMode().isFinalPhase) {
            aggregationNode.unsetNeedsFinalize();
        }
        PhysicalHashAggregate firstAggregateInFragment = context.getFirstAggregateInFragment(currentFragment);

        switch (aggregate.getAggPhase()) {
            case LOCAL:
                // we should set is useStreamingAgg when has exchange,
                // so the `aggregationNode.setUseStreamingPreagg()` in the visitPhysicalDistribute
                break;
            case DISTINCT_LOCAL:
                aggregationNode.setIntermediateTuple();
                break;
            case GLOBAL:
            case DISTINCT_GLOBAL:
                break;
            default:
                throw new RuntimeException("Unsupported yet");
        }
        if (firstAggregateInFragment == null) {
            context.setFirstAggregateInFragment(currentFragment, aggregate);
        }
        currentFragment.setPlanRoot(aggregationNode);
        if (aggregate.getStats() != null) {
            aggregationNode.setCardinality((long) aggregate.getStats().getRowCount());
        }
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = repeat.child(0).accept(this, context);

        Set<VirtualSlotReference> sortedVirtualSlots = repeat.getSortedVirtualSlots();
        TupleDescriptor virtualSlotsTuple =
                generateTupleDesc(ImmutableList.copyOf(sortedVirtualSlots), null, context);

        ImmutableSet<Expression> flattenGroupingSetExprs = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));

        List<Slot> aggregateFunctionUsedSlots = repeat.getOutputExpressions()
                .stream()
                .filter(output -> !(output instanceof VirtualSlotReference))
                .filter(output -> !flattenGroupingSetExprs.contains(output))
                .distinct()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());

        Set<Expression> usedSlotInRepeat = ImmutableSet.<Expression>builder()
                .addAll(flattenGroupingSetExprs)
                .addAll(aggregateFunctionUsedSlots)
                .build();

        List<Expr> preRepeatExprs = usedSlotInRepeat.stream()
                .map(expr -> ExpressionTranslator.translate(expr, context))
                .collect(ImmutableList.toImmutableList());

        List<Slot> outputSlots = repeat.getOutputExpressions()
                .stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());

        // NOTE: we should first translate preRepeatExprs, then generate output tuple,
        //       or else the preRepeatExprs can not find the bottom slotRef and throw
        //       exception: invalid slot id
        TupleDescriptor outputTuple = generateTupleDesc(outputSlots, null, context);

        // cube and rollup already convert to grouping sets in LogicalPlanBuilder.withAggregate()
        GroupingInfo groupingInfo = new GroupingInfo(
                GroupingType.GROUPING_SETS, virtualSlotsTuple, outputTuple, preRepeatExprs);

        List<Set<Integer>> repeatSlotIdList = repeat.computeRepeatSlotIdList(getSlotIdList(outputTuple));
        Set<Integer> allSlotId = repeatSlotIdList.stream()
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());

        RepeatNode repeatNode = new RepeatNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), groupingInfo, repeatSlotIdList,
                allSlotId, repeat.computeVirtualSlotValues(sortedVirtualSlots));
        repeatNode.setNumInstances(inputPlanFragment.getPlanRoot().getNumInstances());
        inputPlanFragment.addPlanRoot(repeatNode);
        inputPlanFragment.updateDataPartition(DataPartition.RANDOM);
        return inputPlanFragment;
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

        ArrayList<TupleId> tupleIds = new ArrayList<>();
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
        if (oneRowRelation.notBuildUnionNode()) {
            return null;
        }

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
            slotDescriptor.setIsNullable(legacyExprs.get(i).isNullable());
        }

        UnionNode unionNode = new UnionNode(context.nextPlanNodeId(), oneRowTuple.getId());
        unionNode.setCardinality(1L);
        unionNode.addConstExprList(legacyExprs);
        unionNode.finalizeForNereids(oneRowTuple.getSlots(), new ArrayList<>());

        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), unionNode, DataPartition.UNPARTITIONED);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanTranslatorContext context) {
        Preconditions.checkState(storageLayerAggregate.getRelation() instanceof PhysicalOlapScan,
                "PhysicalStorageLayerAggregate only support PhysicalOlapScan: "
                        + storageLayerAggregate.getRelation().getClass().getName());
        PlanFragment planFragment = storageLayerAggregate.getRelation().accept(this, context);

        OlapScanNode olapScanNode = (OlapScanNode) planFragment.getPlanRoot();
        TPushAggOp pushAggOp;
        switch (storageLayerAggregate.getAggOp()) {
            case COUNT:
                pushAggOp = TPushAggOp.COUNT;
                break;
            case MIN_MAX:
                pushAggOp = TPushAggOp.MINMAX;
                break;
            case MIX:
                pushAggOp = TPushAggOp.MIX;
                break;
            default:
                throw new AnalysisException("Unsupported storage layer aggregate: "
                        + storageLayerAggregate.getAggOp());
        }
        olapScanNode.setPushDownAggNoGrouping(pushAggOp);

        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        // Create OlapScanNode
        List<Slot> slotList = new ImmutableList.Builder<Slot>()
                .addAll(olapScan.getOutput())
                .addAll(olapScan.getNonUserVisibleOutput())
                .build();
        OlapTable olapTable = olapScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, olapTable, context);

        // Use column with the same name in selected materialized index meta for slot desc,
        // to get the correct col unique id.
        if (olapScan.getSelectedIndexId() != olapTable.getBaseIndexId()) {
            Map<String, Column> indexCols = olapTable.getSchemaByIndexId(olapScan.getSelectedIndexId())
                    .stream()
                    .collect(Collectors.toMap(Column::getName, Function.identity()));
            tupleDescriptor.getSlots().forEach(slotDesc -> {
                Column column = slotDesc.getColumn();
                if (column != null && indexCols.containsKey(column.getName())) {
                    slotDesc.setColumn(indexCols.get(column.getName()));
                }
            });
        }

        tupleDescriptor.setTable(olapTable);

        OlapScanNode olapScanNode = new OlapScanNode(context.nextPlanNodeId(), tupleDescriptor, "OlapScanNode");
        if (olapScan.getStats() != null) {
            olapScanNode.setCardinality((long) olapScan.getStats().getRowCount());
        }
        // TODO: Do we really need tableName here?
        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, olapTable, tableName);
        tupleDescriptor.setRef(tableRef);
        olapScanNode.setSelectedPartitionIds(olapScan.getSelectedPartitionIds());
        olapScanNode.setSampleTabletIds(olapScan.getSelectedTabletIds());

        switch (olapScan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
            case DUP_KEYS:
                PreAggStatus preAgg = olapScan.getPreAggStatus();
                olapScanNode.setSelectedIndexInfo(olapScan.getSelectedIndexId(), preAgg.isOn(), preAgg.getOffReason());
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
        olapScanNode.finalizeForNerieds();
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

    @Override
    public PlanFragment visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, PlanTranslatorContext context) {
        Table table = schemaScan.getTable();

        List<Slot> slotList = new ImmutableList.Builder<Slot>()
                .addAll(schemaScan.getOutput())
                .addAll(schemaScan.getNonUserVisibleOutput())
                .build();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);

        SchemaScanNode scanNode = new SchemaScanNode(context.nextPlanNodeId(), tupleDescriptor);
        scanNode.finalizeForNereids();
        context.getScanNodes().add(scanNode);
        PlanFragment planFragment =
                new PlanFragment(context.nextFragmentId(), scanNode, DataPartition.RANDOM);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalFileScan(PhysicalFileScan fileScan, PlanTranslatorContext context) {
        List<Slot> slotList = fileScan.getOutput();
        ExternalTable table = fileScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);
        ExternalFileScanNode fileScanNode = new ExternalFileScanNode(context.nextPlanNodeId(), tupleDescriptor);
        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, table, tableName);
        tupleDescriptor.setRef(tableRef);

        Utils.execWithUncheckedException(fileScanNode::init);
        context.addScanNode(fileScanNode);
        Utils.execWithUncheckedException(fileScanNode::finalizeForNerieds);
        // Create PlanFragment
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), fileScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanTranslatorContext context) {
        List<Slot> slots = tvfRelation.getLogicalProperties().getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, tvfRelation.getTable(), context);

        TableValuedFunctionIf catalogFunction = tvfRelation.getFunction().getCatalogFunction();
        ScanNode scanNode = catalogFunction.getScanNode(context.nextPlanNodeId(), tupleDescriptor);
        scanNode.finalizeForNereids();
        context.addScanNode(scanNode);

        // set label for explain
        for (Slot slot : slots) {
            String tableColumnName = "_table_valued_function_" + tvfRelation.getFunction().getName()
                    + "." + slots.get(0).getName();
            context.findSlotRef(slot.getExprId()).setLabel(tableColumnName);
        }

        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), scanNode, DataPartition.RANDOM);
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
        if (sort.getStats() != null) {
            sortNode.setCardinality((long) sort.getStats().getRowCount());
        }
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
                && (joinType == JoinType.LEFT_ANTI_JOIN
                    || joinType == JoinType.LEFT_SEMI_JOIN
                    || joinType == JoinType.NULL_AWARE_LEFT_ANTI_JOIN)) {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                leftIntermediateSlotDescriptor.add(sd);
            }
        } else if (hashJoin.getOtherJoinConjuncts().isEmpty()
                && (joinType == JoinType.RIGHT_ANTI_JOIN || joinType == JoinType.RIGHT_SEMI_JOIN)) {
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                rightIntermediateSlotDescriptor.add(sd);
            }
        } else {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                    hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
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
        if (hashJoin.getStats() != null) {
            hashJoinNode.setCardinality((long) hashJoin.getStats().getRowCount());
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

            JoinType joinType = nestedLoopJoin.getJoinType();

            NestedLoopJoinNode nestedLoopJoinNode = new NestedLoopJoinNode(context.nextPlanNodeId(),
                    leftFragmentPlanRoot, rightFragmentPlanRoot, tupleIds, JoinType.toJoinOperator(joinType),
                    null, null, null);
            if (nestedLoopJoin.getStats() != null) {
                nestedLoopJoinNode.setCardinality((long) nestedLoopJoin.getStats().getRowCount());
            }

            Map<ExprId, SlotReference> leftChildOutputMap = Maps.newHashMap();
            nestedLoopJoin.child(0).getOutput().stream()
                    .map(SlotReference.class::cast)
                    .forEach(s -> leftChildOutputMap.put(s.getExprId(), s));
            Map<ExprId, SlotReference> rightChildOutputMap = Maps.newHashMap();
            nestedLoopJoin.child(1).getOutput().stream()
                    .map(SlotReference.class::cast)
                    .forEach(s -> rightChildOutputMap.put(s.getExprId(), s));
            // make intermediate tuple
            List<SlotDescriptor> leftIntermediateSlotDescriptor = Lists.newArrayList();
            List<SlotDescriptor> rightIntermediateSlotDescriptor = Lists.newArrayList();
            TupleDescriptor intermediateDescriptor = context.generateTupleDesc();

            // Nereids does not care about output order of join,
            // but BE need left child's output must be before right child's output.
            // So we need to swap the output order of left and right child if necessary.
            // TODO: revert this after Nereids could ensure the output order is correct.
            List<SlotDescriptor> leftSlotDescriptors = leftTuples.stream()
                    .map(TupleDescriptor::getSlots)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            List<SlotDescriptor> rightSlotDescriptors = rightTuples.stream()
                    .map(TupleDescriptor::getSlots)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            Map<ExprId, SlotReference> outputSlotReferenceMap = Maps.newHashMap();

            nestedLoopJoin.getOutput().stream()
                    .map(SlotReference.class::cast)
                    .forEach(s -> outputSlotReferenceMap.put(s.getExprId(), s));
            List<SlotReference> outputSlotReferences = Stream.concat(leftTuples.stream(), rightTuples.stream())
                    .map(TupleDescriptor::getSlots)
                    .flatMap(Collection::stream)
                    .map(sd -> context.findExprId(sd.getId()))
                    .map(outputSlotReferenceMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (nestedLoopJoin.getOtherJoinConjuncts().isEmpty()
                    && (joinType == JoinType.LEFT_ANTI_JOIN
                        || joinType == JoinType.LEFT_SEMI_JOIN
                        || joinType == JoinType.NULL_AWARE_LEFT_ANTI_JOIN)) {
                for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                    if (!leftSlotDescriptor.isMaterialized()) {
                        continue;
                    }
                    SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                    SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                    leftIntermediateSlotDescriptor.add(sd);
                }
            } else if (nestedLoopJoin.getOtherJoinConjuncts().isEmpty()
                    && (joinType == JoinType.RIGHT_ANTI_JOIN || joinType == JoinType.RIGHT_SEMI_JOIN)) {
                for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                    if (!rightSlotDescriptor.isMaterialized()) {
                        continue;
                    }
                    SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                    SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                    rightIntermediateSlotDescriptor.add(sd);
                }
            } else {
                for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                    if (!leftSlotDescriptor.isMaterialized()) {
                        continue;
                    }
                    SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                    SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
                    leftIntermediateSlotDescriptor.add(sd);
                }
                for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                    if (!rightSlotDescriptor.isMaterialized()) {
                        continue;
                    }
                    SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                    SlotDescriptor sd = context.createSlotDesc(intermediateDescriptor, sf);
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

            nestedLoopJoinNode.setvIntermediateTupleDescList(Lists.newArrayList(intermediateDescriptor));

            rightFragment.getPlanRoot().setCompactData(false);
            nestedLoopJoinNode.setChild(0, leftFragment.getPlanRoot());
            connectChildFragment(nestedLoopJoinNode, 1, leftFragment, rightFragment, context);
            leftFragment.setPlanRoot(nestedLoopJoinNode);
            List<Expr> joinConjuncts = nestedLoopJoin.getOtherJoinConjuncts().stream()
                    .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());
            nestedLoopJoinNode.setJoinConjuncts(joinConjuncts);

            if (nestedLoopJoin.isShouldTranslateOutput()) {
                // translate output expr on intermediate tuple
                List<Expr> srcToOutput = outputSlotReferences.stream()
                        .map(e -> ExpressionTranslator.translate(e, context))
                        .collect(Collectors.toList());

                TupleDescriptor outputDescriptor = context.generateTupleDesc();
                outputSlotReferences.forEach(s -> context.createSlotDesc(outputDescriptor, s));

                nestedLoopJoinNode.setvOutputTupleDesc(outputDescriptor);
                nestedLoopJoinNode.setvSrcToOutputSMap(srcToOutput);
            }
            if (nestedLoopJoin.getStats() != null) {
                nestedLoopJoinNode.setCardinality((long) nestedLoopJoin.getStats().getRowCount());
            }
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
        if (project.child(0) instanceof PhysicalNestedLoopJoin) {
            ((PhysicalNestedLoopJoin<?, ?>) project.child(0)).setShouldTranslateOutput(false);
        }
        if (project.child(0) instanceof PhysicalFilter) {
            if (project.child(0).child(0) instanceof PhysicalHashJoin) {
                ((PhysicalHashJoin<?, ?>) project.child(0).child(0)).setShouldTranslateOutput(false);
            }
            if (project.child(0).child(0) instanceof PhysicalNestedLoopJoin) {
                ((PhysicalNestedLoopJoin<?, ?>) project.child(0).child(0)).setShouldTranslateOutput(false);
            }
        }
        PlanFragment inputFragment = project.child(0).accept(this, context);
        List<Expr> execExprList = project.getProjects()
                .stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        // TODO: fix the project alias of an aliased relation.

        PlanNode inputPlanNode = inputFragment.getPlanRoot();
        List<Slot> slotList = project.getOutput();
        // For hash join node, use vSrcToOutputSMap to describe the expression calculation, use
        // vIntermediateTupleDescList as input, and set vOutputTupleDesc as the final output.
        // TODO: HashJoinNode's be implementation is not support projection yet, remove this after when supported.
        if (inputPlanNode instanceof JoinNodeBase) {
            TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, null, context);
            JoinNodeBase hashJoinNode = (JoinNodeBase) inputPlanNode;
            hashJoinNode.setvOutputTupleDesc(tupleDescriptor);
            hashJoinNode.setvSrcToOutputSMap(execExprList);
            return inputFragment;
        }
        List<Expr> predicateList = inputPlanNode.getConjuncts();
        Set<SlotId> requiredSlotIdList = new HashSet<>();
        for (Expr expr : predicateList) {
            extractExecSlot(expr, requiredSlotIdList);
        }

        for (Expr expr : execExprList) {
            extractExecSlot(expr, requiredSlotIdList);
        }
        if (inputPlanNode instanceof TableFunctionNode) {
            TableFunctionNode tableFunctionNode = (TableFunctionNode) inputPlanNode;
            tableFunctionNode.setOutputSlotIds(Lists.newArrayList(requiredSlotIdList));
        }

        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, null, context);
        inputPlanNode.setProjectList(execExprList);
        inputPlanNode.setOutputTupleDesc(tupleDescriptor);

        if (inputPlanNode instanceof OlapScanNode) {
            updateChildSlotsMaterialization(inputPlanNode, requiredSlotIdList, context);
            return inputFragment;
        }
        return inputFragment;
    }

    private void updateChildSlotsMaterialization(PlanNode execPlan,
            Set<SlotId> requiredSlotIdList,
            PlanTranslatorContext context) {
        Set<SlotRef> slotRefSet = new HashSet<>();
        for (Expr expr : execPlan.getConjuncts()) {
            expr.collect(SlotRef.class, slotRefSet);
        }
        Set<SlotId> slotIdSet = slotRefSet.stream()
                .map(SlotRef::getSlotId).collect(Collectors.toSet());
        slotIdSet.addAll(requiredSlotIdList);
        boolean noneMaterialized = execPlan.getTupleIds().stream()
                .map(context::getTupleDesc)
                .map(TupleDescriptor::getSlots)
                .flatMap(List::stream)
                .peek(s -> s.setIsMaterialized(slotIdSet.contains(s.getId())))
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
            join.getFilterConjuncts().addAll(filter.getConjuncts());
        }
        PlanFragment inputFragment = filter.child(0).accept(this, context);

        // Union contains oneRowRelation --> inputFragment = null
        if (inputFragment == null) {
            return inputFragment;
        }

        PlanNode planNode = inputFragment.getPlanRoot();
        if (planNode instanceof ExchangeNode || planNode instanceof SortNode || planNode instanceof UnionNode) {
            // the three nodes don't support conjuncts, need create a SelectNode to filter data
            SelectNode selectNode = new SelectNode(context.nextPlanNodeId(), planNode);
            addConjunctsToPlanNode(filter, selectNode, context);
            inputFragment.addPlanRoot(selectNode);
        } else {
            if (!(filter.child(0) instanceof PhysicalHashJoin)) {
                addConjunctsToPlanNode(filter, planNode, context);
            }
        }
        return inputFragment;
    }

    private void addConjunctsToPlanNode(PhysicalFilter<? extends Plan> filter,
            PlanNode planNode,
            PlanTranslatorContext context) {
        filter.getConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(planNode::addConjunct);
    }

    @Override
    public PlanFragment visitPhysicalLimit(PhysicalLimit<? extends Plan> physicalLimit, PlanTranslatorContext context) {
        PlanFragment inputFragment = physicalLimit.child(0).accept(this, context);

        // Union contains oneRowRelation
        if (inputFragment == null) {
            return inputFragment;
        }

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

        if (childFragment.getPlanRoot() instanceof AggregationNode
                && distribute.child() instanceof PhysicalHashAggregate
                && context.getFirstAggregateInFragment(childFragment) == distribute.child()) {
            PhysicalHashAggregate<Plan> hashAggregate = (PhysicalHashAggregate) distribute.child();
            if (hashAggregate.getAggPhase() == AggPhase.LOCAL
                    && hashAggregate.getAggMode() == AggMode.INPUT_TO_BUFFER) {
                AggregationNode aggregationNode = (AggregationNode) childFragment.getPlanRoot();
                aggregationNode.setUseStreamingPreagg(hashAggregate.isMaybeUsingStream());
            }
        }

        ExchangeNode exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot(), false);
        exchange.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.setPlanRoot(exchange);
        return childFragment;
    }

    @Override
    public PlanFragment visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = assertNumRows.child().accept(this, context);
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

    /**
     * Returns a new fragment with a UnionNode as its root. The data partition of the
     * returned fragment and how the data of the child fragments is consumed depends on the
     * data partitions of the child fragments:
     * - All child fragments are unpartitioned or partitioned: The returned fragment has an
     *   UNPARTITIONED or RANDOM data partition, respectively. The UnionNode absorbs the
     *   plan trees of all child fragments.
     * - Mixed partitioned/unpartitioned child fragments: The returned fragment is
     *   RANDOM partitioned. The plan trees of all partitioned child fragments are absorbed
     *   into the UnionNode. All unpartitioned child fragments are connected to the
     *   UnionNode via a RANDOM exchange, and remain unchanged otherwise.
     */
    @Override
    public PlanFragment visitPhysicalSetOperation(
            PhysicalSetOperation setOperation, PlanTranslatorContext context) {
        List<PlanFragment> childrenFragments = new ArrayList<>();
        Map<Plan, PlanFragment> childNodeToFragment = new HashMap<>();
        for (Plan plan : setOperation.children()) {
            PlanFragment planFragment = plan.accept(this, context);
            if (planFragment != null) {
                childrenFragments.add(planFragment);
            }
            childNodeToFragment.put(plan, planFragment);
        }

        PlanFragment setOperationFragment;
        SetOperationNode setOperationNode;

        List<Slot> allSlots = new Builder<Slot>()
                .addAll(setOperation.getOutput())
                .build();
        TupleDescriptor setTuple = generateTupleDesc(allSlots, null, context);
        List<SlotDescriptor> outputSLotDescs = new ArrayList<>(setTuple.getSlots());

        // create setOperationNode
        if (setOperation instanceof PhysicalUnion) {
            setOperationNode = new UnionNode(
                    context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalExcept) {
            setOperationNode = new ExceptNode(
                    context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalIntersect) {
            setOperationNode = new IntersectNode(
                    context.nextPlanNodeId(), setTuple.getId());
        } else {
            throw new RuntimeException("not support");
        }

        SetOperationResult setOperationResult = collectSetOperationResult(setOperation, childNodeToFragment);
        for (List<Expression> expressions : setOperationResult.getResultExpressions()) {
            List<Expr> resultExprs = expressions
                    .stream()
                    .map(expr -> ExpressionTranslator.translate(expr, context))
                    .collect(ImmutableList.toImmutableList());
            setOperationNode.addResultExprLists(resultExprs);
        }

        for (List<Expression> expressions : setOperationResult.getConstExpressions()) {
            List<Expr> constExprs = expressions
                    .stream()
                    .map(expr -> ExpressionTranslator.translate(expr, context))
                    .collect(ImmutableList.toImmutableList());
            setOperationNode.addConstExprList(constExprs);
        }

        for (PlanFragment childFragment : childrenFragments) {
            if (childFragment != null) {
                setOperationNode.addChild(childFragment.getPlanRoot());
            }
        }
        setOperationNode.finalizeForNereids(outputSLotDescs, outputSLotDescs);

        // create setOperationFragment
        // If all child fragments are unpartitioned, return a single unpartitioned fragment
        // with a UnionNode that merges all child fragments.
        if (allChildFragmentsUnPartitioned(childrenFragments)) {
            setOperationFragment = new PlanFragment(
                    context.nextFragmentId(), setOperationNode, DataPartition.UNPARTITIONED);
            // Absorb the plan trees of all childFragments into unionNode
            // and fix up the fragment tree in the process.
            for (int i = 0; i < childrenFragments.size(); ++i) {
                connectChildFragmentNotCheckExchangeNode(setOperationNode, i, setOperationFragment,
                         childrenFragments.get(i),
                         context);
            }
        } else {
            setOperationFragment = new PlanFragment(context.nextFragmentId(), setOperationNode,
                    new DataPartition(TPartitionType.HASH_PARTITIONED,
                            setOperationNode.getMaterializedResultExprLists().get(0)));
            for (int i = 0; i < childrenFragments.size(); ++i) {
                PlanFragment childFragment = childrenFragments.get(i);
                // Connect the unpartitioned child fragments to SetOperationNode via a random exchange.
                connectChildFragmentNotCheckExchangeNode(
                        setOperationNode, i, setOperationFragment, childFragment, context);
                childFragment.setOutputPartition(
                        DataPartition.hashPartitioned(setOperationNode.getMaterializedResultExprLists().get(i)));
            }
        }
        context.addPlanFragment(setOperationFragment);
        return setOperationFragment;
    }

    @Override
    public PlanFragment visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = generate.child().accept(this, context);
        ArrayList<Expr> functionCalls = generate.getGenerators().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toCollection(ArrayList::new));
        TupleDescriptor tupleDescriptor = generateTupleDesc(generate.getGeneratorOutput(), null, context);
        List<SlotId> outputSlotIds = Stream.concat(currentFragment.getPlanRoot().getTupleIds().stream(),
                        Stream.of(tupleDescriptor.getId()))
                .map(id -> context.getTupleDesc(id).getSlots())
                .flatMap(List::stream)
                .map(SlotDescriptor::getId)
                .collect(Collectors.toList());
        TableFunctionNode tableFunctionNode = new TableFunctionNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(), tupleDescriptor.getId(), functionCalls, outputSlotIds);
        currentFragment.addPlanRoot(tableFunctionNode);
        return currentFragment;
    }

    private List<Expression> castCommonDataTypeOutputs(List<Slot> outputs, List<Slot> childOutputs) {
        List<Expression> newChildOutputs = new ArrayList<>();
        for (int i = 0; i < outputs.size(); ++i) {
            Slot right = childOutputs.get(i);
            DataType tightestCommonType = outputs.get(i).getDataType();
            Expression newRight = TypeCoercionUtils.castIfNotSameType(right, tightestCommonType);
            newChildOutputs.add(newRight);
        }
        return ImmutableList.copyOf(newChildOutputs);
    }

    private SetOperationResult collectSetOperationResult(
            PhysicalSetOperation setOperation, Map<Plan, PlanFragment> childPlanToFragment) {
        List<List<Expression>> resultExprs = new ArrayList<>();
        List<List<Expression>> constExprs = new ArrayList<>();
        List<Slot> outputs = setOperation.getOutput();
        for (Plan child : setOperation.children()) {
            List<Expression> castCommonDataTypeOutputs = castCommonDataTypeOutputs(outputs, child.getOutput());
            if (child.anyMatch(PhysicalOneRowRelation.class::isInstance) && childPlanToFragment.get(child) == null) {
                constExprs.add(collectConstExpressions(castCommonDataTypeOutputs, child));
            } else {
                resultExprs.add(castCommonDataTypeOutputs);
            }
        }
        return new SetOperationResult(resultExprs, constExprs);
    }

    private List<Expression> collectConstExpressions(
            List<Expression> castExpressions, Plan child) {
        List<Expression> newCastExpressions = new ArrayList<>();
        for (int i = 0; i < castExpressions.size(); ++i) {
            Expression expression = castExpressions.get(i);
            if (expression instanceof Cast) {
                newCastExpressions.add(expression.withChildren(
                        (collectPhysicalOneRowRelation(child).getProjects().get(i).children())));
            } else {
                newCastExpressions.add(
                        (collectPhysicalOneRowRelation(child).getProjects().get(i)));
            }
        }
        return newCastExpressions;
    }

    private PhysicalOneRowRelation collectPhysicalOneRowRelation(Plan child) {
        return (PhysicalOneRowRelation)
                ((ImmutableSet) child.collect(PhysicalOneRowRelation.class::isInstance)).asList().get(0);
    }

    private boolean allChildFragmentsUnPartitioned(List<PlanFragment> childrenFragments) {
        boolean allChildFragmentsUnPartitioned = true;
        for (PlanFragment child : childrenFragments) {
            allChildFragmentsUnPartitioned = allChildFragmentsUnPartitioned && !child.isPartitioned();
        }
        return allChildFragmentsUnPartitioned;
    }

    private void extractExecSlot(Expr root, Set<SlotId> slotIdList) {
        if (root instanceof SlotRef) {
            slotIdList.add(((SlotRef) root).getDesc().getId());
            return;
        }
        for (Expr child : root.getChildren()) {
            extractExecSlot(child, slotIdList);
        }
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, TableIf table, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot, table);
        }
        return tupleDescriptor;
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, List<OrderKey> orderKeyList,
            PlanTranslatorContext context, Table table) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        Set<ExprId> alreadyExists = Sets.newHashSet();
        tupleDescriptor.setTable(table);
        for (OrderKey orderKey : orderKeyList) {
            SlotReference slotReference;
            if (orderKey.getExpr() instanceof SlotReference) {
                slotReference = (SlotReference) orderKey.getExpr();
            } else {
                slotReference = (SlotReference) new Alias(orderKey.getExpr(), orderKey.getExpr().toString()).toSlot();
            }
            // TODO: trick here, we need semanticEquals to remove redundant expression
            if (alreadyExists.contains(slotReference.getExprId())) {
                continue;
            }
            context.createSlotDesc(tupleDescriptor, slotReference);
            alreadyExists.add(slotReference.getExprId());
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
        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(),
                childFragment.getPlanRoot(), false);
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

    private void connectChildFragmentNotCheckExchangeNode(PlanNode parent, int childIdx,
                                      PlanFragment parentFragment, PlanFragment childFragment,
                                      PlanTranslatorContext context) {
        PlanNode exchange = new ExchangeNode(
                context.nextPlanNodeId(), childFragment.getPlanRoot(), false);
        exchange.setNumInstances(childFragment.getPlanRoot().getNumInstances());
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
        ExchangeNode mergePlan = new ExchangeNode(context.nextPlanNodeId(),
                inputFragment.getPlanRoot(), false);
        DataPartition dataPartition = DataPartition.UNPARTITIONED;
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        PlanFragment fragment = new PlanFragment(context.nextFragmentId(), mergePlan, dataPartition);
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
        rightFragment.getTargetRuntimeFilterIds().stream().forEach(leftFragment::setTargetRuntimeFilterIds);
        rightFragment.getBuilderRuntimeFilterIds().stream().forEach(leftFragment::setBuilderRuntimeFilterIds);
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
        List<ExprId> rightPartitionExprIds = Lists.newArrayList(leftDistributionSpec.getOrderedShuffledColumns());
        for (int i = 0; i < leftDistributionSpec.getOrderedShuffledColumns().size(); i++) {
            int idx = leftDistributionSpec.getExprIdToEquivalenceSet()
                    .get(leftDistributionSpec.getOrderedShuffledColumns().get(i));
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

    private List<SlotReference> collectGroupBySlots(List<Expression> groupByExpressionList,
            List<NamedExpression> outputExpressionList) {
        List<SlotReference> groupSlotList = Lists.newArrayList();
        Set<VirtualSlotReference> virtualSlotReferences = groupByExpressionList.stream()
                .filter(VirtualSlotReference.class::isInstance)
                .map(VirtualSlotReference.class::cast)
                .collect(Collectors.toSet());
        for (Expression e : groupByExpressionList) {
            if (e instanceof SlotReference && outputExpressionList.stream().anyMatch(o -> o.anyMatch(e::equals))) {
                groupSlotList.add((SlotReference) e);
            } else if (e instanceof SlotReference && !virtualSlotReferences.isEmpty()) {
                // When there is a virtualSlot, it is a groupingSets scenario,
                // and the original exprId should be retained at this time.
                groupSlotList.add((SlotReference) e);
            } else {
                groupSlotList.add(new SlotReference(e.toSql(), e.getDataType(), e.nullable(), Collections.emptyList()));
            }
        }
        return groupSlotList;
    }

    private List<Integer> getSlotIdList(TupleDescriptor tupleDescriptor) {
        return tupleDescriptor.getSlots()
                .stream()
                .map(slot -> slot.getId().asInt())
                .collect(ImmutableList.toImmutableList());
    }

    private boolean isUnnecessaryProject(PhysicalProject project) {
        // The project list for agg is always needed,since tuple of agg contains the slots used by group by expr
        return !hasPrune(project) && !hasExprCalc(project);
    }

    private boolean hasPrune(PhysicalProject project) {
        PhysicalPlan child = (PhysicalPlan) project.child(0);

        return project.getProjects().size() != child.getOutput().size();
    }

    private boolean projectOnAgg(PhysicalProject project) {
        PhysicalPlan child = (PhysicalPlan) project.child(0);
        while (child instanceof PhysicalFilter || child instanceof PhysicalDistribute) {
            child = (PhysicalPlan) child.child(0);
        }
        return child instanceof PhysicalHashAggregate;
    }

    private boolean hasExprCalc(PhysicalProject<? extends Plan> project) {
        for (NamedExpression p : project.getProjects()) {
            if (p.children().size() > 1) {
                return true;
            }
            for (Expression e : p.children()) {
                if (!(e instanceof SlotReference)) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<Slot> removeAlias(PhysicalProject project) {
        List<NamedExpression> namedExpressions = project.getProjects();
        List<Slot> slotReferences = new ArrayList<>();
        for (NamedExpression n : namedExpressions) {
            if (n instanceof Alias) {
                slotReferences.add((SlotReference) n.child(0));
            } else {
                slotReferences.add((SlotReference) n);
            }
        }
        return slotReferences;
    }

    private Optional<DataPartition> toDataPartition(PhysicalDistribute distribute,
            Optional<List<Expression>> partitionExpressions, PlanTranslatorContext context) {
        if (distribute.getDistributionSpec() == DistributionSpecGather.INSTANCE) {
            return Optional.of(DataPartition.UNPARTITIONED);
        } else if (distribute.getDistributionSpec() == DistributionSpecReplicated.INSTANCE) {
            // the data partition should be left child of join
            return Optional.empty();
        } else if (distribute.getDistributionSpec() instanceof DistributionSpecHash
                || distribute.getDistributionSpec() == DistributionSpecAny.INSTANCE) {
            if (!partitionExpressions.isPresent()) {
                throw new AnalysisException("Missing partition expressions");
            }
            Preconditions.checkState(
                    partitionExpressions.get().stream().allMatch(expr -> expr instanceof SlotReference),
                    "All partition expression should be slot: " + partitionExpressions.get());
            if (!partitionExpressions.isPresent() || partitionExpressions.get().isEmpty()) {
                return Optional.of(DataPartition.UNPARTITIONED);
            }
            List<Expr> partitionExprs = partitionExpressions.get()
                    .stream()
                    .map(p -> ExpressionTranslator.translate(p, context))
                    .collect(ImmutableList.toImmutableList());
            return Optional.of(new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs));
        } else {
            return Optional.empty();
        }
    }

    private static class SetOperationResult {
        private final List<List<Expression>> resultExpressions;
        private final List<List<Expression>> constExpressions;

        public SetOperationResult(List<List<Expression>> resultExpressions, List<List<Expression>> constExpressions) {
            this.resultExpressions = ImmutableList.copyOf(resultExpressions);
            this.constExpressions = ImmutableList.copyOf(constExpressions);
        }

        public List<List<Expression>> getConstExpressions() {
            return constExpressions;
        }

        public List<List<Expression>> getResultExpressions() {
            return resultExpressions;
        }
    }
}
