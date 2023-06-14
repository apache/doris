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
import org.apache.doris.analysis.AnalyticWindow;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.GroupByClause.GroupingType;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.catalog.external.PaimonExternalTable;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.EsScanNode;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.JdbcScanNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.MultiCastPlanFragment;
import org.apache.doris.planner.NestedLoopJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PartitionSortNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RepeatNode;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.TableFunctionNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.planner.external.hudi.HudiScanNode;
import org.apache.doris.planner.external.iceberg.IcebergScanNode;
import org.apache.doris.planner.external.paimon.PaimonScanNode;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOG = LogManager.getLogger(PhysicalPlanTranslator.class);

    private final StatsErrorEstimator statsErrorEstimator;
    private final PlanTranslatorContext context;

    public PhysicalPlanTranslator() {
        this(null, null);
    }

    public PhysicalPlanTranslator(PlanTranslatorContext context) {
        this(context, null);
    }

    public PhysicalPlanTranslator(PlanTranslatorContext context, StatsErrorEstimator statsErrorEstimator) {
        this.context = context;
        this.statsErrorEstimator = statsErrorEstimator;
    }

    // We use two phase read to optimize sql like: select * from tbl [where xxx = ???] [order by column1] [limit n]
    // in the first phase, we add an extra column `RowId` to Block, and sort blocks in TopN nodes
    // in the second phase, we have n rows, we do a fetch rpc to get all rowids data for the n rows
    // and reconconstruct the final block
    private void setResultSinkFetchOptionIfNeed() {
        boolean needFetch = false;
        // Only single olap table should be fetched
        OlapTable fetchOlapTable = null;
        OlapScanNode scanNode = null;
        for (PlanFragment fragment : context.getPlanFragments()) {
            PlanNode node = fragment.getPlanRoot();
            PlanNode parent = null;
            // OlapScanNode is the last node.
            // So, just get the last two node and check if they are SortNode and OlapScan.
            while (node.getChildren().size() != 0) {
                parent = node;
                node = node.getChildren().get(0);
            }

            // case1: general topn optimized query
            if ((node instanceof OlapScanNode) && (parent instanceof SortNode)) {
                SortNode sortNode = (SortNode) parent;
                scanNode = (OlapScanNode) node;
                if (sortNode.getUseTwoPhaseReadOpt()) {
                    needFetch = true;
                    fetchOlapTable = scanNode.getOlapTable();
                    break;
                }
            }
        }
        for (PlanFragment fragment : context.getPlanFragments()) {
            if (needFetch && fragment.getSink() instanceof ResultSink) {
                TFetchOption fetchOption = new TFetchOption();
                fetchOption.setFetchRowStore(fetchOlapTable.storeRowColumn());
                fetchOption.setUseTwoPhaseFetch(true);
                fetchOption.setNodesInfo(Env.getCurrentSystemInfo().createAliveNodesInfo());
                if (!fetchOlapTable.storeRowColumn()) {
                    // Set column desc for each column
                    List<TColumn> columnsDesc = new ArrayList<TColumn>();
                    scanNode.getColumnDesc(columnsDesc, null, null);
                    fetchOption.setColumnDesc(columnsDesc);
                }
                ((ResultSink) fragment.getSink()).setFetchOption(fetchOption);
                break;
            }
        }
    }

    /**
     * Translate Nereids Physical Plan tree to Stale Planner PlanFragment tree.
     *
     * @param physicalPlan Nereids Physical Plan tree
     * @return Stale Planner PlanFragment tree
     */
    public PlanFragment translatePlan(PhysicalPlan physicalPlan) {
        PlanFragment rootFragment = physicalPlan.accept(this, context);
        if (physicalPlan instanceof PhysicalDistribute) {
            PhysicalDistribute distribute = (PhysicalDistribute) physicalPlan;
            DataPartition dataPartition;
            if (distribute.getDistributionSpec().equals(PhysicalProperties.GATHER.getDistributionSpec())) {
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

        if (!(physicalPlan instanceof PhysicalOlapTableSink) && isFragmentPartitioned(rootFragment)) {
            rootFragment = exchangeToMergeFragment(rootFragment, context);
        }

        if (rootFragment.getOutputExprs() == null) {
            List<Expr> outputExprs = Lists.newArrayList();
            physicalPlan.getOutput().stream().map(Slot::getExprId)
                    .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
            rootFragment.setOutputExprs(outputExprs);
        }
        for (PlanFragment fragment : context.getPlanFragments()) {
            fragment.finalize(null);
        }
        setResultSinkFetchOptionIfNeed();
        Collections.reverse(context.getPlanFragments());
        context.getDescTable().computeMemLayout();
        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> olapTableSink,
            PlanTranslatorContext context) {
        PlanFragment rootFragment = olapTableSink.child().accept(this, context);

        TupleDescriptor olapTuple = context.generateTupleDesc();
        List<Column> targetTableColumns = olapTableSink.getTargetTable().getFullSchema();
        for (Column column : targetTableColumns) {
            SlotDescriptor slotDesc = context.addSlotDesc(olapTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(column.getType());
            slotDesc.setColumn(column);
            slotDesc.setIsNullable(column.isAllowNull());
        }

        OlapTableSink sink = new OlapTableSink(
                olapTableSink.getTargetTable(),
                olapTuple,
                olapTableSink.getPartitionIds(),
                olapTableSink.isSingleReplicaLoad()
        );

        if (rootFragment.getPlanRoot() instanceof ExchangeNode) {
            ExchangeNode exchangeNode = ((ExchangeNode) rootFragment.getPlanRoot());
            PlanFragment currentFragment = new PlanFragment(
                    context.nextFragmentId(),
                    exchangeNode,
                    DataPartition.UNPARTITIONED);

            rootFragment.setPlanRoot(exchangeNode.getChild(0));
            rootFragment.setDestination(exchangeNode);
            context.addPlanFragment(currentFragment);
            rootFragment = currentFragment;
        }

        rootFragment.setSink(sink);

        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);

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
        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // so, if we do final phase or merge without exchange.
        // we need turn of parallel scan to ensure to get correct result.
        PlanNode leftMostNode = currentFragment.getPlanRoot();
        while (leftMostNode.getChildren().size() != 0 && !(leftMostNode instanceof ExchangeNode)) {
            leftMostNode = leftMostNode.getChild(0);
        }
        // TODO: nereids forbid all parallel scan under aggregate temporary, because nereids could generate
        //  so complex aggregate plan than legacy planner, and should add forbid parallel scan hint when
        //  generate physical aggregate plan.
        if (leftMostNode instanceof OlapScanNode && aggregate.getAggregateParam().needColocateScan) {
            currentFragment.setHasColocatePlanNode(true);
        }
        setPlanRoot(currentFragment, aggregationNode, aggregate);
        if (aggregate.getStats() != null) {
            aggregationNode.setCardinality((long) aggregate.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(currentFragment.getPlanRoot(), aggregate);
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
        addPlanRoot(inputPlanFragment, repeatNode, repeat);
        inputPlanFragment.updateDataPartition(DataPartition.RANDOM);
        updateLegacyPlanIdToPhysicalPlan(inputPlanFragment.getPlanRoot(), repeat);
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, PlanTranslatorContext context) {
        List<Slot> output = emptyRelation.getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(output, null, context);
        for (int i = 0; i < output.size(); i++) {
            Slot slot = output.get(i);
            SlotRef slotRef = context.findSlotRef(slot.getExprId());
            slotRef.setLabel(slot.getName());
        }

        ArrayList<TupleId> tupleIds = new ArrayList<>();
        tupleIds.add(tupleDescriptor.getId());
        EmptySetNode emptySetNode = new EmptySetNode(context.nextPlanNodeId(), tupleIds);

        PlanFragment planFragment = createPlanFragment(emptySetNode,
                DataPartition.UNPARTITIONED, emptyRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), emptyRelation);
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
            slotDescriptor.setIsNullable(expr.isNullable());
        }

        UnionNode unionNode = new UnionNode(context.nextPlanNodeId(), oneRowTuple.getId());
        unionNode.setCardinality(1L);
        unionNode.addConstExprList(legacyExprs);
        unionNode.finalizeForNereids(oneRowTuple.getSlots(), new ArrayList<>());

        PlanFragment planFragment = createPlanFragment(unionNode, DataPartition.UNPARTITIONED, oneRowRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), oneRowRelation);
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
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), storageLayerAggregate);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        // Create OlapScanNode
        List<Slot> slotList = olapScan.getOutput();
        Set<ExprId> deferredMaterializedExprIds = Collections.emptySet();
        if (olapScan.getMutableState(PhysicalOlapScan.DEFERRED_MATERIALIZED_SLOTS).isPresent()) {
            deferredMaterializedExprIds = (Set<ExprId>) (olapScan
                    .getMutableState(PhysicalOlapScan.DEFERRED_MATERIALIZED_SLOTS).get());
        }
        OlapTable olapTable = olapScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, olapTable, deferredMaterializedExprIds, context);

        if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
            generateTupleDesc(olapScan.getBaseOutputs(), olapTable, deferredMaterializedExprIds, context);
        }

        if (olapScan.getMutableState(PhysicalOlapScan.DEFERRED_MATERIALIZED_SLOTS).isPresent()) {
            injectRowIdColumnSlot(tupleDescriptor);
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
                runtimeFilterTranslator -> runtimeFilterTranslator.getTargetOnScanNode(olapScan.getId()).forEach(
                        expr -> runtimeFilterTranslator.translateRuntimeFilterTarget(expr, olapScanNode, context)
                )
        );
        olapScanNode.finalizeForNereids();
        // Create PlanFragment
        DataPartition dataPartition = DataPartition.RANDOM;
        if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) olapScan.getDistributionSpec();
            List<Expr> partitionExprs = distributionSpecHash.getOrderedShuffledColumns().stream()
                    .map(context::findSlotRef).collect(Collectors.toList());
            dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
        }
        PlanFragment planFragment = createPlanFragment(olapScanNode, dataPartition, olapScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), olapScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, PlanTranslatorContext context) {
        Table table = schemaScan.getTable();

        List<Slot> slotList = new ImmutableList.Builder<Slot>()
                .addAll(schemaScan.getOutput())
                .build();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);
        SchemaScanNode scanNode = new SchemaScanNode(context.nextPlanNodeId(), tupleDescriptor);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(schemaScan.getId()).forEach(
                    expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, scanNode, context)
            )
        );
        scanNode.finalizeForNereids();
        context.getScanNodes().add(scanNode);
        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, schemaScan);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalFileScan(PhysicalFileScan fileScan, PlanTranslatorContext context) {
        List<Slot> slotList = fileScan.getOutput();
        ExternalTable table = fileScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);

        // TODO(cmy): determine the needCheckColumnPriv param
        ScanNode scanNode = null;
        if (table instanceof HMSExternalTable) {
            switch (((HMSExternalTable) table).getDlaType()) {
                case HUDI:
                    scanNode = new HudiScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
                    break;
                case ICEBERG:
                    scanNode = new IcebergScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
                    break;
                case HIVE:
                    scanNode = new HiveScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
                    break;
                default:
                    break;
            }
        } else if (table instanceof IcebergExternalTable) {
            scanNode = new IcebergScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
        } else if (table instanceof PaimonExternalTable) {
            scanNode = new PaimonScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
        }
        Preconditions.checkNotNull(scanNode);
        fileScan.getConjuncts().stream()
            .map(e -> ExpressionTranslator.translate(e, context))
            .forEach(scanNode::addConjunct);
        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, table, tableName);
        tupleDescriptor.setRef(tableRef);

        Utils.execWithUncheckedException(scanNode::init);
        context.addScanNode(scanNode);
        ScanNode finalScanNode = scanNode;
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(fileScan.getId()).forEach(
                    expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, finalScanNode, context)
            )
        );
        Utils.execWithUncheckedException(scanNode::finalizeForNereids);
        // Create PlanFragment
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = createPlanFragment(scanNode, dataPartition, fileScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), fileScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanTranslatorContext context) {
        List<Slot> slots = tvfRelation.getLogicalProperties().getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, tvfRelation.getTable(), context);

        TableValuedFunctionIf catalogFunction = tvfRelation.getFunction().getCatalogFunction();
        ScanNode scanNode = catalogFunction.getScanNode(context.nextPlanNodeId(), tupleDescriptor);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(tvfRelation.getId()).forEach(
                    expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, scanNode, context)
            )
        );
        Utils.execWithUncheckedException(scanNode::finalizeForNereids);
        context.addScanNode(scanNode);

        // set label for explain
        for (Slot slot : slots) {
            String tableColumnName = "_table_valued_function_" + tvfRelation.getFunction().getName()
                    + "." + slots.get(0).getName();
            context.findSlotRef(slot.getExprId()).setLabel(tableColumnName);
        }

        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, tvfRelation);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, PlanTranslatorContext context) {
        List<Slot> slotList = jdbcScan.getOutput();
        ExternalTable table = jdbcScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);
        JdbcScanNode jdbcScanNode = new JdbcScanNode(context.nextPlanNodeId(), tupleDescriptor, true);
        Utils.execWithUncheckedException(jdbcScanNode::init);
        context.addScanNode(jdbcScanNode);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(jdbcScan.getId()).forEach(
                    expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, jdbcScanNode, context)
            )
        );
        Utils.execWithUncheckedException(jdbcScanNode::finalizeForNereids);
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), jdbcScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), jdbcScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalEsScan(PhysicalEsScan esScan, PlanTranslatorContext context) {
        List<Slot> slotList = esScan.getOutput();
        ExternalTable table = esScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, table, context);
        tupleDescriptor.setTable(table);
        EsScanNode esScanNode = new EsScanNode(context.nextPlanNodeId(), tupleDescriptor, "EsScanNode", true);
        Utils.execWithUncheckedException(esScanNode::init);
        context.addScanNode(esScanNode);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(esScan.getId()).forEach(
                    expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, esScanNode, context)
            )
        );
        Utils.execWithUncheckedException(esScanNode::finalizeForNereids);
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), esScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), esScan);
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
        PlanFragment inputFragment = sort.child(0).accept(this, context);
        PlanFragment currentFragment = inputFragment;

        //1. generate new fragment for sort when the child is exchangeNode
        if (inputFragment.getPlanRoot() instanceof ExchangeNode
                && sort.child(0) instanceof PhysicalDistribute) {
            DataPartition outputPartition = DataPartition.UNPARTITIONED;
            if (sort.getSortPhase().isLocal()) {
                // The window function like over (partition by xx order by) can generate plan
                // shuffle -> localSort -> windowFunction
                // In this case, the child's partition need to keep
                outputPartition = hashSpecToDataPartition((PhysicalDistribute) sort.child(0), context);
            }
            ExchangeNode exchangeNode = (ExchangeNode) inputFragment.getPlanRoot();
            inputFragment.setOutputPartition(outputPartition);
            inputFragment.setPlanRoot(exchangeNode.getChild(0));
            currentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, outputPartition);
            inputFragment.setDestination(exchangeNode);
            context.addPlanFragment(currentFragment);
        }

        // 2. According to the type of sort, generate physical plan
        if (!sort.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add sortNode
            SortNode sortNode = translateSortNode(sort, inputFragment.getPlanRoot(), context);
            addPlanRoot(currentFragment, sortNode, sort);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(currentFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., localSort -> mergeSort
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                return currentFragment;
            }
            Preconditions.checkArgument(inputFragment.getPlanRoot() instanceof SortNode);
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot();
            ((ExchangeNode) currentFragment.getPlanRoot()).setMergeInfo(sortNode.getSortInfo());
        }
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalWindow(PhysicalWindow<? extends Plan> physicalWindow,
                                            PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = physicalWindow.child(0).accept(this, context);

        // 1. translate to old optimizer variable
        // variable in Nereids
        WindowFrameGroup windowFrameGroup = physicalWindow.getWindowFrameGroup();
        List<Expression> partitionKeyList = Lists.newArrayList(windowFrameGroup.getPartitionKeys());
        List<OrderExpression> orderKeyList = windowFrameGroup.getOrderKeys();
        List<NamedExpression> windowFunctionList = windowFrameGroup.getGroups();
        WindowFrame windowFrame = windowFrameGroup.getWindowFrame();

        // partition by clause
        List<Expr> partitionExprs = partitionKeyList.stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        // order by clause
        List<OrderByElement> orderByElements = orderKeyList.stream()
                .map(orderKey -> new OrderByElement(
                        ExpressionTranslator.translate(orderKey.child(), context),
                        orderKey.isAsc(), orderKey.isNullFirst()))
                .collect(Collectors.toList());

        // function calls
        List<Expr> analyticFnCalls = windowFunctionList.stream()
                .map(e -> {
                    Expression function = e.child(0).child(0);
                    if (function instanceof AggregateFunction) {
                        AggregateParam param = AggregateParam.localResult();
                        function = new AggregateExpression((AggregateFunction) function, param);
                    }
                    return ExpressionTranslator.translate(function, context);
                })
                .map(FunctionCallExpr.class::cast)
                .map(fnCall -> {
                    fnCall.setIsAnalyticFnCall(true);
                    ((org.apache.doris.catalog.AggregateFunction) fnCall.getFn()).setIsAnalyticFn(true);
                    return fnCall;
                })
                .collect(Collectors.toList());

        // analytic window
        AnalyticWindow analyticWindow = physicalWindow.translateWindowFrame(windowFrame, context);

        // 2. get bufferedTupleDesc from SortNode and compute isNullableMatched
        Map<ExprId, SlotRef> bufferedSlotRefForWindow = getBufferedSlotRefForWindow(windowFrameGroup, context);
        TupleDescriptor bufferedTupleDesc = context.getBufferedTupleForWindow();

        // generate predicates to check if the exprs of partitionKeys and orderKeys have matched isNullable between
        // sortNode and analyticNode
        Expr partitionExprsIsNullableMatched = partitionExprs.isEmpty() ? null : windowExprsHaveMatchedNullable(
                partitionKeyList, partitionExprs, bufferedSlotRefForWindow);

        Expr orderElementsIsNullableMatched = orderByElements.isEmpty() ? null : windowExprsHaveMatchedNullable(
                orderKeyList.stream().map(order -> order.child()).collect(Collectors.toList()),
                orderByElements.stream().map(order -> order.getExpr()).collect(Collectors.toList()),
                bufferedSlotRefForWindow);

        // 3. generate tupleDesc
        List<Slot> windowSlotList = windowFunctionList.stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toList());
        TupleDescriptor outputTupleDesc = generateTupleDesc(windowSlotList, null, context);
        TupleDescriptor intermediateTupleDesc = outputTupleDesc;

        // 4. generate AnalyticEvalNode
        AnalyticEvalNode analyticEvalNode = new AnalyticEvalNode(
                context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(),
                analyticFnCalls,
                partitionExprs,
                orderByElements,
                analyticWindow,
                intermediateTupleDesc,
                outputTupleDesc,
                partitionExprsIsNullableMatched,
                orderElementsIsNullableMatched,
                bufferedTupleDesc
        );

        if (partitionExprs.isEmpty() && orderByElements.isEmpty()) {
            if (inputPlanFragment.isPartitioned()) {
                PlanFragment parentFragment = new PlanFragment(context.nextFragmentId(), analyticEvalNode,
                        DataPartition.UNPARTITIONED);
                context.addPlanFragment(parentFragment);
                connectChildFragment(analyticEvalNode, 0, parentFragment, inputPlanFragment, context);
                inputPlanFragment = parentFragment;
            } else {
                inputPlanFragment.addPlanRoot(analyticEvalNode);
            }
        } else {
            analyticEvalNode.setNumInstances(inputPlanFragment.getPlanRoot().getNumInstances());
            inputPlanFragment.addPlanRoot(analyticEvalNode);
        }
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN,
                                                   PlanTranslatorContext context) {
        PlanFragment inputFragment = partitionTopN.child(0).accept(this, context);

        Preconditions.checkArgument(!(partitionTopN.child(0) instanceof ExchangeNode));
        PartitionSortNode partitionSortNode = translatePartitionSortNode(partitionTopN,
                inputFragment.getPlanRoot(), context);
        addPlanRoot(inputFragment, partitionSortNode, partitionTopN);

        return inputFragment;
    }

    private PartitionSortNode translatePartitionSortNode(PhysicalPartitionTopN<? extends Plan> partitionTopN,
                                                         PlanNode childNode, PlanTranslatorContext context) {
        // Generate the SortInfo, similar to 'translateSortNode'.
        List<Expr> oldOrderingExprList = Lists.newArrayList();
        List<Boolean> ascOrderList = Lists.newArrayList();
        List<Boolean> nullsFirstParamList = Lists.newArrayList();
        List<OrderKey> orderKeyList = partitionTopN.getOrderKeys();
        // 1. Get previous slotRef
        orderKeyList.forEach(k -> {
            oldOrderingExprList.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrderList.add(k.isAsc());
            nullsFirstParamList.add(k.isNullFirst());
        });
        List<Expr> sortTupleOutputList = new ArrayList<>();
        List<Slot> outputList = partitionTopN.getOutput();
        outputList.forEach(k -> {
            sortTupleOutputList.add(ExpressionTranslator.translate(k, context));
        });
        List<Expr> partitionExprs = partitionTopN.getPartitionKeys().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        // 2. Generate new Tuple and get current slotRef for newOrderingExprList
        List<Expr> newOrderingExprList = Lists.newArrayList();
        TupleDescriptor tupleDesc = generateTupleDesc(outputList, orderKeyList, newOrderingExprList, context, null);
        // 3. fill in SortInfo members
        SortInfo sortInfo = new SortInfo(newOrderingExprList, ascOrderList, nullsFirstParamList, tupleDesc);

        PartitionSortNode partitionSortNode = new PartitionSortNode(context.nextPlanNodeId(), childNode,
                partitionTopN.getFunction(), partitionExprs, sortInfo, partitionTopN.hasGlobalLimit(),
                partitionTopN.getPartitionLimit(), sortTupleOutputList, oldOrderingExprList);

        if (partitionTopN.getStats() != null) {
            partitionSortNode.setCardinality((long) partitionTopN.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(partitionSortNode, partitionTopN);
        return partitionSortNode;
    }

    @Override
    public PlanFragment visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanTranslatorContext context) {
        PlanFragment inputFragment = topN.child(0).accept(this, context);
        PlanFragment currentFragment = inputFragment;

        //1. Generate new fragment for sort when the child is exchangeNode, If the child is
        // mergingExchange, it means we have always generated a new fragment when processing mergeSort
        if (inputFragment.getPlanRoot() instanceof ExchangeNode
                && !((ExchangeNode) inputFragment.getPlanRoot()).isMergingExchange()) {
            // Except LocalTopN->MergeTopN, we don't allow localTopN's child is Exchange Node
            Preconditions.checkArgument(!topN.getSortPhase().isLocal(),
                    "local sort requires any property but child is" + inputFragment.getPlanRoot());
            DataPartition outputPartition = DataPartition.UNPARTITIONED;
            ExchangeNode exchangeNode = (ExchangeNode) inputFragment.getPlanRoot();
            inputFragment.setOutputPartition(outputPartition);
            inputFragment.setPlanRoot(exchangeNode.getChild(0));
            inputFragment.setDestination(exchangeNode);
            currentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
            context.addPlanFragment(currentFragment);
        }

        // 2. According to the type of sort, generate physical plan
        if (!topN.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add TopNNode
            SortNode sortNode = translateSortNode(topN, inputFragment.getPlanRoot(), context);
            sortNode.setOffset(topN.getOffset());
            sortNode.setLimit(topN.getLimit());
            if (topN.getMutableState(PhysicalTopN.TOPN_RUNTIME_FILTER).isPresent()) {
                sortNode.setUseTopnOpt(true);
                PlanNode child = sortNode.getChild(0);
                Preconditions.checkArgument(child instanceof OlapScanNode,
                        "topN opt expect OlapScanNode, but we get " + child);
                ((OlapScanNode) child).setUseTopnOpt(true);
            }
            addPlanRoot(currentFragment, sortNode, topN);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(currentFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., mergeTopN -> localTopN
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                currentFragment.getPlanRoot().setOffset(topN.getOffset());
                currentFragment.getPlanRoot().setLimit(topN.getLimit());
                return currentFragment;
            }
            Preconditions.checkArgument(inputFragment.getPlanRoot() instanceof SortNode,
                    "mergeSort' child must be sortNode");
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot();
            ExchangeNode exchangeNode = (ExchangeNode) currentFragment.getPlanRoot();
            exchangeNode.setMergeInfo(sortNode.getSortInfo());
            exchangeNode.setLimit(topN.getLimit());
            exchangeNode.setOffset(topN.getOffset());
        }
        updateLegacyPlanIdToPhysicalPlan(currentFragment.getPlanRoot(), topN);
        return currentFragment;
    }

    private SortNode translateSortNode(AbstractPhysicalSort<? extends Plan> sort, PlanNode childNode,
            PlanTranslatorContext context) {
        List<Expr> oldOrderingExprList = Lists.newArrayList();
        List<Boolean> ascOrderList = Lists.newArrayList();
        List<Boolean> nullsFirstParamList = Lists.newArrayList();
        List<OrderKey> orderKeyList = sort.getOrderKeys();
        // 1. Get previous slotRef
        orderKeyList.forEach(k -> {
            oldOrderingExprList.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrderList.add(k.isAsc());
            nullsFirstParamList.add(k.isNullFirst());
        });
        List<Expr> sortTupleOutputList = new ArrayList<>();
        List<Slot> outputList = sort.getOutput();
        outputList.forEach(k -> sortTupleOutputList.add(ExpressionTranslator.translate(k, context)));
        // 2. Generate new Tuple and get current slotRef for newOrderingExprList
        List<Expr> newOrderingExprList = Lists.newArrayList();
        TupleDescriptor tupleDesc = generateTupleDesc(outputList, orderKeyList, newOrderingExprList, context, null);
        // 3. fill in SortInfo members
        SortInfo sortInfo = new SortInfo(newOrderingExprList, ascOrderList, nullsFirstParamList, tupleDesc);
        SortNode sortNode = new SortNode(context.nextPlanNodeId(), childNode, sortInfo, true);
        sortNode.finalizeForNereids(tupleDesc, sortTupleOutputList, oldOrderingExprList);
        if (sort.getMutableState(PhysicalTopN.TWO_PHASE_READ_OPT).isPresent()) {
            sortNode.setUseTwoPhaseReadOpt(true);
            sortNode.getSortInfo().setUseTwoPhaseRead();
            injectRowIdColumnSlot(sortNode.getSortInfo().getSortTupleDescriptor());
            TupleDescriptor childTuple = childNode.getOutputTupleDesc() != null
                    ? childNode.getOutputTupleDesc() : context.getTupleDesc(childNode.getTupleIds().get(0));
            SlotDescriptor childRowIdDesc = childTuple.getSlots().get(childTuple.getSlots().size() - 1);
            sortNode.getResolvedTupleExprs().add(new SlotRef(childRowIdDesc));
        }
        if (sort.getStats() != null) {
            sortNode.setCardinality((long) sort.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(sortNode, sort);
        return sortNode;
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
                null, null, null, hashJoin.isMarkJoin());

        PlanFragment currentFragment;
        if (JoinUtils.shouldColocateJoin(physicalHashJoin)) {
            currentFragment = constructColocateJoin(hashJoinNode, leftFragment, rightFragment, context, hashJoin);
        } else if (JoinUtils.shouldBucketShuffleJoin(physicalHashJoin)) {
            currentFragment = constructBucketShuffleJoin(
                    physicalHashJoin, hashJoinNode, leftFragment, rightFragment, context);
        } else if (JoinUtils.shouldBroadcastJoin(physicalHashJoin)) {
            currentFragment = constructBroadcastJoin(hashJoinNode, leftFragment, rightFragment, context, hashJoin);
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

        // translate runtime filter
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> runtimeFilterTranslator
                .getRuntimeFilterOfHashJoinNode(physicalHashJoin)
                .forEach(filter -> runtimeFilterTranslator.createLegacyRuntimeFilter(filter, hashJoinNode, context)));

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
                SlotDescriptor sd;
                if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
        } else if (hashJoin.getOtherJoinConjuncts().isEmpty()
                && (joinType == JoinType.RIGHT_ANTI_JOIN || joinType == JoinType.RIGHT_SEMI_JOIN)) {
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        } else {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                    }
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                    }
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        }

        if (hashJoin.getMarkJoinSlotReference().isPresent()) {
            outputSlotReferences.add(hashJoin.getMarkJoinSlotReference().get());
            context.createSlotDesc(intermediateDescriptor, hashJoin.getMarkJoinSlotReference().get());
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
        updateLegacyPlanIdToPhysicalPlan(currentFragment.getPlanRoot(), hashJoin);
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
                    null, null, null, nestedLoopJoin.isMarkJoin());
            if (nestedLoopJoin.getStats() != null) {
                nestedLoopJoinNode.setCardinality((long) nestedLoopJoin.getStats().getRowCount());
            }
            boolean needNewRootFragment = nestedLoopJoin.child(0) instanceof PhysicalDistribute;
            PlanFragment joinFragment;
            if (needNewRootFragment) {
                joinFragment = createPlanFragment(nestedLoopJoinNode,
                        DataPartition.UNPARTITIONED, nestedLoopJoin);
                context.addPlanFragment(joinFragment);
                connectChildFragment(nestedLoopJoinNode, 0, joinFragment, leftFragment, context);
            } else {
                joinFragment = leftFragment;
                nestedLoopJoinNode.setChild(0, leftFragment.getPlanRoot());
                setPlanRoot(joinFragment, nestedLoopJoinNode, nestedLoopJoin);
            }
            // translate runtime filter
            context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> {
                List<RuntimeFilter> filters = runtimeFilterTranslator
                        .getRuntimeFilterOfHashJoinNode(nestedLoopJoin);
                filters.forEach(filter -> runtimeFilterTranslator
                        .createLegacyRuntimeFilter(filter, nestedLoopJoinNode, context));
                if (!filters.isEmpty()) {
                    nestedLoopJoinNode.setOutputLeftSideOnly(true);
                }
            });

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
            nestedLoopJoin.getFilterConjuncts().stream()
                    .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                    .flatMap(e -> e.getInputSlots().stream())
                    .map(SlotReference.class::cast)
                    .forEach(s -> outputSlotReferenceMap.put(s.getExprId(), s));
            List<SlotReference> outputSlotReferences = Stream.concat(leftTuples.stream(), rightTuples.stream())
                    .map(TupleDescriptor::getSlots)
                    .flatMap(Collection::stream)
                    .map(sd -> context.findExprId(sd.getId()))
                    .map(outputSlotReferenceMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            // TODO: because of the limitation of be, the VNestedLoopJoinNode will output column from both children
            // in the intermediate tuple, so fe have to do the same, if be fix the problem, we can change it back.
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf);
                }
                rightIntermediateSlotDescriptor.add(sd);
            }

            if (nestedLoopJoin.getMarkJoinSlotReference().isPresent()) {
                outputSlotReferences.add(nestedLoopJoin.getMarkJoinSlotReference().get());
                context.createSlotDesc(intermediateDescriptor, nestedLoopJoin.getMarkJoinSlotReference().get());
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

            connectChildFragment(nestedLoopJoinNode, 1, joinFragment, rightFragment, context);
            List<Expr> joinConjuncts = nestedLoopJoin.getOtherJoinConjuncts().stream()
                    .filter(e -> !nestedLoopJoin.isBitmapRuntimeFilterCondition(e))
                    .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());

            if (!nestedLoopJoin.isBitMapRuntimeFilterConditionsEmpty() && joinConjuncts.isEmpty()) {
                //left semi join need at least one conjunct. otherwise left-semi-join fallback to cross-join
                joinConjuncts.add(new BoolLiteral(true));
            }

            nestedLoopJoinNode.setJoinConjuncts(joinConjuncts);

            nestedLoopJoin.getFilterConjuncts().stream()
                    .filter(e -> !(e.equals(BooleanLiteral.TRUE)))
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .forEach(nestedLoopJoinNode::addConjunct);

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
            updateLegacyPlanIdToPhysicalPlan(joinFragment.getPlanRoot(), nestedLoopJoin);
            return joinFragment;
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
        List<Slot> slotList = project.getProjects()
                .stream()
                .map(e -> e.toSlot())
                .collect(Collectors.toList());

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
        Set<SlotId> requiredSlotIdSet = Sets.newHashSet();
        for (Expr expr : execExprList) {
            extractExecSlot(expr, requiredSlotIdSet);
        }
        Set<SlotId> requiredByProjectSlotIdSet = Sets.newHashSet(requiredSlotIdSet);
        for (Expr expr : predicateList) {
            extractExecSlot(expr, requiredSlotIdSet);
        }
        if (inputPlanNode instanceof TableFunctionNode) {
            TableFunctionNode tableFunctionNode = (TableFunctionNode) inputPlanNode;
            tableFunctionNode.setOutputSlotIds(Lists.newArrayList(requiredSlotIdSet));
        }

        if (inputPlanNode instanceof ScanNode) {
            TupleDescriptor tupleDescriptor = null;
            if (requiredByProjectSlotIdSet.size() != requiredSlotIdSet.size()
                    || execExprList.stream().collect(Collectors.toSet()).size() != execExprList.size()
                    || execExprList.stream().anyMatch(expr -> !(expr instanceof SlotRef))) {
                tupleDescriptor = generateTupleDesc(slotList, null, context);
                inputPlanNode.setProjectList(execExprList);
                inputPlanNode.setOutputTupleDesc(tupleDescriptor);
            } else {
                for (int i = 0; i < slotList.size(); ++i) {
                    context.addExprIdSlotRefPair(slotList.get(i).getExprId(),
                            (SlotRef) execExprList.get(i));
                }
            }

            // TODO: this is a temporary scheme to support two phase read when has project.
            //  we need to refactor all topn opt into rbo stage.
            if (inputPlanNode instanceof OlapScanNode) {
                ArrayList<SlotDescriptor> slots =
                        context.getTupleDesc(inputPlanNode.getTupleIds().get(0)).getSlots();
                SlotDescriptor lastSlot = slots.get(slots.size() - 1);
                if (lastSlot.getColumn() != null
                        && lastSlot.getColumn().getName().equals(Column.ROWID_COL)) {
                    if (tupleDescriptor != null) {
                        injectRowIdColumnSlot(tupleDescriptor);
                        SlotRef slotRef = new SlotRef(lastSlot);
                        inputPlanNode.getProjectList().add(slotRef);
                        requiredByProjectSlotIdSet.add(lastSlot.getId());
                    }
                    requiredSlotIdSet.add(lastSlot.getId());
                }
            }
            updateChildSlotsMaterialization(inputPlanNode, requiredSlotIdSet,
                    requiredByProjectSlotIdSet, context);
        } else {
            TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, null, context);
            inputPlanNode.setProjectList(execExprList);
            inputPlanNode.setOutputTupleDesc(tupleDescriptor);
        }
        return inputFragment;
    }

    private void updateChildSlotsMaterialization(PlanNode execPlan,
            Set<SlotId> requiredSlotIdSet, Set<SlotId> requiredByProjectSlotIdSet,
            PlanTranslatorContext context) {
        Set<SlotRef> slotRefSet = new HashSet<>();
        for (Expr expr : execPlan.getConjuncts()) {
            expr.collect(SlotRef.class, slotRefSet);
        }
        Set<SlotId> slotIdSet = slotRefSet.stream()
                .map(SlotRef::getSlotId).collect(Collectors.toSet());
        slotIdSet.addAll(requiredSlotIdSet);
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
        if (execPlan instanceof ScanNode) {
            try {
                ((ScanNode) execPlan).updateRequiredSlots(context, requiredByProjectSlotIdSet);
            } catch (UserException e) {
                Util.logAndThrowRuntimeException(LOG,
                        "User Exception while reset external file scan node contexts.", e);
            }
        }
    }

    @Override
    public PlanFragment visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanTranslatorContext context) {
        if (filter.child(0) instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin join = (AbstractPhysicalJoin<?, ?>) filter.child(0);
            join.addFilterConjuncts(filter.getConjuncts());
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
            addPlanRoot(inputFragment, selectNode, filter);
        } else {
            if (!(filter.child(0) instanceof AbstractPhysicalJoin)) {
                addConjunctsToPlanNode(filter, planNode, context);
                updateLegacyPlanIdToPhysicalPlan(inputFragment.getPlanRoot(), filter);
            }
        }
        //in ut, filter.stats may be null
        if (filter.getStats() != null) {
            inputFragment.getPlanRoot().setCardinalityAfterFilter((long) filter.getStats().getRowCount());
        }
        return inputFragment;
    }

    private void addConjunctsToPlanNode(PhysicalFilter<? extends Plan> filter,
            PlanNode planNode,
            PlanTranslatorContext context) {
        filter.getConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(planNode::addConjunct);
        updateLegacyPlanIdToPhysicalPlan(planNode, filter);
    }

    @Override
    public PlanFragment visitPhysicalLimit(PhysicalLimit<? extends Plan> physicalLimit, PlanTranslatorContext context) {
        PlanFragment inputFragment = physicalLimit.child(0).accept(this, context);

        // Union contains oneRowRelation
        if (inputFragment == null) {
            return null;
        }

        PlanNode child = inputFragment.getPlanRoot();
        if (physicalLimit.isGlobal()) {
            if (child instanceof ExchangeNode) {
                DataPartition outputPartition = DataPartition.UNPARTITIONED;
                ExchangeNode exchangeNode = (ExchangeNode) inputFragment.getPlanRoot();
                inputFragment.setOutputPartition(outputPartition);
                inputFragment.setPlanRoot(exchangeNode.getChild(0));
                inputFragment.setDestination(exchangeNode);
                inputFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
                context.addPlanFragment(inputFragment);
            } else if (physicalLimit.hasValidOffset()) {
                // This case means GlobalLimit's child isn't gatherNode, which suggests the child is UNPARTITIONED
                // When there is valid offset, exchangeNode should be added because other node don't support offset
                inputFragment = createParentFragment(inputFragment, DataPartition.UNPARTITIONED, context);
                child = inputFragment.getPlanRoot();
            }
        }
        child.setOffset(physicalLimit.getOffset());
        child.setLimit(physicalLimit.getLimit());
        updateLegacyPlanIdToPhysicalPlan(child, physicalLimit);
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

        ExchangeNode exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot());
        exchange.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.setPlanRoot(exchange);
        updateLegacyPlanIdToPhysicalPlan(childFragment.getPlanRoot(), distribute);
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
        addPlanRoot(currentFragment, assertNumRowsNode, assertNumRows);
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
            setOperationFragment = createPlanFragment(setOperationNode, DataPartition.UNPARTITIONED, setOperation);
            // Absorb the plan trees of all childFragments into unionNode
            // and fix up the fragment tree in the process.
            for (int i = 0; i < childrenFragments.size(); ++i) {
                connectChildFragmentNotCheckExchangeNode(setOperationNode, i, setOperationFragment,
                        childrenFragments.get(i),
                        context);
            }
        } else {
            setOperationFragment = createPlanFragment(setOperationNode,
                    new DataPartition(TPartitionType.HASH_PARTITIONED,
                            setOperationNode.getMaterializedResultExprLists().get(0)), setOperation);
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
        List<TupleId> childOutputTupleIds = currentFragment.getPlanRoot().getOutputTupleIds();
        if (childOutputTupleIds == null || childOutputTupleIds.isEmpty()) {
            childOutputTupleIds = currentFragment.getPlanRoot().getTupleIds();
        }
        List<SlotId> outputSlotIds = Stream.concat(childOutputTupleIds.stream(),
                        Stream.of(tupleDescriptor.getId()))
                .map(id -> context.getTupleDesc(id).getSlots())
                .flatMap(List::stream)
                .map(SlotDescriptor::getId)
                .collect(Collectors.toList());
        TableFunctionNode tableFunctionNode = new TableFunctionNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(), tupleDescriptor.getId(), functionCalls, outputSlotIds);
        addPlanRoot(currentFragment, tableFunctionNode, generate);
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer,
                                                PlanTranslatorContext context) {
        CTEId cteId = cteConsumer.getCteId();

        MultiCastPlanFragment multCastFragment = (MultiCastPlanFragment) context.getCteProduceFragments().get(cteId);
        Preconditions.checkState(multCastFragment.getSink() instanceof MultiCastDataSink,
                "invalid multCastFragment");

        MultiCastDataSink multiCastDataSink = (MultiCastDataSink) multCastFragment.getSink();
        Preconditions.checkState(multiCastDataSink != null, "invalid multiCastDataSink");

        PhysicalCTEProducer cteProducer = context.getCteProduceMap().get(cteId);
        Preconditions.checkState(cteProducer != null, "invalid cteProducer");

        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), multCastFragment.getPlanRoot());

        DataStreamSink streamSink = new DataStreamSink(exchangeNode.getId());
        streamSink.setPartition(DataPartition.RANDOM);
        streamSink.setFragment(multCastFragment);

        multiCastDataSink.getDataStreamSinks().add(streamSink);
        multiCastDataSink.getDestinations().add(Lists.newArrayList());

        exchangeNode.setNumInstances(multCastFragment.getPlanRoot().getNumInstances());

        PlanFragment consumeFragment = new PlanFragment(context.nextFragmentId(), exchangeNode,
                multCastFragment.getDataPartition());

        Map<Slot, Slot> projectMap = Maps.newHashMap();
        projectMap.putAll(cteConsumer.getProducerToConsumerSlotMap());

        List<NamedExpression> execList = new ArrayList<>();
        PlanNode inputPlanNode = consumeFragment.getPlanRoot();
        List<Slot> cteProjects = cteProducer.getProjects();
        for (Slot slot : cteProjects) {
            if (projectMap.containsKey(slot)) {
                execList.add(projectMap.get(slot));
            } else {
                throw new RuntimeException("could not find slot in cte producer consumer projectMap");
            }
        }

        List<Slot> slotList = execList
                .stream()
                .map(e -> e.toSlot())
                .collect(Collectors.toList());

        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, null, context);

        // update tuple list and tblTupleList
        inputPlanNode.getTupleIds().clear();
        inputPlanNode.getTupleIds().add(tupleDescriptor.getId());
        inputPlanNode.getTblRefIds().clear();
        inputPlanNode.getTblRefIds().add(tupleDescriptor.getId());
        inputPlanNode.getNullableTupleIds().clear();
        inputPlanNode.getNullableTupleIds().add(tupleDescriptor.getId());

        List<Expr> execExprList = execList
                .stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        inputPlanNode.setProjectList(execExprList);
        inputPlanNode.setOutputTupleDesc(tupleDescriptor);

        // update data partition
        DataPartition dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, execExprList);
        consumeFragment.setDataPartition(dataPartition);

        SelectNode projectNode = new SelectNode(context.nextPlanNodeId(), inputPlanNode);
        consumeFragment.setPlanRoot(projectNode);

        multCastFragment.getDestNodeList().add(exchangeNode);
        consumeFragment.addChild(multCastFragment);
        context.getPlanFragments().add(consumeFragment);

        return consumeFragment;
    }

    @Override
    public PlanFragment visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> cteProducer,
                                                PlanTranslatorContext context) {
        PlanFragment child = cteProducer.child().accept(this, context);
        CTEId cteId = cteProducer.getCteId();
        context.getPlanFragments().remove(child);
        MultiCastPlanFragment cteProduce = new MultiCastPlanFragment(child);
        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        cteProduce.setSink(multiCastDataSink);

        List<Expr> outputs = cteProducer.getProjects().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        cteProduce.setOutputExprs(outputs);
        context.getCteProduceFragments().put(cteId, cteProduce);
        context.getCteProduceMap().put(cteId, cteProducer);
        context.getPlanFragments().add(cteProduce);
        return child;
    }

    /**
     * NOTICE: Must translate left, which it's the producer of consumer.
     */
    @Override
    public PlanFragment visitPhysicalCTEAnchor(PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
                                               PlanTranslatorContext context) {
        cteAnchor.child(0).accept(this, context);
        return cteAnchor.child(1).accept(this, context);
    }

    private List<Expression> castCommonDataTypeOutputs(List<Slot> outputs, List<Slot> childOutputs) {
        List<Expression> newChildOutputs = new ArrayList<>();
        for (int i = 0; i < outputs.size(); ++i) {
            Slot right = childOutputs.get(i);
            DataType tightestCommonType = outputs.get(i).getDataType();
            Expression newRight = TypeCoercionUtils.castIfNotMatchType(right, tightestCommonType);
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

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, TableIf table,
            Set<ExprId> deferredMaterializedExprIds, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            SlotDescriptor slotDescriptor = context.createSlotDesc(tupleDescriptor, (SlotReference) slot, table);
            if (deferredMaterializedExprIds.contains(slot.getExprId())) {
                slotDescriptor.setNeedMaterialize(false);
            }
        }
        return tupleDescriptor;
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
            List<Expr> newOrderingExprList,
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
                newOrderingExprList.add(context.findSlotRef(slotReference.getExprId()));
                continue;
            }
            context.createSlotDesc(tupleDescriptor, slotReference);
            newOrderingExprList.add(context.findSlotRef(slotReference.getExprId()));
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
        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot());
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
            exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot());
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
        PlanNode exchange = new ExchangeNode(context.nextPlanNodeId(), childFragment.getPlanRoot());
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
        if (!inputFragment.isPartitioned()) {
            return inputFragment;
        }

        // exchange node clones the behavior of its input, aside from the conjuncts
        ExchangeNode mergePlan = new ExchangeNode(context.nextPlanNodeId(), inputFragment.getPlanRoot());
        DataPartition dataPartition = DataPartition.UNPARTITIONED;
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        PlanFragment fragment = new PlanFragment(context.nextFragmentId(), mergePlan, dataPartition);
        inputFragment.setDestination(mergePlan);
        context.addPlanFragment(fragment);
        return fragment;
    }

    private PlanFragment constructColocateJoin(HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context, AbstractPlan join) {
        // TODO: add reason
        hashJoinNode.setColocate(true, "");
        hashJoinNode.setChild(0, leftFragment.getPlanRoot());
        hashJoinNode.setChild(1, rightFragment.getPlanRoot());
        setPlanRoot(leftFragment, hashJoinNode, join);
        rightFragment.getTargetRuntimeFilterIds().forEach(leftFragment::setTargetRuntimeFilterIds);
        rightFragment.getBuilderRuntimeFilterIds().forEach(leftFragment::setBuilderRuntimeFilterIds);
        context.removePlanFragment(rightFragment);
        leftFragment.setHasColocatePlanNode(true);
        return leftFragment;
    }

    private PlanFragment constructBucketShuffleJoin(PhysicalHashJoin<PhysicalPlan, PhysicalPlan> physicalHashJoin,
            HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context) {
        // according to left partition to generate right partition expr list
        DistributionSpecHash leftDistributionSpec
                = (DistributionSpecHash) physicalHashJoin.left().getPhysicalProperties().getDistributionSpec();

        Pair<List<ExprId>, List<ExprId>> onClauseUsedSlots = physicalHashJoin.getHashConjunctsExprIds();
        List<ExprId> rightPartitionExprIds = Lists.newArrayList(leftDistributionSpec.getOrderedShuffledColumns());
        for (int i = 0; i < leftDistributionSpec.getOrderedShuffledColumns().size(); i++) {
            int idx = leftDistributionSpec.getExprIdToEquivalenceSet()
                    .get(leftDistributionSpec.getOrderedShuffledColumns().get(i));
            ExprId leftShuffleColumnId = leftDistributionSpec.getOrderedShuffledColumns().get(i);
            Set<ExprId> equivalIds = leftDistributionSpec.getEquivalenceExprIdsOf(leftShuffleColumnId);
            int index = -1;
            for (ExprId id : equivalIds) {
                index = onClauseUsedSlots.first.indexOf(id);
                if (index != -1) {
                    break;
                }
            }
            Preconditions.checkArgument(index != -1);
            rightPartitionExprIds.set(idx, onClauseUsedSlots.second.get(index));
        }
        // assemble fragment
        hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.BUCKET_SHUFFLE);
        if (leftDistributionSpec.getShuffleType() != ShuffleType.NATURAL
                && leftDistributionSpec.getShuffleType() != ShuffleType.BUCKETED) {
            hashJoinNode.setDistributionMode(DistributionMode.PARTITIONED);
        }
        connectChildFragment(hashJoinNode, 1, leftFragment, rightFragment, context);
        setPlanRoot(leftFragment, hashJoinNode, physicalHashJoin);
        // HASH_PARTITIONED and BUCKET_SHFFULE_HASH_PARTITIONED are two type of hash algorithm
        // And the nature left child means it use BUCKET_SHFFULE_HASH_PARTITIONED in storage layer
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
            PlanFragment rightFragment, PlanTranslatorContext context, AbstractPlan join) {
        hashJoinNode.setDistributionMode(DistributionMode.BROADCAST);
        setPlanRoot(leftFragment, hashJoinNode, join);
        rightFragment.setRightChildOfBroadcastHashJoin(true);
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
        PlanFragment joinFragment = createPlanFragment(hashJoinNode, lhsJoinPartition, physicalHashJoin);
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
                groupSlotList.add(new SlotReference(e.toSql(), e.getDataType(), e.nullable(), ImmutableList.of()));
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

    private boolean isFragmentPartitioned(PlanFragment fragment) {
        return fragment.isPartitioned() && fragment.getPlanRoot().getNumInstances() > 1;
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

    private Map<ExprId, SlotRef> getBufferedSlotRefForWindow(WindowFrameGroup windowFrameGroup,
                                                             PlanTranslatorContext context) {
        Map<ExprId, SlotRef> bufferedSlotRefForWindow = context.getBufferedSlotRefForWindow();

        // set if absent
        windowFrameGroup.getPartitionKeys().stream()
                .map(NamedExpression.class::cast)
                .forEach(expression -> {
                    ExprId exprId = expression.getExprId();
                    bufferedSlotRefForWindow.putIfAbsent(exprId, context.findSlotRef(exprId));
                });
        windowFrameGroup.getOrderKeys().stream()
                .map(ok -> ok.child())
                .map(NamedExpression.class::cast)
                .forEach(expression -> {
                    ExprId exprId = expression.getExprId();
                    bufferedSlotRefForWindow.putIfAbsent(exprId, context.findSlotRef(exprId));
                });
        return bufferedSlotRefForWindow;
    }

    private Expr windowExprsHaveMatchedNullable(List<Expression> expressions, List<Expr> exprs,
                                                Map<ExprId, SlotRef> bufferedSlotRef) {
        Map<ExprId, Expr> exprIdToExpr = Maps.newHashMap();
        for (int i = 0; i < expressions.size(); i++) {
            NamedExpression expression = (NamedExpression) expressions.get(i);
            exprIdToExpr.put(expression.getExprId(), exprs.get(i));
        }
        return windowExprsHaveMatchedNullable(exprIdToExpr, bufferedSlotRef, expressions, 0, expressions.size());
    }

    private Expr windowExprsHaveMatchedNullable(Map<ExprId, Expr> exprIdToExpr, Map<ExprId, SlotRef> exprIdToSlotRef,
                                                List<Expression> expressions, int i, int size) {
        if (i > size - 1) {
            return new BoolLiteral(true);
        }

        ExprId exprId = ((NamedExpression) expressions.get(i)).getExprId();
        Expr lhs = exprIdToExpr.get(exprId);
        Expr rhs = exprIdToSlotRef.get(exprId);

        Expr bothNull = new CompoundPredicate(CompoundPredicate.Operator.AND,
                new IsNullPredicate(lhs, false, true), new IsNullPredicate(rhs, false, true));
        Expr lhsEqRhsNotNull = new CompoundPredicate(CompoundPredicate.Operator.AND,
                new CompoundPredicate(CompoundPredicate.Operator.AND,
                        new IsNullPredicate(lhs, true, true), new IsNullPredicate(rhs, true, true)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs,
                        Type.BOOLEAN, NullableMode.DEPEND_ON_ARGUMENT));

        Expr remainder = windowExprsHaveMatchedNullable(exprIdToExpr, exprIdToSlotRef, expressions, i + 1, size);
        return new CompoundPredicate(CompoundPredicate.Operator.AND,
                new CompoundPredicate(CompoundPredicate.Operator.OR, bothNull, lhsEqRhsNotNull), remainder);
    }

    private DataPartition hashSpecToDataPartition(PhysicalDistribute distribute, PlanTranslatorContext context) {
        Preconditions.checkState(distribute.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash hashSpec = (DistributionSpecHash) distribute.getDistributionSpec();
        List<Expr> partitions = hashSpec.getOrderedShuffledColumns().stream()
                .map(exprId -> context.findSlotRef(exprId))
                .collect(Collectors.toList());
        return new DataPartition(TPartitionType.HASH_PARTITIONED, partitions);
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

    private PlanFragment createPlanFragment(PlanNode planNode, DataPartition dataPartition, AbstractPlan physicalPlan) {
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), planNode, dataPartition);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        return planFragment;
    }

    private void setPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.setPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    private void addPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.addPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    private void updateLegacyPlanIdToPhysicalPlan(PlanNode planNode, AbstractPlan physicalPlan) {
        if (statsErrorEstimator != null) {
            statsErrorEstimator.updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        }
    }

    private SlotDescriptor injectRowIdColumnSlot(TupleDescriptor tupleDesc) {
        SlotDescriptor slotDesc = context.addSlotDesc(tupleDesc);
        LOG.debug("inject slot {}", slotDesc);
        String name = Column.ROWID_COL;
        Column col = new Column(name, Type.STRING, false, null, false, "", "rowid column");
        slotDesc.setType(Type.STRING);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(false);
        slotDesc.setIsMaterialized(true);
        return slotDesc;
    }
}
