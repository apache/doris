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
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.catalog.external.JdbcExternalTable;
import org.apache.doris.catalog.external.MaxComputeExternalTable;
import org.apache.doris.catalog.external.PaimonExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.properties.DistributionSpecExecutionAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.properties.DistributionSpecStorageGather;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileSink;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.EsScanNode;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.IntersectNode;
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
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.TableFunctionNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.planner.external.MaxComputeScanNode;
import org.apache.doris.planner.external.hudi.HudiScanNode;
import org.apache.doris.planner.external.iceberg.IcebergScanNode;
import org.apache.doris.planner.external.jdbc.JdbcScanNode;
import org.apache.doris.planner.external.paimon.PaimonScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    /**
     * Translate Nereids Physical Plan tree to Stale Planner PlanFragment tree.
     *
     * @param physicalPlan Nereids Physical Plan tree
     * @return Stale Planner PlanFragment tree
     */
    public PlanFragment translatePlan(PhysicalPlan physicalPlan) {
        PlanFragment rootFragment = physicalPlan.accept(this, context);
        List<Expr> outputExprs = Lists.newArrayList();
        physicalPlan.getOutput().stream().map(Slot::getExprId)
                .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
        rootFragment.setOutputExprs(outputExprs);
        Collections.reverse(context.getPlanFragments());
        // TODO: maybe we need to trans nullable directly? and then we could remove call computeMemLayout
        context.getDescTable().computeMemLayout();
        return rootFragment;
    }

    /* ********************************************************************************************
     * distribute node
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            PlanTranslatorContext context) {
        PlanFragment inputFragment = distribute.child().accept(this, context);
        // TODO: why need set streaming here? should remove this.
        if (inputFragment.getPlanRoot() instanceof AggregationNode
                && distribute.child() instanceof PhysicalHashAggregate
                && context.getFirstAggregateInFragment(inputFragment) == distribute.child()) {
            PhysicalHashAggregate<?> hashAggregate = (PhysicalHashAggregate<?>) distribute.child();
            if (hashAggregate.getAggPhase() == AggPhase.LOCAL
                    && hashAggregate.getAggMode() == AggMode.INPUT_TO_BUFFER) {
                AggregationNode aggregationNode = (AggregationNode) inputFragment.getPlanRoot();
                aggregationNode.setUseStreamingPreagg(hashAggregate.isMaybeUsingStream());
            }
        }

        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), inputFragment.getPlanRoot());
        updateLegacyPlanIdToPhysicalPlan(exchangeNode, distribute);
        List<ExprId> validOutputIds = distribute.getOutputExprIds();
        if (distribute.child() instanceof PhysicalHashAggregate) {
            // we must add group by keys to output list,
            // otherwise we could not process aggregate's output without group by keys
            List<ExprId> keys = ((PhysicalHashAggregate<?>) distribute.child()).getGroupByExpressions().stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .map(SlotReference::getExprId)
                    .collect(Collectors.toList());
            keys.addAll(validOutputIds);
            validOutputIds = keys;
        }
        DataPartition dataPartition = toDataPartition(distribute.getDistributionSpec(), validOutputIds, context);
        PlanFragment parentFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, dataPartition);
        exchangeNode.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        if (distribute.getDistributionSpec() instanceof DistributionSpecGather) {
            // gather to one instance
            exchangeNode.setNumInstances(1);
        }

        // process multicast sink
        if (inputFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) inputFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            TupleDescriptor tupleDescriptor = generateTupleDesc(distribute.getOutput(), null, context);
            exchangeNode.updateTupleIds(tupleDescriptor);
            dataStreamSink.setExchNodeId(exchangeNode.getId());
            dataStreamSink.setOutputPartition(dataPartition);
            parentFragment.addChild(inputFragment);
            ((MultiCastPlanFragment) inputFragment).addToDest(exchangeNode);

            CTEScanNode cteScanNode = context.getCteScanNodeMap().get(inputFragment.getFragmentId());
            Preconditions.checkState(cteScanNode != null, "cte scan node is null");
            cteScanNode.setFragment(inputFragment);
            cteScanNode.setPlanNodeId(exchangeNode.getId());
            context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator ->
                    runtimeFilterTranslator.getContext().getPlanNodeIdToCTEDataSinkMap()
                            .put(cteScanNode.getId(), dataStreamSink));
        } else {
            inputFragment.setDestination(exchangeNode);
            inputFragment.setOutputPartition(dataPartition);
            DataStreamSink streamSink = new DataStreamSink(exchangeNode.getId());
            streamSink.setOutputPartition(dataPartition);
            inputFragment.setSink(streamSink);
        }

        context.addPlanFragment(parentFragment);
        return parentFragment;
    }

    /* ********************************************************************************************
     * sink Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalResultSink(PhysicalResultSink<? extends Plan> physicalResultSink,
            PlanTranslatorContext context) {
        PlanFragment planFragment = physicalResultSink.child().accept(this, context);
        planFragment.setSink(new ResultSink(planFragment.getPlanRoot().getId()));
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeResultSink(
            PhysicalDeferMaterializeResultSink<? extends Plan> sink,
            PlanTranslatorContext context) {
        PlanFragment planFragment = visitPhysicalResultSink(sink.getPhysicalResultSink(), context);
        TFetchOption fetchOption = sink.getOlapTable().generateTwoPhaseReadOption(sink.getSelectedIndexId());
        ((ResultSink) planFragment.getSink()).setFetchOption(fetchOption);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> olapTableSink,
            PlanTranslatorContext context) {
        PlanFragment rootFragment = olapTableSink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);

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
                olapTableSink.getPartitionIds().isEmpty() ? null : olapTableSink.getPartitionIds(),
                olapTableSink.isSingleReplicaLoad()
        );
        if (olapTableSink.isPartialUpdate()) {
            HashSet<String> partialUpdateCols = new HashSet<>();
            for (Column col : olapTableSink.getCols()) {
                partialUpdateCols.add(col.getName());
            }
            sink.setPartialUpdateInputColumns(true, partialUpdateCols);
        }
        rootFragment.setSink(sink);

        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalFileSink(PhysicalFileSink<? extends Plan> fileSink,
            PlanTranslatorContext context) {
        PlanFragment rootFragment = fileSink.child().accept(this, context);
        OutFileClause outFile = new OutFileClause(
                fileSink.getFilePath(),
                fileSink.getFormat(),
                fileSink.getProperties()
        );

        List<Expr> outputExprs = Lists.newArrayList();
        fileSink.getOutput().stream().map(Slot::getExprId)
                .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
        rootFragment.setOutputExprs(outputExprs);

        // generate colLabels
        List<String> labels = fileSink.getOutput().stream().map(NamedExpression::getName).collect(Collectors.toList());

        // TODO: should not call legacy planner analyze in Nereids
        try {
            outFile.analyze(null, outputExprs, labels);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        ResultFileSink sink = new ResultFileSink(rootFragment.getPlanRoot().getId(), outFile,
                (ArrayList<String>) labels);

        rootFragment.setSink(sink);
        return rootFragment;
    }

    /* ********************************************************************************************
     * scan Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalFileScan(PhysicalFileScan fileScan, PlanTranslatorContext context) {
        List<Slot> slots = fileScan.getOutput();
        ExternalTable table = fileScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);

        // TODO(cmy): determine the needCheckColumnPriv param
        ScanNode scanNode;
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
                    ((HiveScanNode) scanNode).setSelectedPartitions(fileScan.getSelectedPartitions());
                    break;
                default:
                    throw new RuntimeException("do not support DLA type " + ((HMSExternalTable) table).getDlaType());
            }
        } else if (table instanceof IcebergExternalTable) {
            scanNode = new IcebergScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
        } else if (table instanceof PaimonExternalTable) {
            scanNode = new PaimonScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
        } else if (table instanceof MaxComputeExternalTable) {
            scanNode = new MaxComputeScanNode(context.nextPlanNodeId(), tupleDescriptor, false);
        } else {
            throw new RuntimeException("do not support table type " + table.getType());
        }
        scanNode.addConjuncts(translateToLegacyConjuncts(fileScan.getConjuncts()));

        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, table, tableName);
        tupleDescriptor.setRef(tableRef);
        if (fileScan.getStats() != null) {
            scanNode.setCardinality((long) fileScan.getStats().getRowCount());
        }
        Utils.execWithUncheckedException(scanNode::init);
        context.addScanNode(scanNode);
        ScanNode finalScanNode = scanNode;
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(fileScan.getRelationId()).forEach(
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
    public PlanFragment visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, PlanTranslatorContext context) {
        List<Slot> output = emptyRelation.getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(output, null, context);
        for (Slot slot : output) {
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
    public PlanFragment visitPhysicalEsScan(PhysicalEsScan esScan, PlanTranslatorContext context) {
        List<Slot> slots = esScan.getOutput();
        ExternalTable table = esScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        EsScanNode esScanNode = new EsScanNode(context.nextPlanNodeId(), tupleDescriptor, true);
        esScanNode.addConjuncts(translateToLegacyConjuncts(esScan.getConjuncts()));
        Utils.execWithUncheckedException(esScanNode::init);
        context.addScanNode(esScanNode);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(esScan.getRelationId()).forEach(
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

    @Override
    public PlanFragment visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, PlanTranslatorContext context) {
        List<Slot> slots = jdbcScan.getOutput();
        TableIf table = jdbcScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        JdbcScanNode jdbcScanNode = new JdbcScanNode(context.nextPlanNodeId(), tupleDescriptor,
                table instanceof JdbcExternalTable);
        jdbcScanNode.addConjuncts(translateToLegacyConjuncts(jdbcScan.getConjuncts()));
        Utils.execWithUncheckedException(jdbcScanNode::init);
        context.addScanNode(jdbcScanNode);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(jdbcScan.getRelationId()).forEach(
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
    public PlanFragment visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        List<Slot> slots = olapScan.getOutput();
        OlapTable olapTable = olapScan.getTable();
        // generate real output tuple
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, olapTable, context);
        // generate base index tuple because this fragment partitioned expr relay on slots of based index
        if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
            generateTupleDesc(olapScan.getBaseOutputs(), olapTable, context);
        }

        OlapScanNode olapScanNode = new OlapScanNode(context.nextPlanNodeId(), tupleDescriptor, "OlapScanNode");
        // TODO: move all node set cardinality into one place
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

        // TODO:  remove this switch?
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

        // create scan range
        Utils.execWithUncheckedException(olapScanNode::init);
        // TODO: process collect scan node in one place
        context.addScanNode(olapScanNode);
        // TODO: process translate runtime filter in one place
        //   use real plan node to present rf apply and rf generator
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterTranslator -> runtimeFilterTranslator.getTargetOnScanNode(olapScan.getRelationId())
                        .forEach(expr -> runtimeFilterTranslator.translateRuntimeFilterTarget(
                                expr, olapScanNode, context)
                )
        );
        // TODO: we need to remove all finalizeForNereids
        olapScanNode.finalizeForNereids();
        // Create PlanFragment
        // TODO: use a util function to convert distribution to DataPartition
        DataPartition dataPartition = DataPartition.RANDOM;
        if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) olapScan.getDistributionSpec();
            List<Expr> partitionExprs = distributionSpecHash.getOrderedShuffledColumns().stream()
                    .map(context::findSlotRef).collect(Collectors.toList());
            dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
        }
        // TODO: maybe we could have a better way to create fragment
        PlanFragment planFragment = createPlanFragment(olapScanNode, dataPartition, olapScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), olapScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeOlapScan(
            PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan, PlanTranslatorContext context) {
        PlanFragment planFragment = visitPhysicalOlapScan(deferMaterializeOlapScan.getPhysicalOlapScan(), context);
        OlapScanNode olapScanNode = (OlapScanNode) planFragment.getPlanRoot();
        TupleDescriptor tupleDescriptor = context.getTupleDesc(olapScanNode.getTupleId());
        for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
            if (deferMaterializeOlapScan.getDeferMaterializeSlotIds()
                    .contains(context.findExprId(slotDescriptor.getId()))) {
                slotDescriptor.setNeedMaterialize(false);
            }
        }
        context.createSlotDesc(tupleDescriptor, deferMaterializeOlapScan.getColumnIdSlot());
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
            slotDescriptor.setIsNullable(slots.get(i).nullable());
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
    public PlanFragment visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, PlanTranslatorContext context) {
        Table table = schemaScan.getTable();
        List<Slot> slots = ImmutableList.copyOf(schemaScan.getOutput());
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        SchemaScanNode scanNode = new SchemaScanNode(context.nextPlanNodeId(), tupleDescriptor);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(schemaScan.getRelationId())
                        .forEach(expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, scanNode, context)
                )
        );
        scanNode.finalizeForNereids();
        context.addScanNode(scanNode);
        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, schemaScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), schemaScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanTranslatorContext context) {
        List<Slot> slots = tvfRelation.getLogicalProperties().getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, tvfRelation.getFunction().getTable(), context);

        TableValuedFunctionIf catalogFunction = tvfRelation.getFunction().getCatalogFunction();
        ScanNode scanNode = catalogFunction.getScanNode(context.nextPlanNodeId(), tupleDescriptor);
        Utils.execWithUncheckedException(scanNode::init);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getTargetOnScanNode(tvfRelation.getRelationId())
                        .forEach(expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, scanNode, context)
                )
        );
        Utils.execWithUncheckedException(scanNode::finalizeForNereids);
        context.addScanNode(scanNode);

        // TODO: it is weird update label in this way
        // set label for explain
        for (Slot slot : slots) {
            String tableColumnName = "_table_valued_function_" + tvfRelation.getFunction().getName()
                    + "." + slots.get(0).getName();
            context.findSlotRef(slot.getExprId()).setLabel(tableColumnName);
        }

        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, tvfRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), tvfRelation);
        return planFragment;
    }


    /* ********************************************************************************************
     * other Node, in lexicographical order, ignore algorithm name. for example, HashAggregate -> Aggregate
     * ******************************************************************************************** */

    /**
     * Translate Agg.
     */
    @Override
    public PlanFragment visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate,
            PlanTranslatorContext context) {

        PlanFragment inputPlanFragment = aggregate.child(0).accept(this, context);

        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();

        // 1. generate slot reference for each group expression
        List<SlotReference> groupSlots = collectGroupBySlots(groupByExpressions, outputExpressions);
        ArrayList<Expr> execGroupingExpressions = groupByExpressions.stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toCollection(ArrayList::new));
        // 2. collect agg expressions and generate agg function to slot reference map
        List<Slot> aggFunctionOutput = Lists.newArrayList();
        List<AggregateExpression> aggregateExpressionList = outputExpressions.stream()
                .filter(o -> o.anyMatch(AggregateExpression.class::isInstance))
                .peek(o -> aggFunctionOutput.add(o.toSlot()))
                .map(o -> o.<Set<AggregateExpression>>collect(AggregateExpression.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toList());
        ArrayList<FunctionCallExpr> execAggregateFunctions = aggregateExpressionList.stream()
                .map(aggregateFunction -> (FunctionCallExpr) ExpressionTranslator.translate(aggregateFunction, context))
                .collect(Collectors.toCollection(ArrayList::new));

        // 3. generate output tuple
        List<Slot> slotList = Lists.newArrayList();
        TupleDescriptor outputTupleDesc;
        slotList.addAll(groupSlots);
        slotList.addAll(aggFunctionOutput);
        outputTupleDesc = generateTupleDesc(slotList, null, context);

        List<Integer> aggFunOutputIds = ImmutableList.of();
        if (!aggFunctionOutput.isEmpty()) {
            aggFunOutputIds = outputTupleDesc
                    .getSlots()
                    .subList(groupSlots.size(), outputTupleDesc.getSlots().size())
                    .stream()
                    .map(slot -> slot.getId().asInt())
                    .collect(ImmutableList.toImmutableList());
        }
        boolean isPartial = aggregate.getAggregateParam().aggMode.productAggregateBuffer;
        AggregateInfo aggInfo = AggregateInfo.create(execGroupingExpressions, execAggregateFunctions,
                aggFunOutputIds, isPartial, outputTupleDesc, outputTupleDesc, aggregate.getAggPhase().toExec());
        AggregationNode aggregationNode = new AggregationNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), aggInfo);
        if (!aggregate.getAggMode().isFinalPhase) {
            aggregationNode.unsetNeedsFinalize();
        }

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
                throw new RuntimeException("Unsupported agg phase: " + aggregate.getAggPhase());
        }
        // TODO: use to set useStreamingAgg, we should remove it by set it in Nereids
        PhysicalHashAggregate firstAggregateInFragment = context.getFirstAggregateInFragment(inputPlanFragment);
        if (firstAggregateInFragment == null) {
            context.setFirstAggregateInFragment(inputPlanFragment, aggregate);
        }

        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // so, if we do final phase or merge without exchange.
        // we need turn of parallel scan to ensure to get correct result.
        PlanNode leftMostNode = inputPlanFragment.getPlanRoot();
        while (leftMostNode.getChildren().size() != 0 && !(leftMostNode instanceof ExchangeNode)) {
            leftMostNode = leftMostNode.getChild(0);
        }
        // TODO: nereids forbid all parallel scan under aggregate temporary, because nereids could generate
        //  so complex aggregate plan than legacy planner, and should add forbid parallel scan hint when
        //  generate physical aggregate plan.
        //  There is one exception, we use some precondition in optimizer, input to buffer always require any for input,
        //  so when agg mode is INPUT_TO_BUFFER, we do not forbid parallel scan
        if (leftMostNode instanceof OlapScanNode
                && inputPlanFragment.getDataPartition().getType() != TPartitionType.RANDOM
                && aggregate.getAggregateParam().aggMode != AggMode.INPUT_TO_BUFFER) {
            inputPlanFragment.setHasColocatePlanNode(true);
        }
        setPlanRoot(inputPlanFragment, aggregationNode, aggregate);
        if (aggregate.getStats() != null) {
            aggregationNode.setCardinality((long) aggregate.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(inputPlanFragment.getPlanRoot(), aggregate);
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanTranslatorContext context) {
        Preconditions.checkState((storageLayerAggregate.getRelation() instanceof PhysicalOlapScan
                        || storageLayerAggregate.getRelation() instanceof PhysicalFileScan),
                "PhysicalStorageLayerAggregate only support PhysicalOlapScan and PhysicalFileScan: "
                        + storageLayerAggregate.getRelation().getClass().getName());
        PlanFragment planFragment = storageLayerAggregate.getRelation().accept(this, context);

        ScanNode scanNode = (ScanNode) planFragment.getPlanRoot();
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
        scanNode.setPushDownAggNoGrouping(pushAggOp);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), storageLayerAggregate);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = assertNumRows.child().accept(this, context);
        // create assertNode
        AssertNumRowsNode assertNumRowsNode = new AssertNumRowsNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(),
                ExpressionTranslator.translateAssert(assertNumRows.getAssertNumRowsElement()));
        addPlanRoot(currentFragment, assertNumRowsNode, assertNumRows);
        return currentFragment;
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

    @Override
    public PlanFragment visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer,
            PlanTranslatorContext context) {
        CTEId cteId = cteConsumer.getCteId();

        MultiCastPlanFragment multiCastFragment = (MultiCastPlanFragment) context.getCteProduceFragments().get(cteId);
        Preconditions.checkState(multiCastFragment.getSink() instanceof MultiCastDataSink,
                "invalid multiCastFragment");

        MultiCastDataSink multiCastDataSink = (MultiCastDataSink) multiCastFragment.getSink();
        Preconditions.checkState(multiCastDataSink != null, "invalid multiCastDataSink");

        PhysicalCTEProducer<?> cteProducer = context.getCteProduceMap().get(cteId);
        Preconditions.checkState(cteProducer != null, "invalid cteProducer");

        context.getCteConsumerMap().put(cteId, cteConsumer);
        // set datasink to multicast data sink but do not set target now
        // target will be set when translate distribute
        DataStreamSink streamSink = new DataStreamSink();
        streamSink.setFragment(multiCastFragment);
        multiCastDataSink.getDataStreamSinks().add(streamSink);
        multiCastDataSink.getDestinations().add(Lists.newArrayList());

        // update expr to slot mapping
        TupleDescriptor tupleDescriptor = null;
        for (Slot producerSlot : cteProducer.getOutput()) {
            Slot consumerSlot = cteConsumer.getProducerToConsumerSlotMap().get(producerSlot);
            SlotRef slotRef = context.findSlotRef(producerSlot.getExprId());
            tupleDescriptor = slotRef.getDesc().getParent();
            context.addExprIdSlotRefPair(consumerSlot.getExprId(), slotRef);
        }
        CTEScanNode cteScanNode = new CTEScanNode(tupleDescriptor);
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator ->
                    runtimeFilterTranslator.getTargetOnScanNode(cteConsumer.getRelationId()).forEach(
                            expr -> runtimeFilterTranslator.translateRuntimeFilterTarget(expr, cteScanNode, context)));
        context.getCteScanNodeMap().put(multiCastFragment.getFragmentId(), cteScanNode);

        return multiCastFragment;
    }

    @Override
    public PlanFragment visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> cteProducer,
            PlanTranslatorContext context) {
        PlanFragment child = cteProducer.child().accept(this, context);
        CTEId cteId = cteProducer.getCteId();
        context.getPlanFragments().remove(child);

        MultiCastPlanFragment multiCastPlanFragment = new MultiCastPlanFragment(child);
        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        multiCastPlanFragment.setSink(multiCastDataSink);

        List<Expr> outputs = cteProducer.getOutput().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        multiCastPlanFragment.setOutputExprs(outputs);
        context.getCteProduceFragments().put(cteId, multiCastPlanFragment);
        context.getCteProduceMap().put(cteId, cteProducer);
        if (context.getRuntimeTranslator().isPresent()) {
            context.getRuntimeTranslator().get().getContext().getCteProduceMap().put(cteId, cteProducer);
        }
        context.getPlanFragments().add(multiCastPlanFragment);
        return child;
    }

    @Override
    public PlanFragment visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanTranslatorContext context) {
        if (filter.child(0) instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) filter.child();
            join.addFilterConjuncts(filter.getConjuncts());
        }
        PlanFragment inputFragment = filter.child(0).accept(this, context);

        // process multicast sink
        if (inputFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) inputFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            filter.getConjuncts().stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .forEach(dataStreamSink::addConjunct);
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

    /**
     * the contract of hash join node with BE
     * 1. hash join contains 3 types of predicates:
     *   a. equal join conjuncts
     *   b. other join conjuncts
     *   c. other predicates (denoted by filter conjuncts in the rest of comments)
     * <p>
     * 2. hash join contains 3 tuple descriptors
     *   a. input tuple descriptors, corresponding to the left child output and right child output.
     *      If its column is selected, it will be displayed in explain by `tuple ids`.
     *      for example, select L.* from L join R on ..., because no column from R are selected, tuple ids only
     *      contains output tuple of L.
     *      equal join conjuncts is bound on input tuple descriptors.
     * <p>
     *   b.intermediate tuple.
     *      This tuple describes schema of the output block after evaluating equal join conjuncts
     *      and other join conjuncts.
     * <p>
     *      Other join conjuncts currently is bound on intermediate tuple. There are some historical reason, and it
     *      should be bound on input tuple in the future.
     * <p>
     *      filter conjuncts will be evaluated on the intermediate tuple. That means the input block of filter is
     *      described by intermediate tuple, and hence filter conjuncts should be bound on intermediate tuple.
     * <p>
     *      In order to be compatible with old version, intermediate tuple is not pruned. For example, intermediate
     *      tuple contains all slots from both sides of children. After probing hash-table, BE does not need to
     *      materialize all slots in intermediate tuple. The slots in HashJoinNode.hashOutputSlotIds will be
     *      materialized by BE. If `hashOutputSlotIds` is empty, all slots will be materialized.
     * <p>
     *      In case of outer join, the slots in intermediate should be set nullable.
     *      For example,
     *      select L.*, R.* from L left outer join R on ...
     *      All slots from R in intermediate tuple should be nullable.
     * <p>
     *   c. output tuple
     *      This describes the schema of hash join output block.
     * 3. Intermediate tuple
     *      for BE performance reason, the slots in intermediate tuple
     *      depends on the join type and other join conjuncts.
     *      In general, intermediate tuple contains all slots of both children, except one case.
     *      For left-semi/left-ant (right-semi/right-semi) join without other join conjuncts, intermediate tuple
     *      only contains left (right) children output slots.
     *
     */
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

        PlanFragment currentFragment = connectJoinNode(hashJoinNode, leftFragment, rightFragment, context, hashJoin);
        if (JoinUtils.shouldColocateJoin(physicalHashJoin)) {
            // TODO: add reason
            hashJoinNode.setColocate(true, "");
            leftFragment.setHasColocatePlanNode(true);
        } else if (JoinUtils.shouldBroadcastJoin(physicalHashJoin)) {
            Preconditions.checkState(rightPlanRoot instanceof ExchangeNode,
                    "right child of broadcast join must be ExchangeNode but it is " + rightFragment.getPlanRoot());
            Preconditions.checkState(rightFragment.getChildren().size() == 1,
                    "right child of broadcast join must have 1 child, but meet " + rightFragment.getChildren().size());
            ((ExchangeNode) rightPlanRoot).setRightChildOfBroadcastHashJoin(true);
            hashJoinNode.setDistributionMode(DistributionMode.BROADCAST);
        } else if (JoinUtils.shouldBucketShuffleJoin(physicalHashJoin)) {
            hashJoinNode.setDistributionMode(DistributionMode.BUCKET_SHUFFLE);
        } else {
            hashJoinNode.setDistributionMode(DistributionMode.PARTITIONED);
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), leftSlotDescriptor.getId());
                    }
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), rightSlotDescriptor.getId());
                    }
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), leftSlotDescriptor.getId());
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), rightSlotDescriptor.getId());
                    }
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        }

        if (hashJoin.getMarkJoinSlotReference().isPresent()) {
            SlotReference sf = hashJoin.getMarkJoinSlotReference().get();
            outputSlotReferences.add(sf);
            context.createSlotDesc(intermediateDescriptor, sf);
            if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                SlotRef markJoinSlotId = context.findSlotRef(sf.getExprId());
                Preconditions.checkState(markJoinSlotId != null);
                hashJoinNode.addSlotIdToHashOutputSlotIds(markJoinSlotId.getSlotId());
                hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), markJoinSlotId.getSlotId());
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
            nestedLoopJoinNode.setChild(0, leftFragment.getPlanRoot());
            nestedLoopJoinNode.setChild(1, rightFragment.getPlanRoot());
            setPlanRoot(leftFragment, nestedLoopJoinNode, nestedLoopJoin);
            // TODO: what's this? do we really need to set this?
            rightFragment.getPlanRoot().setCompactData(false);
            context.mergePlanFragment(rightFragment, leftFragment);
            for (PlanFragment rightChild : rightFragment.getChildren()) {
                leftFragment.addChild(rightChild);
            }
            // translate runtime filter
            context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> {
                Set<RuntimeFilter> filters = runtimeFilterTranslator
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
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
                    sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
                    //sd = context.createSlotDesc(intermediateDescriptor, sf);
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
            updateLegacyPlanIdToPhysicalPlan(leftFragment.getPlanRoot(), nestedLoopJoin);
            return leftFragment;
        } else {
            throw new RuntimeException("Physical nested loop join could not execute with equal join condition.");
        }
    }

    @Override
    public PlanFragment visitPhysicalLimit(PhysicalLimit<? extends Plan> physicalLimit, PlanTranslatorContext context) {
        PlanFragment inputFragment = physicalLimit.child(0).accept(this, context);
        PlanNode child = inputFragment.getPlanRoot();
        child.setOffset(physicalLimit.getOffset());
        child.setLimit(physicalLimit.getLimit());
        updateLegacyPlanIdToPhysicalPlan(child, physicalLimit);
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN,
            PlanTranslatorContext context) {
        PlanFragment inputFragment = partitionTopN.child(0).accept(this, context);
        PartitionSortNode partitionSortNode = translatePartitionSortNode(
                partitionTopN, inputFragment.getPlanRoot(), context);
        addPlanRoot(inputFragment, partitionSortNode, partitionTopN);
        return inputFragment;
    }

    // TODO: generate expression mapping when be project could do in ExecNode.
    @Override
    public PlanFragment visitPhysicalProject(PhysicalProject<? extends Plan> project, PlanTranslatorContext context) {
        if (project.child(0) instanceof AbstractPhysicalJoin) {
            ((AbstractPhysicalJoin<?, ?>) project.child(0)).setShouldTranslateOutput(false);
        }
        if (project.child(0) instanceof PhysicalFilter) {
            if (project.child(0).child(0) instanceof AbstractPhysicalJoin) {
                ((AbstractPhysicalJoin<?, ?>) project.child(0).child(0)).setShouldTranslateOutput(false);
            }
        }

        PlanFragment inputFragment = project.child(0).accept(this, context);

        List<Expr> projectionExprs = project.getProjects()
                .stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        List<Slot> slots = project.getProjects()
                .stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toList());

        // process multicast sink
        if (inputFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) inputFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            TupleDescriptor projectionTuple = generateTupleDesc(slots, null, context);
            dataStreamSink.setProjections(projectionExprs);
            dataStreamSink.setOutputTupleDesc(projectionTuple);
            return inputFragment;
        }

        PlanNode inputPlanNode = inputFragment.getPlanRoot();
        List<Expr> conjuncts = inputPlanNode.getConjuncts();
        Set<SlotId> requiredSlotIdSet = Sets.newHashSet();
        for (Expr expr : projectionExprs) {
            Expr.extractSlots(expr, requiredSlotIdSet);
        }
        Set<SlotId> requiredByProjectSlotIdSet = Sets.newHashSet(requiredSlotIdSet);
        for (Expr expr : conjuncts) {
            Expr.extractSlots(expr, requiredSlotIdSet);
        }
        // For hash join node, use vSrcToOutputSMap to describe the expression calculation, use
        // vIntermediateTupleDescList as input, and set vOutputTupleDesc as the final output.
        // TODO: HashJoinNode's be implementation is not support projection yet, remove this after when supported.
        if (inputPlanNode instanceof JoinNodeBase) {
            TupleDescriptor tupleDescriptor = generateTupleDesc(slots, null, context);
            JoinNodeBase joinNode = (JoinNodeBase) inputPlanNode;
            joinNode.setvOutputTupleDesc(tupleDescriptor);
            joinNode.setvSrcToOutputSMap(projectionExprs);
            // prune the hashOutputSlotIds
            if (joinNode instanceof HashJoinNode) {
                ((HashJoinNode) joinNode).getHashOutputSlotIds().clear();
                Set<ExprId> requiredExprIds = Sets.newHashSet();
                Set<SlotId> requiredOtherConjunctsSlotIdSet = Sets.newHashSet();
                List<Expr> otherConjuncts = ((HashJoinNode) joinNode).getOtherJoinConjuncts();
                for (Expr expr : otherConjuncts) {
                    Expr.extractSlots(expr, requiredOtherConjunctsSlotIdSet);
                }
                requiredOtherConjunctsSlotIdSet.forEach(e -> requiredExprIds.add(context.findExprId(e)));
                requiredSlotIdSet.forEach(e -> requiredExprIds.add(context.findExprId(e)));
                for (ExprId exprId : requiredExprIds) {
                    SlotId slotId = ((HashJoinNode) joinNode).getHashOutputExprSlotIdMap().get(exprId);
                    Preconditions.checkState(slotId != null);
                    ((HashJoinNode) joinNode).addSlotIdToHashOutputSlotIds(slotId);
                }
            }
            return inputFragment;
        }

        if (inputPlanNode instanceof TableFunctionNode) {
            TableFunctionNode tableFunctionNode = (TableFunctionNode) inputPlanNode;
            tableFunctionNode.setOutputSlotIds(Lists.newArrayList(requiredSlotIdSet));
        }

        if (inputPlanNode instanceof ScanNode) {
            TupleDescriptor projectionTuple = null;
            // slotIdsByOrder is used to ensure the ScanNode's output order is same with current Project
            // if we change the output order in translate project, the upper node will receive wrong order
            // tuple, since they get the order from project.getOutput() not scan.getOutput()./
            List<SlotId> slotIdsByOrder = Lists.newArrayList();
            if (requiredByProjectSlotIdSet.size() != requiredSlotIdSet.size()
                    || new HashSet<>(projectionExprs).size() != projectionExprs.size()
                    || projectionExprs.stream().anyMatch(expr -> !(expr instanceof SlotRef))) {
                projectionTuple = generateTupleDesc(slots,
                                  ((ScanNode) inputPlanNode).getTupleDesc().getTable(), context);
                inputPlanNode.setProjectList(projectionExprs);
                inputPlanNode.setOutputTupleDesc(projectionTuple);
            } else {
                for (int i = 0; i < slots.size(); ++i) {
                    context.addExprIdSlotRefPair(slots.get(i).getExprId(),
                            (SlotRef) projectionExprs.get(i));
                    slotIdsByOrder.add(((SlotRef) projectionExprs.get(i)).getSlotId());
                }
            }

            // TODO: this is a temporary scheme to support two phase read when has project.
            //  we need to refactor all topn opt into rbo stage.
            if (inputPlanNode instanceof OlapScanNode) {
                ArrayList<SlotDescriptor> olapScanSlots =
                        context.getTupleDesc(inputPlanNode.getTupleIds().get(0)).getSlots();
                SlotDescriptor lastSlot = olapScanSlots.get(olapScanSlots.size() - 1);
                if (lastSlot.getColumn() != null
                        && lastSlot.getColumn().getName().equals(Column.ROWID_COL)) {
                    if (projectionTuple != null) {
                        injectRowIdColumnSlot(projectionTuple);
                        SlotRef slotRef = new SlotRef(lastSlot);
                        inputPlanNode.getProjectList().add(slotRef);
                        requiredByProjectSlotIdSet.add(lastSlot.getId());
                    } else {
                        slotIdsByOrder.add(lastSlot.getId());
                    }
                    requiredSlotIdSet.add(lastSlot.getId());
                }
            }
            updateScanSlotsMaterialization((ScanNode) inputPlanNode, requiredSlotIdSet,
                    requiredByProjectSlotIdSet, slotIdsByOrder, context);
        } else {
            TupleDescriptor tupleDescriptor = generateTupleDesc(slots, null, context);
            inputPlanNode.setProjectList(projectionExprs);
            inputPlanNode.setOutputTupleDesc(tupleDescriptor);
        }
        return inputFragment;
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
        for (Plan plan : setOperation.children()) {
            childrenFragments.add(plan.accept(this, context));
        }

        TupleDescriptor setTuple = generateTupleDesc(setOperation.getOutput(), null, context);
        List<SlotDescriptor> outputSlotDescs = new ArrayList<>(setTuple.getSlots());

        SetOperationNode setOperationNode;
        // create setOperationNode
        if (setOperation instanceof PhysicalUnion) {
            setOperationNode = new UnionNode(context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalExcept) {
            setOperationNode = new ExceptNode(context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalIntersect) {
            setOperationNode = new IntersectNode(context.nextPlanNodeId(), setTuple.getId());
        } else {
            throw new RuntimeException("not support set operation type " + setOperation);
        }

        setOperation.children().stream()
                .map(Plan::getOutput)
                .map(l -> l.stream()
                        .map(e -> ExpressionTranslator.translate(e, context))
                        .collect(ImmutableList.toImmutableList()))
                .forEach(setOperationNode::addResultExprLists);
        if (setOperation instanceof PhysicalUnion) {
            ((PhysicalUnion) setOperation).getConstantExprsList().stream()
                    .map(l -> l.stream()
                            .map(e -> ExpressionTranslator.translate(e, context))
                            .collect(ImmutableList.toImmutableList()))
                    .forEach(setOperationNode::addConstExprList);
        }

        for (PlanFragment childFragment : childrenFragments) {
            setOperationNode.addChild(childFragment.getPlanRoot());
        }
        setOperationNode.finalizeForNereids(outputSlotDescs, outputSlotDescs);

        PlanFragment setOperationFragment;
        if (childrenFragments.isEmpty()) {
            setOperationFragment = createPlanFragment(setOperationNode,
                    DataPartition.UNPARTITIONED, setOperation);
            context.addPlanFragment(setOperationFragment);
        } else {
            int childrenSize = childrenFragments.size();
            setOperationFragment = childrenFragments.get(childrenSize - 1);
            for (int i = childrenSize - 2; i >= 0; i--) {
                context.mergePlanFragment(childrenFragments.get(i), setOperationFragment);
                for (PlanFragment child : childrenFragments.get(i).getChildren()) {
                    setOperationFragment.addChild(child);
                }
            }
            setPlanRoot(setOperationFragment, setOperationNode, setOperation);
        }

        return setOperationFragment;
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

        // 2. According to the type of sort, generate physical plan
        if (!sort.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add sortNode
            SortNode sortNode = translateSortNode(sort, inputFragment.getPlanRoot(), context);
            addPlanRoot(inputFragment, sortNode, sort);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(inputFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., localSort -> mergeSort
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                return inputFragment;
            }
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot().getChild(0);
            ((ExchangeNode) inputFragment.getPlanRoot()).setMergeInfo(sortNode.getSortInfo());
        }
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanTranslatorContext context) {
        PlanFragment inputFragment = topN.child(0).accept(this, context);

        // 2. According to the type of sort, generate physical plan
        if (!topN.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add TopNNode
            SortNode sortNode = translateSortNode(topN, inputFragment.getPlanRoot(), context);
            sortNode.setOffset(topN.getOffset());
            sortNode.setLimit(topN.getLimit());
            if (topN.isEnableRuntimeFilter()) {
                sortNode.setUseTopnOpt(true);
                PlanNode child = sortNode.getChild(0);
                Preconditions.checkArgument(child instanceof OlapScanNode,
                        "topN opt expect OlapScanNode, but we get " + child);
                OlapScanNode scanNode = ((OlapScanNode) child);
                scanNode.setUseTopnOpt(true);
            }
            // push sort to scan opt
            if (sortNode.getChild(0) instanceof OlapScanNode) {
                OlapScanNode scanNode = ((OlapScanNode) sortNode.getChild(0));
                if (checkPushSort(sortNode, scanNode.getOlapTable())) {
                    SortInfo sortInfo = sortNode.getSortInfo();
                    scanNode.setSortInfo(sortInfo);
                    scanNode.getSortInfo().setSortTupleSlotExprs(sortNode.getResolvedTupleExprs());
                    for (Expr expr : sortInfo.getOrderingExprs()) {
                        scanNode.getSortInfo().addMaterializedOrderingExpr(expr);
                    }
                    if (sortNode.getOffset() > 0) {
                        scanNode.setSortLimit(sortNode.getLimit() + sortNode.getOffset());
                    } else {
                        scanNode.setSortLimit(sortNode.getLimit());
                    }
                }
            }
            addPlanRoot(inputFragment, sortNode, topN);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(inputFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., mergeTopN -> localTopN
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                inputFragment.getPlanRoot().setOffset(topN.getOffset());
                inputFragment.getPlanRoot().setLimit(topN.getLimit());
                return inputFragment;
            }
            ExchangeNode exchangeNode = (ExchangeNode) inputFragment.getPlanRoot();
            exchangeNode.setMergeInfo(((SortNode) exchangeNode.getChild(0)).getSortInfo());
            exchangeNode.setLimit(topN.getLimit());
            exchangeNode.setOffset(topN.getOffset());
        }
        updateLegacyPlanIdToPhysicalPlan(inputFragment.getPlanRoot(), topN);
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            PlanTranslatorContext context) {

        PlanFragment planFragment = visitPhysicalTopN(topN.getPhysicalTopN(), context);
        if (planFragment.getPlanRoot() instanceof SortNode) {
            SortNode sortNode = (SortNode) planFragment.getPlanRoot();
            sortNode.setUseTwoPhaseReadOpt(true);
            sortNode.getSortInfo().setUseTwoPhaseRead();
            TupleDescriptor tupleDescriptor = sortNode.getSortInfo().getSortTupleDescriptor();
            for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
                if (topN.getDeferMaterializeSlotIds()
                        .contains(context.findExprId(slotDescriptor.getId()))) {
                    slotDescriptor.setNeedMaterialize(false);
                }
            }
        }
        return planFragment;
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

        List<Set<Integer>> repeatSlotIdList = repeat.computeRepeatSlotIdList(getSlotIds(outputTuple));
        Set<Integer> allSlotId = repeatSlotIdList.stream()
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());

        RepeatNode repeatNode = new RepeatNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), groupingInfo, repeatSlotIdList,
                allSlotId, repeat.computeVirtualSlotValues(sortedVirtualSlots));
        addPlanRoot(inputPlanFragment, repeatNode, repeat);
        updateLegacyPlanIdToPhysicalPlan(inputPlanFragment.getPlanRoot(), repeat);
        return inputPlanFragment;
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
                        AggregateParam param = AggregateParam.LOCAL_RESULT;
                        function = new AggregateExpression((AggregateFunction) function, param);
                    }
                    return ExpressionTranslator.translate(function, context);
                })
                .map(FunctionCallExpr.class::cast)
                .peek(fnCall -> {
                    fnCall.setIsAnalyticFnCall(true);
                    ((org.apache.doris.catalog.AggregateFunction) fnCall.getFn()).setIsAnalyticFn(true);
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
                orderKeyList.stream().map(UnaryNode::child).collect(Collectors.toList()),
                orderByElements.stream().map(OrderByElement::getExpr).collect(Collectors.toList()),
                bufferedSlotRefForWindow);

        // 3. generate tupleDesc
        List<Slot> windowSlotList = windowFunctionList.stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toList());
        TupleDescriptor outputTupleDesc = generateTupleDesc(windowSlotList, null, context);

        // 4. generate AnalyticEvalNode
        AnalyticEvalNode analyticEvalNode = new AnalyticEvalNode(
                context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(),
                analyticFnCalls,
                partitionExprs,
                orderByElements,
                analyticWindow,
                outputTupleDesc,
                outputTupleDesc,
                partitionExprsIsNullableMatched,
                orderElementsIsNullableMatched,
                bufferedTupleDesc
        );
        inputPlanFragment.addPlanRoot(analyticEvalNode);
        return inputPlanFragment;
    }

    /* ********************************************************************************************
     * private functions
     * ******************************************************************************************** */

    private PartitionSortNode translatePartitionSortNode(PhysicalPartitionTopN<? extends Plan> partitionTopN,
            PlanNode childNode, PlanTranslatorContext context) {
        List<Expr> partitionExprs = partitionTopN.getPartitionKeys().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        // partition key should on child tuple, sort key should on partition top's tuple
        TupleDescriptor sortTuple = generateTupleDesc(partitionTopN.child().getOutput(), null, context);
        List<Expr> orderingExprs = Lists.newArrayList();
        List<Boolean> ascOrders = Lists.newArrayList();
        List<Boolean> nullsFirstParams = Lists.newArrayList();
        List<OrderKey> orderKeys = partitionTopN.getOrderKeys();
        orderKeys.forEach(k -> {
            orderingExprs.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrders.add(k.isAsc());
            nullsFirstParams.add(k.isNullFirst());
        });
        SortInfo sortInfo = new SortInfo(orderingExprs, ascOrders, nullsFirstParams, sortTuple);
        PartitionSortNode partitionSortNode = new PartitionSortNode(context.nextPlanNodeId(), childNode,
                partitionTopN.getFunction(), partitionExprs, sortInfo, partitionTopN.hasGlobalLimit(),
                partitionTopN.getPartitionLimit());
        if (partitionTopN.getStats() != null) {
            partitionSortNode.setCardinality((long) partitionTopN.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(partitionSortNode, partitionTopN);
        return partitionSortNode;
    }

    private SortNode translateSortNode(AbstractPhysicalSort<? extends Plan> sort, PlanNode childNode,
            PlanTranslatorContext context) {
        TupleDescriptor sortTuple = generateTupleDesc(sort.child().getOutput(), null, context);
        List<Expr> orderingExprs = Lists.newArrayList();
        List<Boolean> ascOrders = Lists.newArrayList();
        List<Boolean> nullsFirstParams = Lists.newArrayList();
        List<OrderKey> orderKeys = sort.getOrderKeys();
        orderKeys.forEach(k -> {
            orderingExprs.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrders.add(k.isAsc());
            nullsFirstParams.add(k.isNullFirst());
        });
        SortInfo sortInfo = new SortInfo(orderingExprs, ascOrders, nullsFirstParams, sortTuple);
        SortNode sortNode = new SortNode(context.nextPlanNodeId(), childNode, sortInfo, sort instanceof PhysicalTopN);
        if (sort.getStats() != null) {
            sortNode.setCardinality((long) sort.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(sortNode, sort);
        return sortNode;
    }

    private void updateScanSlotsMaterialization(ScanNode scanNode,
            Set<SlotId> requiredSlotIdSet, Set<SlotId> requiredByProjectSlotIdSet,
            List<SlotId> slotIdsByOrder, PlanTranslatorContext context) {
        // TODO: use smallest slot if do not need any slot in upper node
        SlotDescriptor smallest = scanNode.getTupleDesc().getSlots().get(0);
        if (CollectionUtils.isNotEmpty(slotIdsByOrder)) {
            // if we eliminate project above scan, we should ensure the slot order of scan's output is same with
            // the projection's output. So, we need to reorder the output slot in scan's tuple.
            Map<SlotId, SlotDescriptor> idToSlotDescMap = scanNode.getTupleDesc().getSlots().stream()
                    .filter(s -> requiredSlotIdSet.contains(s.getId()))
                    .collect(Collectors.toMap(SlotDescriptor::getId, s -> s));
            scanNode.getTupleDesc().getSlots().clear();
            for (SlotId slotId : slotIdsByOrder) {
                scanNode.getTupleDesc().getSlots().add(idToSlotDescMap.get(slotId));
            }
        } else {
            scanNode.getTupleDesc().getSlots().removeIf(s -> !requiredSlotIdSet.contains(s.getId()));
        }
        if (scanNode.getTupleDesc().getSlots().isEmpty()) {
            scanNode.getTupleDesc().getSlots().add(smallest);
        }
        try {
            scanNode.updateRequiredSlots(context, requiredByProjectSlotIdSet);
        } catch (UserException e) {
            Util.logAndThrowRuntimeException(LOG,
                    "User Exception while reset external file scan node contexts.", e);
        }
    }

    private void addConjunctsToPlanNode(PhysicalFilter<? extends Plan> filter,
            PlanNode planNode,
            PlanTranslatorContext context) {
        filter.getConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(planNode::addConjunct);
        updateLegacyPlanIdToPhysicalPlan(planNode, filter);
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, TableIf table, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot, table);
        }
        return tupleDescriptor;
    }

    private PlanFragment connectJoinNode(HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context, AbstractPlan join) {
        hashJoinNode.setChild(0, leftFragment.getPlanRoot());
        hashJoinNode.setChild(1, rightFragment.getPlanRoot());
        setPlanRoot(leftFragment, hashJoinNode, join);
        context.mergePlanFragment(rightFragment, leftFragment);
        for (PlanFragment rightChild : rightFragment.getChildren()) {
            leftFragment.addChild(rightChild);
        }
        return leftFragment;
    }

    private List<SlotReference> collectGroupBySlots(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions) {
        List<SlotReference> groupSlots = Lists.newArrayList();
        Set<VirtualSlotReference> virtualSlotReferences = groupByExpressions.stream()
                .filter(VirtualSlotReference.class::isInstance)
                .map(VirtualSlotReference.class::cast)
                .collect(Collectors.toSet());
        for (Expression e : groupByExpressions) {
            if (e instanceof SlotReference && outputExpressions.stream().anyMatch(o -> o.anyMatch(e::equals))) {
                groupSlots.add((SlotReference) e);
            } else if (e instanceof SlotReference && !virtualSlotReferences.isEmpty()) {
                // When there is a virtualSlot, it is a groupingSets scenario,
                // and the original exprId should be retained at this time.
                groupSlots.add((SlotReference) e);
            } else {
                groupSlots.add(new SlotReference(e.toSql(), e.getDataType(), e.nullable(), ImmutableList.of()));
            }
        }
        return groupSlots;
    }

    private List<Integer> getSlotIds(TupleDescriptor tupleDescriptor) {
        return tupleDescriptor.getSlots()
                .stream()
                .map(slot -> slot.getId().asInt())
                .collect(ImmutableList.toImmutableList());
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
                .map(UnaryNode::child)
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

    private PlanFragment createPlanFragment(PlanNode planNode, DataPartition dataPartition, AbstractPlan physicalPlan) {
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), planNode, dataPartition);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        return planFragment;
    }

    // TODO: refactor this, call it every where is not a good way
    private void setPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.setPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    // TODO: refactor this, call it every where is not a good way
    private void addPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.addPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    private DataPartition toDataPartition(DistributionSpec distributionSpec,
            List<ExprId> childOutputIds, PlanTranslatorContext context) {
        if (distributionSpec instanceof DistributionSpecAny
                || distributionSpec instanceof DistributionSpecStorageAny
                || distributionSpec instanceof DistributionSpecExecutionAny) {
            return DataPartition.RANDOM;
        } else if (distributionSpec instanceof DistributionSpecGather
                || distributionSpec instanceof DistributionSpecStorageGather
                || distributionSpec instanceof DistributionSpecReplicated) {
            return DataPartition.UNPARTITIONED;
        } else if (distributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) distributionSpec;
            List<Expr> partitionExprs = Lists.newArrayList();
            for (int i = 0; i < distributionSpecHash.getEquivalenceExprIds().size(); i++) {
                Set<ExprId> equivalenceExprId = distributionSpecHash.getEquivalenceExprIds().get(i);
                for (ExprId exprId : equivalenceExprId) {
                    if (childOutputIds.contains(exprId)) {
                        partitionExprs.add(context.findSlotRef(exprId));
                        break;
                    }
                }
                if (partitionExprs.size() != i + 1) {
                    throw new RuntimeException("Cannot translate DistributionSpec to DataPartition,"
                            + " DistributionSpec: " + distributionSpec
                            + ", child output: " + childOutputIds);
                }
            }
            TPartitionType partitionType;
            switch (distributionSpecHash.getShuffleType()) {
                case STORAGE_BUCKETED:
                    partitionType = TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED;
                    break;
                case EXECUTION_BUCKETED:
                    partitionType = TPartitionType.HASH_PARTITIONED;
                    break;
                case NATURAL:
                default:
                    throw new RuntimeException("Do not support shuffle type: "
                            + distributionSpecHash.getShuffleType());
            }
            return new DataPartition(partitionType, partitionExprs);
        } else {
            throw new RuntimeException("Unknown DistributionSpec: " + distributionSpec);
        }
    }

    // TODO: refactor this, call it every where is not a good way
    private void updateLegacyPlanIdToPhysicalPlan(PlanNode planNode, AbstractPlan physicalPlan) {
        if (statsErrorEstimator != null) {
            statsErrorEstimator.updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        }
    }

    private void injectRowIdColumnSlot(TupleDescriptor tupleDesc) {
        SlotDescriptor slotDesc = context.addSlotDesc(tupleDesc);
        LOG.debug("inject slot {}", slotDesc);
        String name = Column.ROWID_COL;
        Column col = new Column(name, Type.STRING, false, null, false, "", "rowid column");
        slotDesc.setType(Type.STRING);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(false);
        slotDesc.setIsMaterialized(true);
    }

    /**
     * topN opt: using storage data ordering to accelerate topn operation.
     * refer pr: optimize topn query if order by columns is prefix of sort keys of table (#10694)
     */
    private boolean checkPushSort(SortNode sortNode, OlapTable olapTable) {
        // Ensure limit is less than threshold
        if (sortNode.getLimit() <= 0
                || sortNode.getLimit() > ConnectContext.get().getSessionVariable().topnOptLimitThreshold) {
            return false;
        }

        // Ensure all isAscOrder is same, ande length != 0. Can't be z-order.
        if (sortNode.getSortInfo().getIsAscOrder().stream().distinct().count() != 1 || olapTable.isZOrderSort()) {
            return false;
        }

        // Tablet's order by key only can be the front part of schema.
        // Like: schema: a.b.c.d.e.f.g order by key: a.b.c (no a,b,d)
        // Do **prefix match** to check if order by key can be pushed down.
        // olap order by key: a.b.c.d
        // sort key: (a) (a,b) (a,b,c) (a,b,c,d) is ok
        //           (a,c) (a,c,d), (a,c,b) (a,c,f) (a,b,c,d,e)is NOT ok
        List<Expr> sortExprs = sortNode.getSortInfo().getOrderingExprs();
        List<Boolean> nullsFirsts = sortNode.getSortInfo().getNullsFirst();
        List<Boolean> isAscOrders = sortNode.getSortInfo().getIsAscOrder();
        if (sortExprs.size() > olapTable.getDataSortInfo().getColNum()) {
            return false;
        }
        for (int i = 0; i < sortExprs.size(); i++) {
            // table key.
            Column tableKey = olapTable.getFullSchema().get(i);
            // sort slot.
            Expr sortExpr = sortExprs.get(i);
            if (sortExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) sortExpr;
                if (tableKey.equals(slotRef.getColumn())) {
                    // ORDER BY DESC NULLS FIRST can not be optimized to only read file tail,
                    // since NULLS is at file head but data is at tail
                    if (tableKey.isAllowNull() && nullsFirsts.get(i) && !isAscOrders.get(i)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        return true;
    }

    private List<Expr> translateToLegacyConjuncts(Set<Expression> conjuncts) {
        List<Expr> outputExprs = Lists.newArrayList();
        if (conjuncts != null) {
            conjuncts.stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                    .forEach(outputExprs::add);
        }
        return outputExprs;
    }
}
