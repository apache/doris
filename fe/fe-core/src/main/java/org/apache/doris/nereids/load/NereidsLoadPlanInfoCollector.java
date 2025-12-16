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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPostProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.planner.GroupCommitBlockSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * visit logical plan tree and store required information in LoadPlanInfo
 */
public class NereidsLoadPlanInfoCollector extends DefaultPlanVisitor<Void, PlanTranslatorContext> {

    /**
     * store OlapTableSink and required information for FileLoadScanNode
     */
    public static class LoadPlanInfo {
        // the file source tuple's id, the tuple represents original columns reading from file
        private TupleId srcTupleId;
        // the file source tuple's slots
        private List<SlotId> srcSlotIds;
        // FileLoadScanNode's output tuple, represents remapped columns
        // For multiple file groups in Broker load, this destTuple is shared across all file groups
        private TupleDescriptor destTuple;
        // the map of slots in destTuple and its mapping expression
        private Map<SlotId, Expr> destSlotIdToExprMap;
        // if the file source slot has a same name with dest table and no corresponding mapping expr,
        // it means the column data will be imported to dest table without any transformation.
        // we put the dest slot and src slot without transformation to destSlotIdToSrcSlotIdWithoutTrans
        private Map<SlotId, SlotId> destSlotIdToSrcSlotIdWithoutTrans;
        // the file source slot without transformation must have default value,
        // the default value is its corresponding dest column's default value or null literal
        private Map<SlotId, Expr> srcSlotIdToDefaultValueMap;
        // the filter before column transformation
        private List<Expr> preFilterExprList;
        // the filter after column transformation
        private List<Expr> postFilterExprList;
        // OlapTableSink for dest table
        private OlapTableSink olapTableSink;

        public TupleDescriptor getDestTuple() {
            return destTuple;
        }

        public List<Expr> getPreFilterExprList() {
            return preFilterExprList;
        }

        public List<Expr> getPostFilterExprList() {
            return postFilterExprList;
        }

        public OlapTableSink getOlapTableSink() {
            return olapTableSink;
        }

        /**
         * create a TFileScanRangeParams
         */
        public TFileScanRangeParams toFileScanRangeParams(TUniqueId loadId, NereidsFileGroupInfo fileGroupInfo) {
            NereidsBrokerFileGroup fileGroup = fileGroupInfo.getFileGroup();
            TFileScanRangeParams params = new TFileScanRangeParams();

            params.setFileType(fileGroupInfo.getFileType());

            params.setSrcTupleId(srcTupleId.asInt());

            params.setDestTupleId(destTuple.getId().asInt());

            int columnCountFromPath = 0;
            if (fileGroup.getColumnNamesFromPath() != null) {
                columnCountFromPath = fileGroup.getColumnNamesFromPath().size();
            }
            int numColumnsFromFile = srcSlotIds.size() - columnCountFromPath;
            Preconditions.checkState(numColumnsFromFile >= 0,
                    "srcSlotIds.size is: " + srcSlotIds.size() + ", num columns from path: "
                            + columnCountFromPath);
            params.setNumOfColumnsFromFile(numColumnsFromFile);

            for (int i = 0; i < srcSlotIds.size(); ++i) {
                TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
                slotInfo.setSlotId(srcSlotIds.get(i).asInt());
                slotInfo.setIsFileSlot(i < numColumnsFromFile);
                params.addToRequiredSlots(slotInfo);
            }

            Map<String, String> properties = fileGroupInfo.getBrokerDesc().getBackendConfigProperties();
            if (fileGroupInfo.getBrokerDesc().getFileType() == TFileType.FILE_HDFS) {
                THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(properties);
                params.setHdfsParams(tHdfsParams);
            }

            params.setProperties(properties);

            for (Map.Entry<SlotId, Expr> entry : destSlotIdToExprMap.entrySet()) {
                params.putToExprOfDestSlot(entry.getKey().asInt(), entry.getValue().treeToThrift());
            }

            for (Map.Entry<SlotId, Expr> entry : srcSlotIdToDefaultValueMap.entrySet()) {
                if (entry.getValue() != null) {
                    params.putToDefaultValueOfSrcSlot(entry.getKey().asInt(), entry.getValue().treeToThrift());
                } else {
                    TExpr tExpr = new TExpr();
                    tExpr.setNodes(Lists.newArrayList());
                    params.putToDefaultValueOfSrcSlot(entry.getKey().asInt(), tExpr);
                }
            }

            for (Map.Entry<SlotId, SlotId> entry : destSlotIdToSrcSlotIdWithoutTrans.entrySet()) {
                params.putToDestSidToSrcSidWithoutTrans(entry.getKey().asInt(), entry.getValue().asInt());
            }

            params.setStrictMode(fileGroupInfo.isStrictMode());

            TFileAttributes fileAttributes = fileGroup.getFileFormatProperties().toTFileAttributes();
            fileAttributes.setReadByColumnDef(true);
            fileAttributes.setIgnoreCsvRedundantCol(fileGroup.getIgnoreCsvRedundantCol());
            params.setFileAttributes(fileAttributes);

            if (preFilterExprList != null) {
                for (Expr conjunct : preFilterExprList) {
                    params.addToPreFilterExprsList(conjunct.treeToThrift());
                }
            }

            params.setLoadId(loadId);

            if (fileGroupInfo.getSequenceMapCol() != null) {
                params.setSequenceMapCol(fileGroupInfo.getSequenceMapCol());
            }

            return params;
        }
    }

    private LoadPlanInfo loadPlanInfo;
    private OlapTable destTable;
    private NereidsLoadTaskInfo taskInfo;
    private TUniqueId loadId;
    private long dbId;
    private TUniqueKeyUpdateMode uniquekeyUpdateMode;
    private TPartialUpdateNewRowPolicy uniquekeyUpdateNewRowPolicy;
    private HashSet<String> partialUpdateInputColumns;
    private Map<String, Expression> exprMap;

    private LogicalPlan logicalPlan;
    private Map<String, SlotDescriptor> srcSlots = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private List<Slot> partitionSlots = Lists.newArrayList();
    private Expression filterPredicate;

    /**
     * NereidsLoadPlanTranslator
     */
    public NereidsLoadPlanInfoCollector(OlapTable destTable, NereidsLoadTaskInfo taskInfo, TUniqueId loadId, long dbId,
            TUniqueKeyUpdateMode uniquekeyUpdateMode, TPartialUpdateNewRowPolicy uniquekeyUpdateNewRowPolicy,
            HashSet<String> partialUpdateInputColumns,
            Map<String, Expression> exprMap) {
        loadPlanInfo = new LoadPlanInfo();
        this.destTable = destTable;
        this.taskInfo = taskInfo;
        this.loadId = loadId;
        this.dbId = dbId;
        this.uniquekeyUpdateMode = uniquekeyUpdateMode;
        this.uniquekeyUpdateNewRowPolicy = uniquekeyUpdateNewRowPolicy;
        this.partialUpdateInputColumns = partialUpdateInputColumns;
        this.exprMap = exprMap;
    }

    /**
     * visit logical plan tree and create a LoadPlanInfo
     */
    public LoadPlanInfo collectLoadPlanInfo(LogicalPlan logicalPlan, DescriptorTable descTable,
            TupleDescriptor scanDescriptor) {
        this.logicalPlan = logicalPlan;
        CascadesContext cascadesContext = CascadesContext.initContext(new StatementContext(),
                logicalPlan, PhysicalProperties.ANY);
        PlanTranslatorContext context = new PlanTranslatorContext(cascadesContext, descTable);
        loadPlanInfo.destTuple = scanDescriptor;
        logicalPlan.accept(this, context);
        return loadPlanInfo;
    }

    @Override
    public Void visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> logicalOlapTableSink,
            PlanTranslatorContext context) {
        logicalOlapTableSink.child().accept(this, context);
        List<Slot> targetTableSlots = logicalOlapTableSink.getTargetTableSlots();
        List<SlotDescriptor> destSlots = loadPlanInfo.destTuple.getSlots();
        Preconditions.checkState(targetTableSlots.size() == destSlots.size());

        loadPlanInfo.destSlotIdToSrcSlotIdWithoutTrans = Maps.newHashMap();
        for (int i = 0; i < targetTableSlots.size(); ++i) {
            if (targetTableSlots.get(i).isColumnFromTable()) {
                SlotDescriptor srcSlot = srcSlots
                        .get(((SlotReference) targetTableSlots.get(i)).getOriginalColumn().get().getName());
                if (srcSlot != null) {
                    loadPlanInfo.destSlotIdToSrcSlotIdWithoutTrans.put(destSlots.get(i).getId(), srcSlot.getId());
                }
            }
        }

        TupleDescriptor olapTuple = generateTupleDesc(logicalOlapTableSink.getTargetTableSlots(),
                logicalOlapTableSink.getTargetTable(), context);
        List<Expr> partitionExprs = logicalOlapTableSink.getPartitionExprList().stream()
                .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());
        Map<Long, Expr> syncMvWhereClauses = new HashMap<>();
        for (Map.Entry<Long, Expression> entry : logicalOlapTableSink.getSyncMvWhereClauses().entrySet()) {
            syncMvWhereClauses.put(entry.getKey(), ExpressionTranslator.translate(entry.getValue(), context));
        }
        List<Long> partitionIds;
        try {
            partitionIds = getAllPartitionIds();
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        boolean enableMemtableOnSinkNode = destTable.getTableProperty().getUseSchemaLightChange()
                ? taskInfo.isMemtableOnSinkNode()
                : false;
        boolean enableSingleReplicaLoad = enableMemtableOnSinkNode ? false : Config.enable_single_replica_load;
        if (taskInfo instanceof NereidsStreamLoadTask && ((NereidsStreamLoadTask) taskInfo).getGroupCommit() != null) {
            loadPlanInfo.olapTableSink = new GroupCommitBlockSink(destTable, olapTuple, partitionIds,
                    enableSingleReplicaLoad, partitionExprs, syncMvWhereClauses,
                    ((NereidsStreamLoadTask) taskInfo).getGroupCommit(),
                    taskInfo.getMaxFilterRatio());
        } else {
            loadPlanInfo.olapTableSink = new OlapTableSink(destTable, olapTuple, partitionIds, enableSingleReplicaLoad,
                    partitionExprs, syncMvWhereClauses);
        }
        int timeout = taskInfo.getTimeout();
        try {
            loadPlanInfo.olapTableSink.init(loadId, taskInfo.getTxnId(), dbId, timeout,
                    taskInfo.getSendBatchParallelism(),
                    taskInfo.isLoadToSingleTablet(), taskInfo.isStrictMode(), timeout, uniquekeyUpdateMode,
                    uniquekeyUpdateNewRowPolicy, partialUpdateInputColumns);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        return null;
    }

    @Override
    public Void visitLogicalProject(LogicalProject<? extends Plan> logicalProject, PlanTranslatorContext context) {
        logicalProject.child().accept(this, context);
        List<NamedExpression> outputs = logicalProject.getOutputs();
        for (NamedExpression expr : outputs) {
            if (expr.containsType(AggregateFunction.class)) {
                throw new AnalysisException("Don't support aggregation function in load expression");
            }
        }

        List<SlotDescriptor> slotDescriptorList = loadPlanInfo.destTuple.getSlots();
        loadPlanInfo.destSlotIdToExprMap = Maps.newHashMap();
        for (int i = 0; i < slotDescriptorList.size(); ++i) {
            DataType targetType = DataType.fromCatalogType(slotDescriptorList.get(i).getType());
            Expression output = outputs.get(i);
            if (!(targetType.isJsonType() && output.getDataType().isStringLikeType())) {
                if (output instanceof Alias) {
                    output = TypeCoercionUtils.castIfNotSameType(((Alias) output).child(), targetType);
                } else {
                    output = TypeCoercionUtils.castIfNotSameType(output, targetType);
                }
                if (output instanceof Cast && output.getDataType().isAggStateType()) {
                    output = ConvertAggStateCast.convert((Cast) output);
                }
            }
            Expr expr = ExpressionTranslator.translate(output, context);
            loadPlanInfo.destSlotIdToExprMap.put(slotDescriptorList.get(i).getId(), expr);
        }
        return null;
    }

    @Override
    public Void visitLogicalPostProject(LogicalPostProject<? extends Plan> logicalPostProject,
                                        PlanTranslatorContext context) {
        List<NamedExpression> outputs = logicalPostProject.getOutputs();
        for (NamedExpression expr : outputs) {
            if (expr.containsType(AggregateFunction.class)) {
                throw new AnalysisException("Don't support aggregation function in load expression");
            }
        }

        // For Broker load with multiple file groups, all file groups share the same destTuple.
        // Create slots for destTuple only when processing the first file group (when slots are empty).
        // Subsequent file groups will reuse the slots created by the first file group.
        if (loadPlanInfo.destTuple.getSlots().isEmpty()) {
            List<Slot> slotList = outputs.stream().map(NamedExpression::toSlot).collect(Collectors.toList());
            for (Slot slot : slotList) {
                context.createSlotDesc(loadPlanInfo.destTuple, (SlotReference) slot, destTable);
            }
        }
        logicalPostProject.child().accept(this, context);
        return null;
    }

    @Override
    public Void visitLogicalFilter(LogicalFilter<? extends Plan> logicalFilter, PlanTranslatorContext context) {
        logicalFilter.child().accept(this, context);
        loadPlanInfo.postFilterExprList = new ArrayList<>(logicalFilter.getConjuncts().size());
        for (Expression conjunct : logicalFilter.getConjuncts()) {
            Expr expr = ExpressionTranslator.translate(conjunct, context);
            loadPlanInfo.postFilterExprList.add(expr);
        }
        filterPredicate = logicalFilter.getPredicate();
        for (Column c : destTable.getPartitionColumns()) {
            Slot partitionSlot = null;
            for (Slot slot : logicalFilter.getOutput()) {
                if (slot.getName().equalsIgnoreCase(c.getName())) {
                    partitionSlot = slot;
                    break;
                }
            }
            if (partitionSlot != null) {
                partitionSlots.add(partitionSlot);
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalPreFilter(LogicalPreFilter<? extends Plan> logicalPreFilter,
            PlanTranslatorContext context) {
        logicalPreFilter.child(0).accept(this, context);
        loadPlanInfo.preFilterExprList = new ArrayList<>(logicalPreFilter.getConjuncts().size());
        logicalPreFilter.getConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(loadPlanInfo.preFilterExprList::add);
        return null;
    }

    @Override
    public Void visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation,
            PlanTranslatorContext context) {
        List<Slot> slots = oneRowRelation.getLogicalProperties().getOutput();
        TupleDescriptor oneRowTuple = generateTupleDesc(slots, null, context);

        loadPlanInfo.srcTupleId = oneRowTuple.getId();
        loadPlanInfo.srcSlotIds = new ArrayList<>(oneRowTuple.getAllSlotIds());
        loadPlanInfo.srcSlotIdToDefaultValueMap = Maps.newHashMap();
        for (SlotDescriptor slotDescriptor : oneRowTuple.getSlots()) {
            Column column = destTable.getColumn(slotDescriptor.getColumn().getName());
            if (column != null) {
                Expression expression;
                if (column.getDefaultValue() != null) {
                    expression = new NereidsParser().parseExpression(column.getDefaultValueSql());
                    ExpressionAnalyzer analyzer = new ExpressionAnalyzer(
                            null, new Scope(ImmutableList.of()), null, true, true);
                    expression = analyzer.analyze(expression);
                } else {
                    if (column.isAllowNull()) {
                        expression = new NullLiteral(VarcharType.SYSTEM_DEFAULT);
                    } else {
                        expression = null;
                    }
                }
                if (exprMap.containsKey(column.getName())) {
                    continue;
                }
                Expr expr = null;
                if (expression != null) {
                    expression = TypeCoercionUtils.castIfNotSameType(expression,
                            DataType.fromCatalogType(slotDescriptor.getType()));
                    expr = ExpressionTranslator.translate(expression, context);
                }
                loadPlanInfo.srcSlotIdToDefaultValueMap.put(slotDescriptor.getId(), expr);
                srcSlots.put(column.getName(), slotDescriptor);
            }
        }
        return null;
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, TableIf table, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot, table);
        }
        return tupleDescriptor;
    }

    // get all specified partition ids.
    // if no partition specified, return null
    private List<Long> getAllPartitionIds() throws DdlException, AnalysisException {
        PartitionNamesInfo partitionNamesInfo = taskInfo.getPartitionNamesInfo();
        if (partitionNamesInfo != null) {
            List<Long> partitionIds = new ArrayList<>(partitionNamesInfo.getPartitionNames().size());
            for (String partName : partitionNamesInfo.getPartitionNames()) {
                Partition part = destTable.getPartition(partName, partitionNamesInfo.isTemp());
                if (part == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_PARTITION, partName, destTable.getName());
                }
                partitionIds.add(part.getId());
            }
            return partitionIds;
        } else {
            if (destTable.getPartitionInfo().getType() != PartitionType.UNPARTITIONED && filterPredicate != null) {
                PartitionInfo partitionInfo = destTable.getPartitionInfo();
                Map<Long, PartitionItem> idToPartitions = partitionInfo.getIdToItem(false);
                Optional<SortedPartitionRanges<Long>> sortedPartitionRanges = Optional.empty();
                List<Long> prunedPartitions = PartitionPruner.prune(
                        partitionSlots, filterPredicate, idToPartitions,
                        CascadesContext.initContext(new StatementContext(), logicalPlan, PhysicalProperties.ANY),
                        PartitionTableType.OLAP, sortedPartitionRanges).first;
                return prunedPartitions;
            } else {
                return null;
            }
        }
    }
}
