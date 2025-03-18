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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNotnullErrorToInvalid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullableErrorToNull;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLoadProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * NereidsStreamLoadPlanner
 */
public class NereidsStreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(NereidsStreamLoadPlanner.class);

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private NereidsLoadTaskInfo taskInfo;
    private ScanNode scanNode;

    public NereidsStreamLoadPlanner(Database db, OlapTable destTable, NereidsLoadTaskInfo taskInfo) {
        this.db = db;
        this.destTable = destTable;
        this.taskInfo = taskInfo;
    }

    // can only be called after "plan()", or it will return null
    public OlapTable getDestTable() {
        return destTable;
    }

    // the caller should get table read lock when call this method
    public TPipelineFragmentParams plan(TUniqueId loadId) throws UserException {
        return this.plan(loadId, 1);
    }

    /**
     * the caller should get table read lock when call this method
     * create the plan. the plan's query id and load id are same, using the parameter 'loadId'
     */
    public TPipelineFragmentParams plan(TUniqueId loadId, int fragmentInstanceIdIndex) throws UserException {
        if (destTable.getKeysType() != KeysType.UNIQUE_KEYS
                && taskInfo.getMergeType() != LoadTask.MergeType.APPEND) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (taskInfo.getMergeType() != LoadTask.MergeType.APPEND
                && !destTable.hasDeleteSign()) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }
        TUniqueKeyUpdateMode uniquekeyUpdateMode = taskInfo.getUniqueKeyUpdateMode();
        if (uniquekeyUpdateMode != TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && destTable.hasSequenceCol() && !taskInfo.hasSequenceCol() && destTable.getSequenceMapCol() == null) {
            throw new UserException("Table " + destTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (!destTable.hasSequenceCol() && taskInfo.hasSequenceCol()) {
            throw new UserException("There is no sequence column in the table " + destTable.getName());
        }

        boolean negative = taskInfo.getNegative();
        // get partial update related info
        if (uniquekeyUpdateMode != TUniqueKeyUpdateMode.UPSERT && !destTable.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Only unique key merge on write support partial update");
        }

        // try to convert to upsert if only has missing auto-increment key column
        boolean hasMissingColExceptAutoIncKey = false;
        if (taskInfo.getColumnExprDescs().descs.isEmpty()
                && uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
        }

        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                || uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
            boolean hasSyncMaterializedView = destTable.getFullSchema().stream()
                    .anyMatch(col -> col.isMaterializedViewColumn());
            if (hasSyncMaterializedView) {
                throw new DdlException("Can't do partial update on merge-on-write Unique table"
                        + " with sync materialized view.");
            }
            if (destTable.isUniqKeyMergeOnWriteWithClusterKeys()) {
                throw new UserException("Can't do partial update on merge-on-write Unique table with cluster keys");
            }
        }

        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS && !destTable.hasSkipBitmapColumn()) {
            String tblName = destTable.getName();
            throw new UserException("Flexible partial update can only support table with skip bitmap hidden column."
                    + " But table " + tblName + " doesn't have it. You can use `ALTER TABLE " + tblName
                    + " ENABLE FEATURE \"UPDATE_FLEXIBLE_COLUMNS\";` to add it to the table.");
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && !destTable.getEnableLightSchemaChange()) {
            throw new UserException("Flexible partial update can only support table with light_schema_change enabled."
                    + " But table " + destTable.getName() + "'s property light_schema_change is false");
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && destTable.hasVariantColumns()) {
            throw new UserException("Flexible partial update can only support table without variant columns.");
        }
        HashSet<String> partialUpdateInputColumns = new HashSet<>();
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            for (Column col : destTable.getFullSchema()) {
                boolean existInExpr = false;
                if (col.hasOnUpdateDefaultValue()) {
                    partialUpdateInputColumns.add(col.getName());
                }
                for (NereidsImportColumnDesc importColumnDesc : taskInfo.getColumnExprDescs().descs) {
                    if (importColumnDesc.getColumnName() != null
                            && importColumnDesc.getColumnName().equals(col.getName())) {
                        if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                            throw new UserException("Partial update should not include invisible column except"
                                    + " delete sign column: " + col.getName());
                        }
                        partialUpdateInputColumns.add(col.getName());
                        if (destTable.hasSequenceCol()
                                && (taskInfo.hasSequenceCol() || (destTable.getSequenceMapCol() != null
                                        && destTable.getSequenceMapCol().equalsIgnoreCase(col.getName())))) {
                            partialUpdateInputColumns.add(Column.SEQUENCE_COL);
                        }
                        existInExpr = true;
                        break;
                    }
                }
                if (!existInExpr) {
                    if (col.isKey() && !col.isAutoInc()) {
                        throw new UserException("Partial update should include all key columns, missing: "
                                + col.getName());
                    }
                    if (!(col.isKey() && col.isAutoInc()) && col.isVisible()) {
                        hasMissingColExceptAutoIncKey = true;
                    }
                }

                if (!col.getGeneratedColumnsThatReferToThis().isEmpty()
                        && col.getGeneratedColumnInfo() == null && !existInExpr) {
                    throw new UserException("Partial update should include"
                            + " all ordinary columns referenced"
                            + " by generated columns, missing: " + col.getName());
                }
            }
            if (taskInfo.getMergeType() == LoadTask.MergeType.DELETE
                    || taskInfo.getMergeType() == LoadTask.MergeType.MERGE) {
                partialUpdateInputColumns.add(Column.DELETE_SIGN);
            }
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS && !hasMissingColExceptAutoIncKey) {
            uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
        }
        // here we should be full schema to fill the descriptor table
        for (Column col : destTable.getFullSchema()) {
            if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                    && !partialUpdateInputColumns.contains(col.getName())) {
                continue;
            }
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggregateType. column:" + col.getName());
            }
        }
        // 1. create file group
        NereidsDataDescription dataDescription = new NereidsDataDescription(destTable.getName(), taskInfo);
        dataDescription.analyzeWithoutCheckPriv(db.getFullName());
        NereidsBrokerFileGroup fileGroup = new NereidsBrokerFileGroup(dataDescription);
        fileGroup.setWhereExpr(taskInfo.getWhereExpr());
        fileGroup.parse(db, dataDescription);
        // 2. create dummy file status
        TBrokerFileStatus fileStatus = new TBrokerFileStatus();
        if (taskInfo.getFileType() == TFileType.FILE_LOCAL) {
            fileStatus.setPath(taskInfo.getPath());
            fileStatus.setIsDir(false);
            fileStatus.setSize(taskInfo.getFileSize()); // must set to -1, means stream.
        } else {
            fileStatus.setPath("");
            fileStatus.setIsDir(false);
            fileStatus.setSize(-1); // must set to -1, means stream.
        }
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(loadId, taskInfo.getTxnId(), destTable,
                BrokerDesc.createForStreamLoad(), fileGroup,
                fileStatus, taskInfo.isStrictMode(), taskInfo.getFileType(), taskInfo.getHiddenColumns(),
                uniquekeyUpdateMode, destTable.getSequenceMapCol());
        NereidsLoadScanProvider loadScanProvider = new NereidsLoadScanProvider(fileGroupInfo,
                partialUpdateInputColumns);
        NereidsParamCreateContext context = loadScanProvider.createLoadContext();
        LogicalPlan streamLoadPlan = (LogicalPlan) createStreamLoadPlan(fileGroupInfo, dataDescription, context,
                hasMissingColExceptAutoIncKey);
        NereidsLoadPlanTranslator loadPlanTranslator = new NereidsLoadPlanTranslator(destTable, taskInfo, loadId,
                db.getId(), uniquekeyUpdateMode, partialUpdateInputColumns, context.exprMap);
        NereidsLoadPlanTranslator.LoadPlanInfo loadPlanInfo = loadPlanTranslator.translatePlan(streamLoadPlan);
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), loadPlanInfo.getDestTuple());
        fileScanNode.finalizeForNereids(loadId, Lists.newArrayList(fileGroupInfo), Lists.newArrayList(context),
                loadPlanInfo);
        scanNode = fileScanNode;

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(loadPlanInfo.getOlapTableSink());

        fragment.finalize(null);

        TPipelineFragmentParams params = new TPipelineFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDescTbl(loadPlanInfo.getDescriptorTable().toThrift());
        params.setCoord(new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port));
        params.setCurrentConnectFe(new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port));

        TPipelineInstanceParams execParams = new TPipelineInstanceParams();
        // user load id (streamLoadTask.id) as query id
        params.setQueryId(loadId);
        execParams.setFragmentInstanceId(new TUniqueId(loadId.hi, loadId.lo + fragmentInstanceIdIndex));
        params.per_exch_num_senders = Maps.newHashMap();
        params.destinations = Lists.newArrayList();
        Map<Integer, List<TScanRangeParams>> perNodeScanRange = Maps.newHashMap();
        List<TScanRangeParams> scanRangeParams = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            scanRangeParams.add(new TScanRangeParams(locations.getScanRange()));
        }
        // For stream load, only one sender
        execParams.setSenderId(0);
        params.setNumSenders(1);
        perNodeScanRange.put(scanNode.getId().asInt(), scanRangeParams);
        execParams.setPerNodeScanRanges(perNodeScanRange);
        params.addToLocalParams(execParams);
        params.setLoadStreamPerNode(taskInfo.getStreamPerNode());
        params.setTotalLoadStreams(taskInfo.getStreamPerNode());
        params.setNumLocalSink(1);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQueryType(TQueryType.LOAD);
        int timeout = taskInfo.getTimeout();
        queryOptions.setQueryTimeout(timeout);
        queryOptions.setExecutionTimeout(timeout);
        if (timeout < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
        queryOptions.setMemLimit(taskInfo.getMemLimit());
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setLoadMemLimit(taskInfo.getMemLimit());
        // load
        queryOptions.setBeExecVersion(Config.be_exec_version);
        queryOptions.setIsReportSuccess(taskInfo.getEnableProfile());
        queryOptions.setEnableProfile(taskInfo.getEnableProfile());
        boolean enableMemtableOnSinkNode = destTable.getTableProperty().getUseSchemaLightChange()
                ? taskInfo.isMemtableOnSinkNode()
                : false;
        queryOptions.setEnableMemtableOnSinkNode(enableMemtableOnSinkNode);
        params.setQueryOptions(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setTimeZone(taskInfo.getTimezone());
        queryGlobals.setLoadZeroTolerance(taskInfo.getMaxFilterRatio() <= 0.0);
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());

        params.setQueryGlobals(queryGlobals);
        params.setTableName(destTable.getName());
        params.setIsMowTable(destTable.getEnableUniqueKeyMergeOnWrite());
        return params;
    }

    private Plan createStreamLoadPlan(NereidsFileGroupInfo fileGroupInfo, NereidsDataDescription dataDescription,
            NereidsParamCreateContext context, boolean isPartialUpdate) throws UserException {
        List<NamedExpression> projects = new ArrayList<>(context.exprMap.size());
        List<String> colNames = new ArrayList<>(context.exprMap.size());
        Set<String> uniqueColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Expression> entry : context.exprMap.entrySet()) {
            projects.add(new Alias(entry.getValue(), entry.getKey()));
            colNames.add(entry.getKey());
            uniqueColNames.add(entry.getKey());
        }
        Table targetTable = fileGroupInfo.getTargetTable();
        for (SlotReference slotReference : context.scanSlots) {
            String colName = slotReference.getName();
            if (targetTable.getColumn(colName) != null && !uniqueColNames.contains(colName)) {
                projects.add(slotReference);
                colNames.add(colName);
                uniqueColNames.add(colName);
            }
        }

        LogicalPlan currentRootPlan = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                Lists.newArrayList(context.scanSlots));
        if (context.fileGroup.getPrecedingFilterExpr() != null) {
            Set<Expression> conjuncts = new HashSet<>();
            conjuncts.add(context.fileGroup.getPrecedingFilterExpr());
            currentRootPlan = new LogicalPreFilter<>(conjuncts, currentRootPlan);
        }
        if (!projects.isEmpty()) {
            currentRootPlan = new LogicalLoadProject(projects, currentRootPlan);
        }
        if (context.fileGroup.getWhereExpr() != null) {
            Set<Expression> conjuncts = new HashSet<>();
            conjuncts.add(context.fileGroup.getWhereExpr());
            currentRootPlan = new LogicalFilter<>(conjuncts, currentRootPlan);
        }
        PartitionNames partitionNames = dataDescription.getPartitionNames();
        currentRootPlan = UnboundTableSinkCreator.createUnboundTableSink(targetTable.getFullQualifiers(), colNames,
                ImmutableList.of(),
                partitionNames != null && partitionNames.isTemp(),
                partitionNames != null ? partitionNames.getPartitionNames() : ImmutableList.of(), isPartialUpdate,
                DMLCommandType.LOAD, currentRootPlan);

        CascadesContext cascadesContext = CascadesContext.initContext(new StatementContext(), currentRootPlan,
                PhysicalProperties.ANY);
        try {
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                    ImmutableList.of(Rewriter.bottomUp(new BindExpression(),
                            new LoadProjectRewrite(fileGroupInfo.getTargetTable()),
                            new BindSink(), new PushDownProjectThroughFilter(), new MergeProjects(),
                            new ExpressionNormalization())))
                    .execute();
        } catch (Exception exception) {
            throw new UserException(exception.getMessage());
        }

        Plan boundPlan = cascadesContext.getRewritePlan();
        return boundPlan;
    }

    /** LoadProjectExpressionRewrite */
    private class LoadProjectRewrite extends OneRewriteRuleFactory {
        private Table tbl;

        public LoadProjectRewrite(Table tbl) {
            this.tbl = tbl;
        }

        @Override
        public Rule build() {
            return logicalLoadProject().thenApply(ctx -> {
                LogicalLoadProject<Plan> project = ctx.root;
                List<NamedExpression> projects = project.getProjects();
                List<NamedExpression> newProjects = new ArrayList<>(projects.size());
                for (NamedExpression expression : projects) {
                    Column column = tbl.getColumn(expression.getName());
                    if (column != null) {
                        if (column.getAggregationType() != null) {
                            if (column.getAggregationType() == AggregateType.BITMAP_UNION
                                    && !expression.getDataType().isBitmapType()) {
                                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                        String.format("bitmap column %s require the function return type is BITMAP",
                                                column.getName()));
                            }
                            if (column.getAggregationType() == AggregateType.QUANTILE_UNION
                                    && !expression.getDataType().isQuantileStateType()) {
                                throw new org.apache.doris.nereids.exceptions.AnalysisException(String.format(
                                        "quantile_state column %s require the function return type is QUANTILE_STATE",
                                        column.getName()));
                            }
                        }
                        if (column.getType().isJsonbType() && expression.getDataType().isStringLikeType()) {
                            Expression realExpr = expression instanceof Alias ? ((Alias) expression).child()
                                    : expression;
                            if (column.isAllowNull() || expression.nullable()) {
                                newProjects.add(
                                        new Alias(new JsonbParseNullableErrorToNull(realExpr), expression.getName()));
                            } else {
                                newProjects.add(
                                        new Alias(new JsonbParseNotnullErrorToInvalid(realExpr), expression.getName()));
                            }
                        } else {
                            newProjects.add(expression);
                        }
                    } else {
                        throw new org.apache.doris.nereids.exceptions.AnalysisException(String
                                .format("can not find column %s in table %s", expression.getName(), tbl.getName()));
                    }
                }
                return new LogicalProject(newProjects, project.child());
            }).toRule(RuleType.REWRITE_LOAD_PROJECT_FOR_STREAM_LOAD);
        }
    }

    private class PushDownProjectThroughFilter extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject(logicalFilter()).thenApply(ctx -> {
                LogicalProject<LogicalFilter<Plan>> project = ctx.root;
                Map<Slot, Expression> replaceMap = Maps.newHashMap();
                for (NamedExpression output : project.getOutputs()) {
                    if (output instanceof Alias) {
                        Set<Slot> inputSlots = output.getInputSlots();
                        int size = inputSlots.size();
                        if (size == 1) {
                            replaceMap.put(inputSlots.iterator().next(), output.toSlot());
                        } else if (size > 1) {
                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                    String.format("expression %s is not supported", output));
                        }
                    }
                }
                LogicalFilter<Plan> filter = project.child();
                Plan filterChild = filter.child();
                return filter.withConjunctsAndChild(ExpressionUtils.replace(filter.getConjuncts(), replaceMap),
                        project.withChildren(filterChild));
            }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_FILTER);
        }
    }
}
