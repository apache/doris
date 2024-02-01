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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.planner.external.FileGroupInfo;
import org.apache.doris.planner.external.FileScanNode;
import org.apache.doris.planner.external.LoadScanProvider;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * FileLoadScanNode for broker load and stream load.
 */
public class FileLoadScanNode extends FileScanNode {
    private static final Logger LOG = LogManager.getLogger(FileLoadScanNode.class);

    public static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TupleDescriptor destTupleDescriptor;
        // === Set when init ===
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, SlotDescriptor> srcSlotDescByName;
        public Map<String, Expr> exprMap;
        public String timezone;
        // === Set when init ===
        public TFileScanRangeParams params;
    }

    // Save all info about load attributes and files.
    // Each DataDescription in a load stmt conreponding to a FileGroupInfo in this list.
    private final List<FileGroupInfo> fileGroupInfos = Lists.newArrayList();
    // For load, the num of providers equals to the num of file group infos.
    private final List<LoadScanProvider> scanProviders = Lists.newArrayList();
    // For load, the num of ParamCreateContext equals to the num of file group infos.
    private final List<ParamCreateContext> contexts = Lists.newArrayList();

    /**
     * External file scan node for load from file
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public FileLoadScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "FILE_LOAD_SCAN_NODE", StatisticalType.FILE_SCAN_NODE, false);
    }

    // Only for broker load job.
    public void setLoadInfo(long loadJobId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatusesList,
            int filesAdded, boolean strictMode, int loadParallelism, UserIdentity userIdentity) {
        Preconditions.checkState(fileGroups.size() == fileStatusesList.size());
        for (int i = 0; i < fileGroups.size(); ++i) {
            FileGroupInfo fileGroupInfo = new FileGroupInfo(loadJobId, txnId, targetTable, brokerDesc,
                    fileGroups.get(i), fileStatusesList.get(i), filesAdded, strictMode, loadParallelism);
            fileGroupInfos.add(fileGroupInfo);
        }
    }

    // Only for stream load/routine load job.
    public void setLoadInfo(TUniqueId loadId, long txnId, Table targetTable, BrokerDesc brokerDesc,
                            BrokerFileGroup fileGroup, TBrokerFileStatus fileStatus, boolean strictMode,
                            TFileType fileType, List<String> hiddenColumns, boolean isPartialUpdate) {
        FileGroupInfo fileGroupInfo = new FileGroupInfo(loadId, txnId, targetTable, brokerDesc,
                fileGroup, fileStatus, strictMode, fileType, hiddenColumns, isPartialUpdate);
        fileGroupInfos.add(fileGroupInfo);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        for (FileGroupInfo fileGroupInfo : fileGroupInfos) {
            this.scanProviders.add(new LoadScanProvider(fileGroupInfo, desc));
        }
        initParamCreateContexts(analyzer);
    }

    // For each scan provider, create a corresponding ParamCreateContext
    private void initParamCreateContexts(Analyzer analyzer) throws UserException {
        for (LoadScanProvider scanProvider : scanProviders) {
            ParamCreateContext context = scanProvider.createContext(analyzer);
            // set where and preceding filter.
            // FIXME(cmy): we should support set different expr for different file group.
            initAndSetPrecedingFilter(context.fileGroup.getPrecedingFilterExpr(), context.srcTupleDescriptor, analyzer);
            initAndSetWhereExpr(context.fileGroup.getWhereExpr(), context.destTupleDescriptor, analyzer);
            setDefaultValueExprs(scanProvider.getTargetTable(), context.srcSlotDescByName, context.params, true);
            this.contexts.add(context);
        }
    }

    private void initAndSetPrecedingFilter(Expr whereExpr, TupleDescriptor tupleDesc, Analyzer analyzer)
            throws UserException {
        Expr newWhereExpr = initWhereExpr(whereExpr, tupleDesc, analyzer);
        if (newWhereExpr != null) {
            addPreFilterConjuncts(newWhereExpr.getConjuncts());
        }
    }

    private void initAndSetWhereExpr(Expr whereExpr, TupleDescriptor tupleDesc, Analyzer analyzer)
            throws UserException {
        Expr newWhereExpr = initWhereExpr(whereExpr, tupleDesc, analyzer);
        if (newWhereExpr != null) {
            addConjuncts(newWhereExpr.getConjuncts());
        }
    }

    private Expr initWhereExpr(Expr whereExpr, TupleDescriptor tupleDesc, Analyzer analyzer) throws UserException {
        if (whereExpr == null) {
            return null;
        }

        Map<String, SlotDescriptor> dstDescMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (SlotDescriptor slotDescriptor : tupleDesc.getSlots()) {
            dstDescMap.put(slotDescriptor.getColumn().getName(), slotDescriptor);
        }

        // substitute SlotRef in filter expression
        // where expr must be equal first to transfer some predicates(eg: BetweenPredicate to BinaryPredicate)
        Expr newWhereExpr = analyzer.getExprRewriter()
                .rewrite(whereExpr, analyzer, ExprRewriter.ClauseType.WHERE_CLAUSE);
        List<SlotRef> slots = Lists.newArrayList();
        newWhereExpr.collect(SlotRef.class, slots);

        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        for (SlotRef slot : slots) {
            SlotDescriptor slotDesc = dstDescMap.get(slot.getColumnName());
            if (slotDesc == null) {
                throw new UserException(
                    "unknown column reference in where statement, reference=" + slot.getColumnName());
            }
            smap.getLhs().add(slot);
            smap.getRhs().add(new SlotRef(slotDesc));
        }
        newWhereExpr = newWhereExpr.clone(smap);
        newWhereExpr.analyze(analyzer);
        if (!newWhereExpr.getType().equals(org.apache.doris.catalog.Type.BOOLEAN)) {
            throw new UserException("where statement is not a valid statement return bool");
        }
        return newWhereExpr;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        Preconditions.checkState(contexts.size() == scanProviders.size(),
                contexts.size() + " vs. " + scanProviders.size());
        // ATTN: for load scan node, do not use backend policy in ExternalScanNode.
        // Because backend policy in ExternalScanNode may only contain compute backend.
        // But for load job, we should select backends from all backends, both compute and mix.
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .build();
        FederationBackendPolicy localBackendPolicy = new FederationBackendPolicy();
        localBackendPolicy.init(policy);
        for (int i = 0; i < contexts.size(); ++i) {
            FileLoadScanNode.ParamCreateContext context = contexts.get(i);
            LoadScanProvider scanProvider = scanProviders.get(i);
            finalizeParamsForLoad(context, analyzer);
            createScanRangeLocations(context, scanProvider, localBackendPolicy);
            this.inputSplitsNum += scanProvider.getInputSplitNum();
            this.totalFileSize += scanProvider.getInputFileSize();
        }
    }

    // TODO: This api is for load job only. Will remove it later.
    private void createScanRangeLocations(FileLoadScanNode.ParamCreateContext context,
            LoadScanProvider scanProvider, FederationBackendPolicy backendPolicy)
            throws UserException {
        scanProvider.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        // do nothing, we have already created scan range locations in finalize
    }

    protected void finalizeParamsForLoad(ParamCreateContext context,
            Analyzer analyzer) throws UserException {
        Map<String, SlotDescriptor> slotDescByName = context.srcSlotDescByName;
        Map<String, Expr> exprMap = context.exprMap;
        TupleDescriptor srcTupleDesc = context.srcTupleDescriptor;
        boolean negative = context.fileGroup.isNegative();

        TFileScanRangeParams params = context.params;
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        for (SlotDescriptor destSlotDesc : desc.getSlots()) {
            if (!destSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprMap != null) {
                expr = exprMap.get(destSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(destSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                    // If dest is allow null, we set source to nullable
                    if (destSlotDesc.getColumn().isAllowNull()) {
                        srcSlotDesc.setIsNullable(true);
                    }
                    expr = new SlotRef(srcSlotDesc);
                } else {
                    Column column = destSlotDesc.getColumn();
                    if (column.getDefaultValue() != null) {
                        if (column.getDefaultValueExprDef() != null) {
                            expr = column.getDefaultValueExpr();
                            expr.analyze(analyzer);
                        } else {
                            expr = new StringLiteral(destSlotDesc.getColumn().getDefaultValue());
                        }
                    } else {
                        if (column.isAllowNull()) {
                            expr = NullLiteral.create(column.getType());
                        } else if (column.isAutoInc()) {
                            // auto-increment column should be non-nullable
                            // however, here we use `NullLiteral` to indicate that a cell should
                            // be filled with generated value in `VOlapTableSink::_fill_auto_inc_cols()`
                            expr = NullLiteral.create(column.getType());
                        } else {
                            throw new AnalysisException("column has no source field, column=" + column.getName());
                        }
                    }
                }
            }

            // check hll_hash
            if (destSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                        + destSlotDesc.getColumn().getName() + "=" + FunctionSet.HLL_HASH + "(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.HLL_HASH) && !fn.getFnName()
                        .getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                        + destSlotDesc.getColumn().getName() + "=" + FunctionSet.HLL_HASH + "(xxx) or "
                        + destSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(org.apache.doris.catalog.Type.HLL);
            }

            checkBitmapCompatibility(analyzer, destSlotDesc, expr);
            checkQuantileStateCompatibility(analyzer, destSlotDesc, expr);

            if (negative && destSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr.analyze(analyzer);
            }

            // for jsonb type, use jsonb_parse_xxx to parse src string to jsonb.
            // and if input string is not a valid json string, return null.
            PrimitiveType dstType = destSlotDesc.getType().getPrimitiveType();
            PrimitiveType srcType = expr.getType().getPrimitiveType();
            if (dstType == PrimitiveType.JSONB
                    && (srcType == PrimitiveType.VARCHAR || srcType == PrimitiveType.STRING)) {
                List<Expr> args = Lists.newArrayList();
                args.add(expr);
                String nullable = "notnull";
                if (destSlotDesc.getIsNullable() || expr.isNullable()) {
                    nullable = "nullable";
                }
                String name = "jsonb_parse_" + nullable + "_error_to_null";
                expr = new FunctionCallExpr(name, args);
                expr.analyze(analyzer);
            } else {
                expr = castToSlot(destSlotDesc, expr);
            }
            params.putToExprOfDestSlot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        params.setDestSidToSrcSidWithoutTrans(destSidToSrcSidWithoutTrans);
        params.setDestTupleId(desc.getId().asInt());
        params.setSrcTupleId(srcTupleDesc.getId().asInt());

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeStatAndMemLayout();

        for (Expr conjunct : preFilterConjuncts) {
            params.addToPreFilterExprsList(conjunct.treeToThrift());
        }
    }

    protected void checkBitmapCompatibility(Analyzer analyzer, SlotDescriptor slotDesc, Expr expr)
            throws AnalysisException {
        if (slotDesc.getColumn().getAggregationType() == AggregateType.BITMAP_UNION) {
            expr.analyze(analyzer);
            if (!expr.getType().isBitmapType()) {
                String errorMsg = String.format("bitmap column %s require the function return type is BITMAP",
                        slotDesc.getColumn().getName());
                throw new AnalysisException(errorMsg);
            }
        }
    }

    protected void checkQuantileStateCompatibility(Analyzer analyzer, SlotDescriptor slotDesc, Expr expr)
            throws AnalysisException {
        if (slotDesc.getColumn().getAggregationType() == AggregateType.QUANTILE_UNION) {
            expr.analyze(analyzer);
            if (!expr.getType().isQuantileStateType()) {
                String errorMsg = "quantile_state column %s require the function return type is QUANTILE_STATE";
                throw new AnalysisException(errorMsg);
            }
        }
    }
}

