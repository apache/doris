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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SchemaChangeExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.iceberg.IcebergApiSource;
import org.apache.doris.planner.external.iceberg.IcebergHMSSource;
import org.apache.doris.planner.external.iceberg.IcebergScanProvider;
import org.apache.doris.planner.external.iceberg.IcebergSource;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ExternalFileScanNode for the file access type of catalog, now only support
 * hive,hudi and iceberg.
 */
public class ExternalFileScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(ExternalFileScanNode.class);

    public static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public List<Expr> conjuncts;

        public TupleDescriptor destTupleDescriptor;
        public Map<String, SlotDescriptor> destSlotDescByName;
        // === Set when init ===
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, SlotDescriptor> srcSlotDescByName;
        public Map<String, Expr> exprMap;
        public String timezone;
        // === Set when init ===

        public TFileScanRangeParams params;

        public void createDestSlotMap() {
            Preconditions.checkNotNull(destTupleDescriptor);
            destSlotDescByName = Maps.newHashMap();
            for (SlotDescriptor slot : destTupleDescriptor.getSlots()) {
                destSlotDescByName.put(slot.getColumn().getName(), slot);
            }
        }
    }

    public enum Type {
        LOAD, QUERY
    }

    private Type type = Type.QUERY;
    private final FederationBackendPolicy backendPolicy = new FederationBackendPolicy();

    // Only for load job.
    // Save all info about load attributes and files.
    // Each DataDescription in a load stmt conreponding to a FileGroupInfo in this list.
    private List<FileGroupInfo> fileGroupInfos = Lists.newArrayList();
    // For query, there is only one FileScanProvider in this list.
    // For load, the num of providers equals to the num of file group infos.
    private List<FileScanProviderIf> scanProviders = Lists.newArrayList();
    // For query, there is only one ParamCreateContext in this list.
    // For load, the num of ParamCreateContext equals to the num of file group infos.
    private List<ParamCreateContext> contexts = Lists.newArrayList();

    // Final output of this file scan node
    private List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();

    // For explain
    private long inputSplitsNum = 0;
    private long totalFileSize = 0;
    private long totalPartitionNum = 0;
    private long readPartitionNum = 0;

    /**
     * External file scan node for:
     * 1. Query hms table
     * 2. Load from file
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf, load scan node.
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public ExternalFileScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "EXTERNAL_FILE_SCAN_NODE", StatisticalType.FILE_SCAN_NODE, needCheckColumnPriv);
    }

    // Only for broker load job.
    public void setLoadInfo(long loadJobId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded,
            boolean strictMode, int loadParallelism, UserIdentity userIdentity) {
        Preconditions.checkState(fileGroups.size() == fileStatusesList.size());
        for (int i = 0; i < fileGroups.size(); ++i) {
            FileGroupInfo fileGroupInfo = new FileGroupInfo(loadJobId, txnId, targetTable, brokerDesc,
                    fileGroups.get(i), fileStatusesList.get(i), filesAdded, strictMode, loadParallelism);
            fileGroupInfos.add(fileGroupInfo);
        }
        this.type = Type.LOAD;
    }

    // Only for stream load/routine load job.
    public void setLoadInfo(TUniqueId loadId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            BrokerFileGroup fileGroup, TBrokerFileStatus fileStatus, boolean strictMode, TFileType fileType,
            List<String> hiddenColumns) {
        FileGroupInfo fileGroupInfo = new FileGroupInfo(loadId, txnId, targetTable, brokerDesc,
                fileGroup, fileStatus, strictMode, fileType, hiddenColumns);
        fileGroupInfos.add(fileGroupInfo);
        this.type = Type.LOAD;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        if (!Config.enable_vectorized_load) {
            throw new UserException(
                    "Please set 'enable_vectorized_load=true' in fe.conf to enable external file scan node");
        }

        switch (type) {
            case QUERY:
                // prepare for partition prune
                computeColumnFilter();
                if (this.desc.getTable() instanceof HMSExternalTable) {
                    HMSExternalTable hmsTable = (HMSExternalTable) this.desc.getTable();
                    initHMSExternalTable(hmsTable);
                } else if (this.desc.getTable() instanceof FunctionGenTable) {
                    FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
                    initFunctionGenTable(table, (ExternalFileTableValuedFunction) table.getTvf());
                } else if (this.desc.getTable() instanceof IcebergExternalTable) {
                    IcebergExternalTable table = (IcebergExternalTable) this.desc.getTable();
                    initIcebergExternalTable(table);
                }
                break;
            case LOAD:
                for (FileGroupInfo fileGroupInfo : fileGroupInfos) {
                    this.scanProviders.add(new LoadScanProvider(fileGroupInfo, desc));
                }
                break;
            default:
                throw new UserException("Unknown type: " + type);
        }

        backendPolicy.init();
        numNodes = backendPolicy.numBackends();

        initParamCreateContexts(analyzer);
    }

    /**
     * Init ExternalFileScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    public void init() throws UserException {
        if (!Config.enable_vectorized_load) {
            throw new UserException(
                "Please set 'enable_vectorized_load=true' in fe.conf to enable external file scan node");
        }

        switch (type) {
            case QUERY:
                // prepare for partition prune
                // computeColumnFilter();
                if (this.desc.getTable() instanceof HMSExternalTable) {
                    HMSExternalTable hmsTable = (HMSExternalTable) this.desc.getTable();
                    initHMSExternalTable(hmsTable);
                } else if (this.desc.getTable() instanceof FunctionGenTable) {
                    FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
                    initFunctionGenTable(table, (ExternalFileTableValuedFunction) table.getTvf());
                } else if (this.desc.getTable() instanceof IcebergExternalTable) {
                    IcebergExternalTable table = (IcebergExternalTable) this.desc.getTable();
                    initIcebergExternalTable(table);
                }
                break;
            default:
                throw new UserException("Unknown type: " + type);
        }

        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
        for (FileScanProviderIf scanProvider : scanProviders) {
            ParamCreateContext context = scanProvider.createContext(analyzer);
            context.createDestSlotMap();
            initAndSetPrecedingFilter(context.fileGroup.getPrecedingFilterExpr(), context.srcTupleDescriptor, analyzer);
            initAndSetWhereExpr(context.fileGroup.getWhereExpr(), context.destTupleDescriptor, analyzer);
            context.conjuncts = conjuncts;
            this.contexts.add(context);
        }
    }

    /**
     * Reset required_slots in contexts. This is called after Nereids planner do the projection.
     * In the projection process, some slots may be removed. So call this to update the slots info.
     */
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        for (int i = 0; i < contexts.size(); i++) {
            ParamCreateContext context = contexts.get(i);
            FileScanProviderIf scanProvider = scanProviders.get(i);
            context.params.unsetRequiredSlots();
            for (SlotDescriptor slot : desc.getSlots()) {
                if (!slot.isMaterialized()) {
                    continue;
                }

                TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
                slotInfo.setSlotId(slot.getId().asInt());
                slotInfo.setIsFileSlot(!scanProvider.getPathPartitionKeys().contains(slot.getColumn().getName()));
                context.params.addToRequiredSlots(slotInfo);
            }
        }
    }

    private void initHMSExternalTable(HMSExternalTable hmsTable) throws UserException {
        Preconditions.checkNotNull(hmsTable);

        if (hmsTable.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '[%s].%s.%s' is not supported", hmsTable.getDlaType(),
                            hmsTable.getDbName(), hmsTable.getName()));
        }

        FileScanProviderIf scanProvider;
        switch (hmsTable.getDlaType()) {
            case HUDI:
                scanProvider = new HudiScanProvider(hmsTable, desc, columnNameToRange);
                break;
            case ICEBERG:
                IcebergSource hmsSource = new IcebergHMSSource(hmsTable, desc, columnNameToRange);
                scanProvider = new IcebergScanProvider(hmsSource, analyzer);
                break;
            case HIVE:
                String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
                if (inputFormat.contains("TextInputFormat")) {
                    for (SlotDescriptor slot : desc.getSlots()) {
                        if (!slot.getType().isScalarType()) {
                            throw new UserException("For column `" + slot.getColumn().getName()
                                    + "`, The column types ARRAY/MAP/STRUCT are not supported yet"
                                    + " for text input format of Hive. ");
                        }
                    }
                }
                scanProvider = new HiveScanProvider(hmsTable, desc, columnNameToRange);
                break;
            default:
                throw new UserException("Unknown table type: " + hmsTable.getDlaType());
        }
        this.scanProviders.add(scanProvider);
    }

    private void initIcebergExternalTable(IcebergExternalTable icebergTable) throws UserException {
        Preconditions.checkNotNull(icebergTable);
        if (icebergTable.isView()) {
            throw new AnalysisException(
                String.format("Querying external view '%s.%s' is not supported", icebergTable.getDbName(),
                        icebergTable.getName()));
        }

        FileScanProviderIf scanProvider;
        String catalogType = icebergTable.getIcebergCatalogType();
        switch (catalogType) {
            case IcebergExternalCatalog.ICEBERG_HMS:
            case IcebergExternalCatalog.ICEBERG_REST:
            case IcebergExternalCatalog.ICEBERG_DLF:
            case IcebergExternalCatalog.ICEBERG_GLUE:
                IcebergSource icebergSource = new IcebergApiSource(
                        icebergTable, desc, columnNameToRange);
                scanProvider = new IcebergScanProvider(icebergSource, analyzer);
                break;
            default:
                throw new UserException("Unknown iceberg catalog type: " + catalogType);
        }
        this.scanProviders.add(scanProvider);
    }

    private void initFunctionGenTable(FunctionGenTable table, ExternalFileTableValuedFunction tvf) {
        Preconditions.checkNotNull(table);
        FileScanProviderIf scanProvider = new TVFScanProvider(table, desc, tvf);
        this.scanProviders.add(scanProvider);
    }

    // For each scan provider, create a corresponding ParamCreateContext
    private void initParamCreateContexts(Analyzer analyzer) throws UserException {
        for (FileScanProviderIf scanProvider : scanProviders) {
            ParamCreateContext context = scanProvider.createContext(analyzer);
            context.createDestSlotMap();
            // set where and preceding filter.
            // FIXME(cmy): we should support set different expr for different file group.
            initAndSetPrecedingFilter(context.fileGroup.getPrecedingFilterExpr(), context.srcTupleDescriptor, analyzer);
            initAndSetWhereExpr(context.fileGroup.getWhereExpr(), context.destTupleDescriptor, analyzer);
            context.conjuncts = conjuncts;
            this.contexts.add(context);
        }
    }

    private void initAndSetPrecedingFilter(Expr whereExpr, TupleDescriptor tupleDesc, Analyzer analyzer)
            throws UserException {
        if (type != Type.LOAD) {
            return;
        }
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
        for (int i = 0; i < contexts.size(); ++i) {
            ParamCreateContext context = contexts.get(i);
            FileScanProviderIf scanProvider = scanProviders.get(i);
            setDefaultValueExprs(scanProvider, context);
            setColumnPositionMappingForTextFile(scanProvider, context);
            finalizeParamsForLoad(context, analyzer);
            createScanRangeLocations(context, scanProvider);
            this.inputSplitsNum += scanProvider.getInputSplitNum();
            this.totalFileSize += scanProvider.getInputFileSize();
            TableIf table = desc.getTable();
            if (table instanceof HMSExternalTable) {
                if (((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE)) {
                    genSlotToSchemaIdMap(context);
                }
            }
            if (scanProvider instanceof HiveScanProvider) {
                this.totalPartitionNum = ((HiveScanProvider) scanProvider).getTotalPartitionNum();
                this.readPartitionNum = ((HiveScanProvider) scanProvider).getReadPartitionNum();
            }
        }
    }

    @Override
    public void finalizeForNereids() throws UserException {
        Preconditions.checkState(contexts.size() == scanProviders.size(),
                contexts.size() + " vs. " + scanProviders.size());
        for (int i = 0; i < contexts.size(); ++i) {
            ParamCreateContext context = contexts.get(i);
            FileScanProviderIf scanProvider = scanProviders.get(i);
            setDefaultValueExprs(scanProvider, context);
            setColumnPositionMappingForTextFile(scanProvider, context);
            finalizeParamsForLoad(context, analyzer);
            createScanRangeLocations(context, scanProvider);
            this.inputSplitsNum += scanProvider.getInputSplitNum();
            this.totalFileSize += scanProvider.getInputFileSize();
            TableIf table = desc.getTable();
            if (table instanceof HMSExternalTable) {
                if (((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE)) {
                    genSlotToSchemaIdMap(context);
                }
            }
            if (scanProvider instanceof HiveScanProvider) {
                this.totalPartitionNum = ((HiveScanProvider) scanProvider).getTotalPartitionNum();
                this.readPartitionNum = ((HiveScanProvider) scanProvider).getReadPartitionNum();
            }
        }
    }

    private void setColumnPositionMappingForTextFile(FileScanProviderIf scanProvider, ParamCreateContext context)
            throws UserException {
        if (type != Type.QUERY) {
            return;
        }
        TableIf tbl = scanProvider.getTargetTable();
        List<Integer> columnIdxs = Lists.newArrayList();

        for (TFileScanSlotInfo slot : context.params.getRequiredSlots()) {
            if (!slot.isIsFileSlot()) {
                continue;
            }
            SlotDescriptor slotDesc = desc.getSlot(slot.getSlotId());
            String colName = slotDesc.getColumn().getName();
            int idx = tbl.getBaseColumnIdxByName(colName);
            if (idx == -1) {
                throw new UserException("Column " + colName + " not found in table " + tbl.getName());
            }
            columnIdxs.add(idx);
        }
        context.params.setColumnIdxs(columnIdxs);
    }

    protected void setDefaultValueExprs(FileScanProviderIf scanProvider, ParamCreateContext context)
            throws UserException {
        TableIf tbl = scanProvider.getTargetTable();
        Preconditions.checkNotNull(tbl);
        TExpr tExpr = new TExpr();
        tExpr.setNodes(Lists.newArrayList());

        for (Column column : tbl.getBaseSchema()) {
            Expr expr;
            if (column.getDefaultValue() != null) {
                if (column.getDefaultValueExprDef() != null) {
                    expr = column.getDefaultValueExpr();
                } else {
                    expr = new StringLiteral(column.getDefaultValue());
                }
            } else {
                if (column.isAllowNull()) {
                    if (type == Type.LOAD) {
                        // In load process, the source type is string.
                        expr = NullLiteral.create(org.apache.doris.catalog.Type.VARCHAR);
                    } else {
                        expr = NullLiteral.create(column.getType());
                    }
                } else {
                    expr = null;
                }
            }
            SlotDescriptor slotDesc = null;
            switch (type) {
                case LOAD: {
                    slotDesc = context.srcSlotDescByName.get(column.getName());
                    break;
                }
                case QUERY: {
                    slotDesc = context.destSlotDescByName.get(column.getName());
                    break;
                }
                default:
                    Preconditions.checkState(false, type);
            }
            // if slot desc is null, which mean it is an unrelated slot, just skip.
            // eg:
            // (a, b, c) set (x=a, y=b, z=c)
            // c does not exist in file, the z will be filled with null, even if z has default value.
            // and if z is not nullable, the load will fail.
            if (slotDesc != null) {
                if (expr != null) {
                    expr = castToSlot(slotDesc, expr);
                    context.params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), expr.treeToThrift());
                } else {
                    context.params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), tExpr);
                }
            }
        }
    }

    protected void finalizeParamsForLoad(ParamCreateContext context, Analyzer analyzer) throws UserException {
        if (type != Type.LOAD) {
            context.params.setSrcTupleId(-1);
            return;
        }
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
                        } else {
                            expr = new StringLiteral(destSlotDesc.getColumn().getDefaultValue());
                        }
                    } else {
                        if (column.isAllowNull()) {
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
            } else if (dstType == PrimitiveType.VARIANT) {
                // Generate SchemaChange expr for dynamicly generating columns
                TableIf targetTbl = desc.getTable();
                expr = new SchemaChangeExpr((SlotRef) expr, (int) targetTbl.getId());
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

        if (!preFilterConjuncts.isEmpty()) {
            Expr vPreFilterExpr = convertConjunctsToAndCompoundPredicate(preFilterConjuncts);
            initCompoundPredicate(vPreFilterExpr);
            params.setPreFilterExprs(vPreFilterExpr.treeToThrift());
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

    private void createScanRangeLocations(ParamCreateContext context, FileScanProviderIf scanProvider)
            throws UserException {
        scanProvider.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
    }

    private void genSlotToSchemaIdMap(ParamCreateContext context) {
        List<Column> baseSchema = desc.getTable().getBaseSchema();
        Map<String, Integer> columnNameToPosition = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            int idx = 0;
            for (Column col : baseSchema) {
                if (col.getName().equals(slot.getColumn().getName())) {
                    columnNameToPosition.put(col.getName(), idx);
                    break;
                }
                idx += 1;
            }
        }
        context.params.setSlotNameToSchemaPos(columnNameToPosition);
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.FILE_SCAN_NODE);
        TFileScanNode fileScanNode = new TFileScanNode();
        fileScanNode.setTupleId(desc.getId().asInt());
        planNode.setFileScanNode(fileScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        LOG.debug("There is {} scanRangeLocations for execution.", scanRangeLocations.size());
        return scanRangeLocations;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table: ").append(desc.getTable().getName()).append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(false));
        }

        output.append(prefix).append("inputSplitNum=").append(inputSplitsNum).append(", totalFileSize=")
                .append(totalFileSize).append(", scanRanges=").append(scanRangeLocations.size()).append("\n");
        output.append(prefix).append("partition=").append(readPartitionNum).append("/").append(totalPartitionNum)
                .append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(prefix).append("backends:").append("\n");
            Multimap<Long, TFileRangeDesc> scanRangeLocationsMap = ArrayListMultimap.create();
            // 1. group by backend id
            for (TScanRangeLocations locations : scanRangeLocations) {
                scanRangeLocationsMap.putAll(locations.getLocations().get(0).backend_id,
                        locations.getScanRange().getExtScanRange().getFileScanRange().getRanges());
            }
            for (long beId : scanRangeLocationsMap.keySet()) {
                output.append(prefix).append("  ").append(beId).append("\n");
                List<TFileRangeDesc> fileRangeDescs = Lists.newArrayList(scanRangeLocationsMap.get(beId));
                // 2. sort by file start offset
                Collections.sort(fileRangeDescs, new Comparator<TFileRangeDesc>() {
                    @Override
                    public int compare(TFileRangeDesc o1, TFileRangeDesc o2) {
                        return Long.compare(o1.getStartOffset(), o2.getStartOffset());
                    }
                });
                // 3. if size <= 4, print all. if size > 4, print first 3 and last 1
                int size = fileRangeDescs.size();
                if (size <= 4) {
                    for (TFileRangeDesc file : fileRangeDescs) {
                        output.append(prefix).append("    ").append(file.getPath())
                                .append(" start: ").append(file.getStartOffset())
                                .append(" length: ").append(file.getSize())
                                .append("\n");
                    }
                } else {
                    for (int i = 0; i < 3; i++) {
                        TFileRangeDesc file = fileRangeDescs.get(i);
                        output.append(prefix).append("    ").append(file.getPath())
                                .append(" start: ").append(file.getStartOffset())
                                .append(" length: ").append(file.getSize())
                                .append("\n");
                    }
                    int other = size - 4;
                    output.append(prefix).append("    ... other ").append(other).append(" files ...\n");
                    TFileRangeDesc file = fileRangeDescs.get(size - 1);
                    output.append(prefix).append("    ").append(file.getPath())
                            .append(" start: ").append(file.getStartOffset())
                            .append(" length: ").append(file.getSize())
                            .append("\n");
                }
            }
        }

        output.append(prefix);
        if (cardinality > 0) {
            output.append(String.format("cardinality=%s, ", cardinality));
        }
        if (avgRowSize > 0) {
            output.append(String.format("avgRowSize=%s, ", avgRowSize));
        }
        output.append(String.format("numNodes=%s", numNodes)).append("\n");

        return output.toString();
    }
}

