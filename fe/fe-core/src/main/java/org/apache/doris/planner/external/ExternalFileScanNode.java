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
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * ExternalFileScanNode for the file access type of catalog, now only support
 * hive,hudi and iceberg.
 */
public class ExternalFileScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(ExternalFileScanNode.class);

    // Just for explain
    private int inputSplitsNum = 0;
    private long totalFileSize = 0;

    public static class ParamCreateContext {
        public FileScanProviderIf scanProvider;
        public TupleDescriptor srcTupleDescriptor;
        public TupleDescriptor destTupleDescriptor;
        public TFileScanRangeParams params;
        public BrokerFileGroup fileGroup;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
        public String timezone;
        public Expr deleteCondition;
        public LoadTask.MergeType mergeType;
        public List<Expr> conjuncts;
    }

    public enum Type {
        LOAD, QUERY
    }

    private Type type = Type.QUERY;
    private final BackendPolicy backendPolicy = new BackendPolicy();

    private List<FileScanProviderIf> scanProviders = Lists.newArrayList();
    private List<ParamCreateContext> contexts = Lists.newArrayList();

    private final List<String> partitionKeys = Lists.newArrayList();

    private List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();

    private List<FileGroupInfo> fileGroupInfos = Lists.newArrayList();

    /**
     * External file scan node for hms table.
     */
    public ExternalFileScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName, StatisticalType.FILE_SCAN_NODE);
    }

    public void setLoadInfo(long loadJobId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded,
            boolean strictMode, int loadParallelism, UserIdentity userIdentity) {
        Preconditions.checkState(fileGroups.size() == fileStatusesList.size());
        for (int i = 0; i < fileGroups.size(); ++i) {
            FileGroupInfo fileGroupInfo = new FileGroupInfo(loadJobId, txnId, targetTable, brokerDesc,
                    fileGroups.get(i), fileStatusesList.get(i), filesAdded, strictMode, loadParallelism, userIdentity);
            fileGroupInfos.add(fileGroupInfo);
        }
        this.type = Type.LOAD;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        switch (type) {
            case QUERY:
                HMSExternalTable hmsTable = (HMSExternalTable) this.desc.getTable();
                Preconditions.checkNotNull(hmsTable);

                if (hmsTable.isView()) {
                    throw new AnalysisException(
                            String.format("Querying external view '[%s].%s.%s' is not supported", hmsTable.getDlaType(),
                                    hmsTable.getDbName(), hmsTable.getName()));
                }

                FileScanProviderIf scanProvider;
                switch (hmsTable.getDlaType()) {
                    case HUDI:
                        scanProvider = new HudiScanProvider(hmsTable);
                        break;
                    case ICEBERG:
                        scanProvider = new IcebergScanProvider(hmsTable);
                        break;
                    case HIVE:
                        scanProvider = new HiveScanProvider(hmsTable);
                        break;
                    default:
                        throw new UserException("Unknown table type: " + hmsTable.getDlaType());
                }
                this.scanProviders.add(scanProvider);
                break;
            case LOAD:
                for (FileGroupInfo fileGroupInfo : fileGroupInfos) {
                    this.scanProviders.add(new LoadScanProvider(fileGroupInfo));
                }
                break;
            default:
                throw new UserException("Unknown type: " + type);
        }

        backendPolicy.init();
        numNodes = backendPolicy.numBackends();

        initFileGroupContexts(analyzer);
    }

    private void initFileGroupContexts(Analyzer analyzer) throws UserException {
        for (FileScanProviderIf scanProvider : scanProviders) {
            ParamCreateContext context = scanProvider.createContext(analyzer);
            // set where and preceding filter.
            // FIXME(cmy): we should support set different expr for different file group.
            initAndSetPrecedingFilter(context.fileGroup.getPrecedingFilterExpr(), context.srcTupleDescriptor, analyzer);
            initAndSetWhereExpr(context.fileGroup.getWhereExpr(), context.destTupleDescriptor, analyzer);
            context.destTupleDescriptor = desc;
            context.conjuncts = conjuncts;
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
        for (ParamCreateContext context : contexts) {
            if (type == Type.LOAD) {
                finalizeParamsForLoad(context, analyzer);
            }
            processFileGroup(context);
        }
    }

    protected void finalizeParamsForLoad(ParamCreateContext context, Analyzer analyzer) throws UserException {
        Map<String, SlotDescriptor> slotDescByName = context.slotDescByName;
        Map<String, Expr> exprMap = context.exprMap;
        TFileScanRangeParams params = context.params;
        TupleDescriptor srcTupleDesc = context.srcTupleDescriptor;
        boolean strictMode = context.params.strict_mode;
        boolean negative = context.fileGroup.isNegative();

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

            // check quantile_state

            if (negative && destSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr.analyze(analyzer);
            }
            expr = castToSlot(destSlotDesc, expr);
            params.putToExprOfDestSlot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        params.setDestSidToSrcSidWithoutTrans(destSidToSrcSidWithoutTrans);
        params.setDestTupleId(desc.getId().asInt());
        params.setStrictMode(strictMode);
        params.setSrcTupleId(srcTupleDesc.getId().asInt());

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeStatAndMemLayout();
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

    private void processFileGroup(ParamCreateContext context) throws UserException {
        context.scanProvider.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
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
        if (!preFilterConjuncts.isEmpty()) {
            if (Config.enable_vectorized_load && vpreFilterConjunct != null) {
                fileScanNode.addToPreFilterExprs(vpreFilterConjunct.treeToThrift());
            } else {
                for (Expr e : preFilterConjuncts) {
                    fileScanNode.addToPreFilterExprs(e.treeToThrift());
                }
            }
        }
        planNode.setFileScanNode(fileScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        LOG.debug("There is {} scanRangeLocations for execution.", scanRangeLocations.size());
        return scanRangeLocations;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder(prefix);
        // output.append(fileTable.getExplainString(prefix));

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(false));
        }

        output.append(prefix).append("inputSplitNum=").append(inputSplitsNum).append(", totalFileSize=")
                .append(totalFileSize).append(", scanRanges=").append(scanRangeLocations.size()).append("\n");

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
