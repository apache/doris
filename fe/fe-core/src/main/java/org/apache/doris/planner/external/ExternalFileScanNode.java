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

import com.google.common.base.Strings;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.*;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.mysql.privilege.UserProperty;

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.doris.planner.HiveScanNode.HIVE_DEFAULT_COLUMN_SEPARATOR;
import static org.apache.doris.planner.HiveScanNode.HIVE_DEFAULT_LINE_DELIMITER;

public class ExternalFileScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(ExternalFileScanNode.class);

    private static class ParamCreateContext {
        public TBrokerScanRangeParams params;
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
    }

    private final org.apache.doris.catalog.Table catalogTable;

    private List<TScanRangeLocations> scanRangeLocations;

    private UserIdentity userIdentity;

    private List<Backend> backends;
    private int nextBe = 0;

    private Table remoteHiveTable;
    /* hudi table properties */
    private final ParamCreateContext context = new ParamCreateContext();

    private ExternalFileScanProvider scanProvider;

    public ExternalFileScanNode(
            PlanNodeId id,
            TupleDescriptor desc,
            String planNodeName) {
        super(id, desc, planNodeName, NodeType.BROKER_SCAN_NODE);
        if (ConnectContext.get() != null) {
            this.userIdentity = ConnectContext.get().getCurrentUserIdentity();
        }
        this.catalogTable = desc.getTable();

        DLAType type = getDLAType();
        switch (type) {
            case HUDI:
                this.scanProvider = new ExternalHudiScanProvider(this.catalogTable);
                break;
            case ICE_BERG:
                this.scanProvider = new ExternalIcebergScanProvider(this.catalogTable);
                break;
            case HIVE:
                this.scanProvider = new ExternalHiveScanProvider(this.catalogTable);
                break;
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        this.remoteHiveTable = scanProvider.getRemoteHiveTable();
        assignBackends();

        initContext(context);
    }

    private void assignBackends() throws UserException {
        Set<Tag> tags = Sets.newHashSet();
        if (userIdentity != null) {
            tags = Catalog.getCurrentCatalog().getAuth().getResourceTags(userIdentity.getQualifiedUser());
            if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                throw new UserException("No valid resource tag for user: " + userIdentity.getQualifiedUser());
            }
        } else {
            LOG.debug("user info in BrokerScanNode should not be null, add log to observer");
        }
        backends = Lists.newArrayList();
        // broker scan node is used for query or load
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needQueryAvailable().needLoadAvailable()
                .addTags(tags).build();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (policy.isMatch(be)) {
                backends.add(be);
            }
        }
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
        Random random = new Random(System.currentTimeMillis());
        Collections.shuffle(backends, random);
    }

    private void initContext(ParamCreateContext context) throws DdlException {
        context.srcTupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        context.slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.params = new TBrokerScanRangeParams();
        if (scanProvider.getTableFormatType().equals(TFileFormatType.FORMAT_CSV_PLAIN)) {
            Map<String, String> serDeInfoParams = remoteHiveTable.getSd().getSerdeInfo().getParameters();
            String columnSeparator = Strings.isNullOrEmpty(serDeInfoParams.get("field.delim"))
                ? HIVE_DEFAULT_COLUMN_SEPARATOR : serDeInfoParams.get("field.delim");
            String lineDelimiter = Strings.isNullOrEmpty(serDeInfoParams.get("line.delim"))
                ? HIVE_DEFAULT_LINE_DELIMITER : serDeInfoParams.get("line.delim");
            context.params.setColumnSeparator(columnSeparator.getBytes(StandardCharsets.UTF_8)[0]);
            context.params.setLineDelimiter(lineDelimiter.getBytes(StandardCharsets.UTF_8)[0]);
            context.params.setColumnSeparatorStr(columnSeparator);
            context.params.setLineDelimiterStr(lineDelimiter);
            context.params.setColumnSeparatorLength(columnSeparator.getBytes(StandardCharsets.UTF_8).length);
            context.params.setLineDelimiterLength(lineDelimiter.getBytes(StandardCharsets.UTF_8).length);
        }

        Map<String, SlotDescriptor> slotDescByName = Maps.newHashMap();

        List<Column> columns = catalogTable.getBaseSchema(false);
        // init slot desc add expr map, also transform hadoop functions
        for (Column column : columns) {
            SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(context.srcTupleDescriptor);
            slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
            slotDesc.setIsMaterialized(true);
            slotDesc.setIsNullable(true);
            slotDesc.setColumn(new Column(column.getName(), PrimitiveType.VARCHAR));
            context.params.addToSrcSlotIds(slotDesc.getId().asInt());
            slotDescByName.put(column.getName(), slotDesc);
        }
        context.slotDescByName = slotDescByName;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        try {
            finalizeParams(context.slotDescByName, context.exprMap, context.params,
                    context.srcTupleDescriptor, false, false, analyzer);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        try {
            buildScanRange();
        } catch (IOException e) {
            LOG.error("Build scan range failed.", e);
            throw new UserException("Build scan range failed.", e);
        }
    }

    public enum DLAType {
        HIVE,
        HUDI,
        ICE_BERG
    }

    private DLAType getDLAType() {
        if (remoteHiveTable.getParameters().containsKey("table_type") &&
            remoteHiveTable.getParameters().get("table_type").equalsIgnoreCase("ICEBERG")){
            return DLAType.ICE_BERG;
        } else if (remoteHiveTable.getSd().getInputFormat().toLowerCase().contains("hoodie")) {
            return DLAType.HUDI;
        } else {
            return DLAType.HIVE;
        }
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void buildScanRange() throws UserException, IOException {
        scanRangeLocations = Lists.newArrayList();
        InputSplit[] inputSplits = scanProvider.getSplits(conjuncts);
        if (inputSplits.length == 0) {
            return;
        }

        THdfsParams hdfsParams = new THdfsParams();
        String fullPath = ((FileSplit) inputSplits[0]).getPath().toUri().toString();
        String filePath = ((FileSplit) inputSplits[0]).getPath().toUri().getPath();
        String fsName = fullPath.replace(filePath, "");
        hdfsParams.setFsName(fsName);
        Log.debug("Hudi path's host is " + fsName);
        List<String> partitionKeys = new ArrayList<>();
        for (FieldSchema fieldSchema : remoteHiveTable.getPartitionKeys()) {
            partitionKeys.add(fieldSchema.getName());
        }

        for (InputSplit split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;

            TScanRangeLocations curLocations = newLocations(context.params);
            List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(), partitionKeys);
            int numberOfColumnsFromFile = context.slotDescByName.size() - partitionValuesFromPath.size();

            TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(fileSplit, partitionValuesFromPath, numberOfColumnsFromFile);
            rangeDesc.setHdfsParams(hdfsParams);
            rangeDesc.setReadByColumnDef(true);

            curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
            Log.debug("Assign to backend " + curLocations.getLocations().get(0).getBackendId()
                    + " with hudi split: " +  fileSplit.getPath()
                    + " ( " + fileSplit.getStart() + "," + fileSplit.getLength() + ")");

            // Put the last file
            if (curLocations.getScanRange().getBrokerScanRange().isSetRanges()) {
                scanRangeLocations.add(curLocations);
            }
        }
    }

    protected TScanRangeLocations newLocations(TBrokerScanRangeParams params) {

        Backend selectedBackend = backends.get(nextBe++);
        nextBe = nextBe % backends.size();


        // Generate on broker scan range
        TBrokerScanRange brokerScanRange = new TBrokerScanRange();
        brokerScanRange.setParams(params);
        brokerScanRange.setBrokerAddresses(new ArrayList<>());

        // Scan range
        TScanRange scanRange = new TScanRange();
        scanRange.setBrokerScanRange(brokerScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);

        TScanRangeLocation location = new TScanRangeLocation();
        location.setBackendId(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
    }

    private TBrokerRangeDesc createBrokerRangeDesc(FileSplit fileSplit, List<String> columnsFromPath, int numberOfColumnsFromFile) throws DdlException {
        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFileType(scanProvider.getTableFileType());
        rangeDesc.setFormatType(scanProvider.getTableFormatType());
        rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        rangeDesc.setSplittable(true);
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        rangeDesc.setNumOfColumnsFromFile(numberOfColumnsFromFile);
        rangeDesc.setColumnsFromPath(columnsFromPath);
        // set hdfs params for hdfs file type.
        if (scanProvider.getTableFileType() == TFileType.FILE_HDFS) {
            BrokerUtil.generateHdfsParam(scanProvider.getTableProperties(), rangeDesc);
        }
        return rangeDesc;
    }

    protected void finalizeParams(Map<String, SlotDescriptor> slotDescByName,
            Map<String, Expr> exprMap,
            TBrokerScanRangeParams params,
            TupleDescriptor srcTupleDesc,
            boolean strictMode,
            boolean negative,
            Analyzer analyzer) throws UserException {
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
                        expr = new StringLiteral(destSlotDesc.getColumn().getDefaultValue());
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
                if (!fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.HLL_HASH)
                        && !fn.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                            + destSlotDesc.getColumn().getName() + "=" + FunctionSet.HLL_HASH
                            + "(xxx) or " + destSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(Type.HLL);
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

    protected void checkBitmapCompatibility(Analyzer analyzer, SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
        if (slotDesc.getColumn().getAggregationType() == AggregateType.BITMAP_UNION) {
            expr.analyze(analyzer);
            if (!expr.getType().isBitmapType()) {
                String errorMsg = String.format("bitmap column %s require the function return type is BITMAP",
                        slotDesc.getColumn().getName());
                throw new AnalysisException(errorMsg);
            }
        }
    }

    protected void checkQuantileStateCompatibility(Analyzer analyzer, SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
        if (slotDesc.getColumn().getAggregationType() == AggregateType.QUANTILE_UNION) {
            expr.analyze(analyzer);
            if (!expr.getType().isQuantileStateType()) {
                String errorMsg = String.format("quantile_state column %s require the function return type is QUANTILE_STATE");
                throw new AnalysisException(errorMsg);
            }
        }
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.BROKER_SCAN_NODE);
        TBrokerScanNode brokerScanNode = new TBrokerScanNode(desc.getId().asInt());
        if (!preFilterConjuncts.isEmpty()) {
            if (Config.enable_vectorized_load && vpreFilterConjunct != null) {
                brokerScanNode.addToPreFilterExprs(vpreFilterConjunct.treeToThrift());
            } else {
                for (Expr e : preFilterConjuncts) {
                    brokerScanNode.addToPreFilterExprs(e.treeToThrift());
                }
            }
        }
        planNode.setBrokerScanNode(brokerScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "TABLE: " + catalogTable.getName() + "\n" + prefix + "TYPE: " + catalogTable.getType() + "\n";
    }
}
