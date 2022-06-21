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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanNode;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * ExternalFileScanNode for the file access type of datasource, now only support hive,hudi and iceberg.
 */
public class ExternalFileScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(ExternalFileScanNode.class);

    private static final String HIVE_DEFAULT_COLUMN_SEPARATOR = "\001";

    private static final String HIVE_DEFAULT_LINE_DELIMITER = "\n";

    private static class ParamCreateContext {
        public TBrokerScanRangeParams params;
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, SlotDescriptor> slotDescByName;
    }

    private static class BackendPolicy {
        private final List<Backend> backends = Lists.newArrayList();

        private int nextBe = 0;

        public void init() throws UserException {
            Set<Tag> tags = Sets.newHashSet();
            if (ConnectContext.get().getCurrentUserIdentity() != null) {
                String qualifiedUser = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
                tags = Catalog.getCurrentCatalog().getAuth().getResourceTags(qualifiedUser);
                if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                    throw new UserException("No valid resource tag for user: " + qualifiedUser);
                }
            } else {
                LOG.debug("user info in ExternalFileScanNode should not be null, add log to observer");
            }

            // scan node is used for query
            BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                    .needQueryAvailable()
                    .needLoadAvailable()
                    .addTags(tags)
                    .build();
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

        public Backend getNextBe() {
            Backend selectedBackend = backends.get(nextBe++);
            nextBe = nextBe % backends.size();
            return selectedBackend;
        }
    }

    private enum DLAType {
        HIVE,
        HUDI,
        ICE_BERG
    }

    private final BackendPolicy backendPolicy = new BackendPolicy();

    private final ParamCreateContext context = new ParamCreateContext();

    private List<TScanRangeLocations> scanRangeLocations;

    private final HMSExternalTable hmsTable;

    private ExternalFileScanProvider scanProvider;

    /**
     * External file scan node for hms table.
     */
    public ExternalFileScanNode(
            PlanNodeId id,
            TupleDescriptor desc,
            String planNodeName) throws MetaNotFoundException {
        super(id, desc, planNodeName, StatisticalType.BROKER_SCAN_NODE);

        this.hmsTable = (HMSExternalTable) desc.getTable();

        DLAType type = getDLAType();
        switch (type) {
            case HUDI:
                this.scanProvider = new ExternalHudiScanProvider(this.hmsTable);
                break;
            case ICE_BERG:
                this.scanProvider = new ExternalIcebergScanProvider(this.hmsTable);
                break;
            case HIVE:
                this.scanProvider = new ExternalHiveScanProvider(this.hmsTable);
                break;
            default:
                LOG.warn("Unknown table for dla.");
        }
    }

    private DLAType getDLAType() throws MetaNotFoundException {
        if (hmsTable.getRemoteTable().getParameters().containsKey("table_type")
                && hmsTable.getRemoteTable().getParameters().get("table_type").equalsIgnoreCase("ICEBERG")) {
            return DLAType.ICE_BERG;
        } else if (hmsTable.getRemoteTable().getSd().getInputFormat().toLowerCase().contains("hoodie")) {
            return DLAType.HUDI;
        } else {
            return DLAType.HIVE;
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        backendPolicy.init();
        initContext(context);
    }

    private void initContext(ParamCreateContext context) throws DdlException, MetaNotFoundException {
        context.srcTupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        context.slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.params = new TBrokerScanRangeParams();
        if (scanProvider.getTableFormatType().equals(TFileFormatType.FORMAT_CSV_PLAIN)) {
            Map<String, String> serDeInfoParams = hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters();
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

        List<Column> columns = hmsTable.getBaseSchema(false);
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
            finalizeParams(context.slotDescByName, context.params, context.srcTupleDescriptor);
            buildScanRange();
        } catch (IOException e) {
            LOG.error("Finalize failed.", e);
            throw new UserException("Finalize failed.", e);
        }
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void buildScanRange() throws UserException, IOException {
        scanRangeLocations = Lists.newArrayList();
        InputSplit[] inputSplits = scanProvider.getSplits(conjuncts);
        if (0 == inputSplits.length) {
            return;
        }

        THdfsParams hdfsParams = new THdfsParams();
        String fullPath = ((FileSplit) inputSplits[0]).getPath().toUri().toString();
        String filePath = ((FileSplit) inputSplits[0]).getPath().toUri().getPath();
        String fsName = fullPath.replace(filePath, "");
        hdfsParams.setFsName(fsName);
        List<String> partitionKeys = new ArrayList<>();
        for (FieldSchema fieldSchema : hmsTable.getRemoteTable().getPartitionKeys()) {
            partitionKeys.add(fieldSchema.getName());
        }

        for (InputSplit split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;

            TScanRangeLocations curLocations = newLocations(context.params);
            List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                    partitionKeys);
            int numberOfColumnsFromFile = context.slotDescByName.size() - partitionValuesFromPath.size();

            TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(fileSplit, partitionValuesFromPath,
                    numberOfColumnsFromFile);
            rangeDesc.setHdfsParams(hdfsParams);
            rangeDesc.setReadByColumnDef(true);

            curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
            Log.debug("Assign to backend " + curLocations.getLocations().get(0).getBackendId()
                    + " with table split: " +  fileSplit.getPath()
                    + " ( " + fileSplit.getStart() + "," + fileSplit.getLength() + ")");

            // Put the last file
            if (curLocations.getScanRange().getBrokerScanRange().isSetRanges()) {
                scanRangeLocations.add(curLocations);
            }
        }
    }

    private TScanRangeLocations newLocations(TBrokerScanRangeParams params) {
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
        Backend selectedBackend = backendPolicy.getNextBe();
        location.setBackendId(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
    }

    private TBrokerRangeDesc createBrokerRangeDesc(
            FileSplit fileSplit,
            List<String> columnsFromPath,
            int numberOfColumnsFromFile) throws DdlException, MetaNotFoundException {
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

    private void finalizeParams(
            Map<String, SlotDescriptor> slotDescByName,
            TBrokerScanRangeParams params,
            TupleDescriptor srcTupleDesc) throws UserException {
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        for (SlotDescriptor destSlotDesc : desc.getSlots()) {
            Expr expr;
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

            expr = castToSlot(destSlotDesc, expr);
            params.putToExprOfDestSlot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        params.setDestSidToSrcSidWithoutTrans(destSidToSrcSidWithoutTrans);
        params.setDestTupleId(desc.getId().asInt());
        params.setStrictMode(false);
        params.setSrcTupleId(srcTupleDesc.getId().asInt());

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeStatAndMemLayout();
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
        String url;
        try {
            url = scanProvider.getMetaStoreUrl();
        } catch (MetaNotFoundException e) {
            LOG.warn("Can't get url error", e);
            url = "Can't get url error.";
        }
        return prefix + "DATABASE: " + hmsTable.getDbName() + "\n"
                + prefix + "TABLE: " + hmsTable.getName() + "\n"
                + prefix + "HIVE URL: " + url + "\n";
    }
}
