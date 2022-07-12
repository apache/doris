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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
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
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileTextScanRangeParams;
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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;
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

    // Just for explain
    private int inputSplitsNum = 0;
    private long totalFileSize = 0;

    private static class ParamCreateContext {
        public TFileScanRangeParams params;
        public TupleDescriptor srcTupleDescriptor;
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

        public int numBackends() {
            return backends.size();
        }
    }

    private static class FileSplitStrategy {
        private long totalSplitSize;
        private int splitNum;

        FileSplitStrategy() {
            this.totalSplitSize = 0;
            this.splitNum = 0;
        }

        public void update(FileSplit split) {
            totalSplitSize += split.getLength();
            splitNum++;
        }

        public boolean hasNext() {
            return totalSplitSize > Config.file_scan_node_split_size || splitNum > Config.file_scan_node_split_num;
        }

        public void next() {
            totalSplitSize = 0;
            splitNum = 0;
        }
    }

    private final BackendPolicy backendPolicy = new BackendPolicy();

    private final ParamCreateContext context = new ParamCreateContext();

    private final List<String> partitionKeys = new ArrayList<>();

    private List<TScanRangeLocations> scanRangeLocations;

    private final HMSExternalTable hmsTable;

    private ExternalFileScanProvider scanProvider;

    /**
     * External file scan node for hms table.
     */
    public ExternalFileScanNode(
            PlanNodeId id,
            TupleDescriptor desc,
            String planNodeName) {

        super(id, desc, planNodeName, StatisticalType.FILE_SCAN_NODE);

        this.hmsTable = (HMSExternalTable) this.desc.getTable();

        switch (this.hmsTable.getDlaType()) {
            case HUDI:
                this.scanProvider = new ExternalHudiScanProvider(this.hmsTable);
                break;
            case ICEBERG:
                this.scanProvider = new ExternalIcebergScanProvider(this.hmsTable);
                break;
            case HIVE:
                this.scanProvider = new ExternalHiveScanProvider(this.hmsTable);
                break;
            default:
                LOG.warn("Unknown table for dla.");
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        if (hmsTable.isView()) {
            throw new AnalysisException(String.format("Querying external view '[%s].%s.%s' is not supported",
                    hmsTable.getDlaType(), hmsTable.getDbName(), hmsTable.getName()));
        }
        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
        initContext();
    }

    private void initContext() throws DdlException, MetaNotFoundException {
        context.srcTupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        context.params = new TFileScanRangeParams();
        if (scanProvider.getTableFormatType().equals(TFileFormatType.FORMAT_CSV_PLAIN)) {
            Map<String, String> serDeInfoParams = hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters();
            String columnSeparator = Strings.isNullOrEmpty(serDeInfoParams.get("field.delim"))
                    ? HIVE_DEFAULT_COLUMN_SEPARATOR : serDeInfoParams.get("field.delim");
            String lineDelimiter = Strings.isNullOrEmpty(serDeInfoParams.get("line.delim"))
                    ? HIVE_DEFAULT_LINE_DELIMITER : serDeInfoParams.get("line.delim");

            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            textParams.setLineDelimiterStr(lineDelimiter);
            textParams.setColumnSeparatorStr(columnSeparator);

            context.params.setTextParams(textParams);
        }

        context.params.setSrcTupleId(context.srcTupleDescriptor.getId().asInt());
        // Need re compute memory layout after set some slot descriptor to nullable
        context.srcTupleDescriptor.computeStatAndMemLayout();

        Map<String, SlotDescriptor> slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        List<Column> columns = hmsTable.getBaseSchema(false);
        for (Column column : columns) {
            SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(context.srcTupleDescriptor);
            slotDesc.setType(column.getType());
            slotDesc.setIsMaterialized(true);
            slotDesc.setIsNullable(true);
            slotDesc.setColumn(new Column(column));
            slotDescByName.put(column.getName(), slotDesc);
        }

        // Hive table must extract partition value from path and hudi/iceberg table keep partition field in file.
        partitionKeys.addAll(scanProvider.getPathPartitionKeys());
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            int slotId = slotDescByName.get(slot.getColumn().getName()).getId().asInt();

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slotId);
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));

            context.params.addToRequiredSlots(slotInfo);
        }
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        try {
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
        inputSplitsNum = inputSplits.length;

        String fullPath = ((FileSplit) inputSplits[0]).getPath().toUri().toString();
        String filePath = ((FileSplit) inputSplits[0]).getPath().toUri().getPath();
        String fsName = fullPath.replace(filePath, "");

        TScanRangeLocations curLocations = newLocations(context.params);

        FileSplitStrategy fileSplitStrategy = new FileSplitStrategy();

        for (InputSplit split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;
            totalFileSize += split.getLength();

            List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                    partitionKeys);

            TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath);
            rangeDesc.getHdfsParams().setFsName(fsName);

            curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            Log.debug("Assign to backend " + curLocations.getLocations().get(0).getBackendId()
                    + " with table split: " +  fileSplit.getPath()
                    + " ( " + fileSplit.getStart() + "," + fileSplit.getLength() + ")");

            fileSplitStrategy.update(fileSplit);
            // Add a new location when it's can be split
            if (fileSplitStrategy.hasNext()) {
                scanRangeLocations.add(curLocations);
                curLocations = newLocations(context.params);
                fileSplitStrategy.next();
            }
        }
        if (curLocations.getScanRange().getExtScanRange().getFileScanRange().getRangesSize() > 0) {
            scanRangeLocations.add(curLocations);
        }
    }

    private TScanRangeLocations newLocations(TFileScanRangeParams params) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.setParams(params);

        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

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

    private TFileRangeDesc createFileRangeDesc(
            FileSplit fileSplit,
            List<String> columnsFromPath) throws DdlException, MetaNotFoundException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setFileType(scanProvider.getTableFileType());
        rangeDesc.setFormatType(scanProvider.getTableFormatType());
        rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        // set hdfs params for hdfs file type.
        if (scanProvider.getTableFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = BrokerUtil.generateHdfsParam(scanProvider.getTableProperties());
            rangeDesc.setHdfsParams(tHdfsParams);
        }
        return rangeDesc;
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
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table: ").append(hmsTable.getDbName()).append(".").append(hmsTable.getName())
                .append("\n").append(prefix).append("hms url: ").append(scanProvider.getMetaStoreUrl()).append("\n");

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

