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
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

// Broker scan node
public class BrokerScanNode extends LoadScanNode {
    private static final Logger LOG = LogManager.getLogger(BrokerScanNode.class);
    private static final TBrokerFileStatusComparator T_BROKER_FILE_STATUS_COMPARATOR
            = new TBrokerFileStatusComparator();

    public static class TBrokerFileStatusComparator implements Comparator<TBrokerFileStatus> {
        @Override
        public int compare(TBrokerFileStatus o1, TBrokerFileStatus o2) {
            if (o1.size < o2.size) {
                return -1;
            } else if (o1.size > o2.size) {
                return 1;
            }
            return 0;
        }
    }

    private final Random random = new Random(System.currentTimeMillis());

    // File groups need to
    private List<TScanRangeLocations> locationsList;

    // used both for load statement and select statement
    private long totalBytes;
    private long bytesPerInstance;

    // Parameters need to process
    private long loadJobId = -1; // -1 means this scan node is not for a load job
    private long txnId = -1;
    private Table targetTable;
    private BrokerDesc brokerDesc;
    private List<BrokerFileGroup> fileGroups;
    private boolean strictMode = false;
    private int loadParallelism = 1;

    private List<List<TBrokerFileStatus>> fileStatusesList;
    // file num
    private int filesAdded;

    // Only used for external table in select statement
    private List<Backend> backends;
    private int nextBe = 0;

    private Analyzer analyzer;

    private static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TBrokerScanRangeParams params;
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
        public String timezone;
    }

    private List<ParamCreateContext> paramCreateContexts;

    public BrokerScanNode(PlanNodeId id, TupleDescriptor destTupleDesc, String planNodeName,
                          List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, destTupleDesc, planNodeName);
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        this.analyzer = analyzer;
        if (desc.getTable() != null) {
            BrokerTable brokerTable = (BrokerTable) desc.getTable();
            try {
                fileGroups = Lists.newArrayList(new BrokerFileGroup(brokerTable));
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            brokerDesc = new BrokerDesc(brokerTable.getBrokerName(), brokerTable.getBrokerProperties());
            targetTable = brokerTable;
        }

        // Get all broker file status
        assignBackends();
        getFileStatusAndCalcInstance();

        paramCreateContexts = Lists.newArrayList();
        for (BrokerFileGroup fileGroup : fileGroups) {
            ParamCreateContext context = new ParamCreateContext();
            context.fileGroup = fileGroup;
            context.timezone = analyzer.getTimezone();
            initParams(context);
            paramCreateContexts.add(context);
        }
    }

    private boolean isLoad() {
        return desc.getTable() == null;
    }

    @Deprecated
    public void setLoadInfo(Table targetTable,
                            BrokerDesc brokerDesc,
                            List<BrokerFileGroup> fileGroups) {
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
    }

    public void setLoadInfo(long loadJobId,
                            long txnId,
                            Table targetTable,
                            BrokerDesc brokerDesc,
                            List<BrokerFileGroup> fileGroups,
                            boolean strictMode,
                            int loadParallelism) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.strictMode = strictMode;
        this.loadParallelism = loadParallelism;
    }

    // Called from init, construct source tuple information
    private void initParams(ParamCreateContext context)
            throws UserException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        context.params = params;

        BrokerFileGroup fileGroup = context.fileGroup;
        params.setColumnSeparator(fileGroup.getValueSeparator().getBytes(Charset.forName("UTF-8"))[0]);
        params.setLineDelimiter(fileGroup.getLineDelimiter().getBytes(Charset.forName("UTF-8"))[0]);
        params.setStrictMode(strictMode);
        params.setProperties(brokerDesc.getProperties());
        deleteCondition = fileGroup.getDeleteCondition();
        mergeType = fileGroup.getMergeType();
        initColumns(context);
        initAndSetPrecedingFilter(fileGroup.getPrecedingFilterExpr(), context.srcTupleDescriptor, analyzer);
        initAndSetWhereExpr(fileGroup.getWhereExpr(), this.desc, analyzer);
    }

    /**
     * This method is used to calculate the slotDescByName and exprMap.
     * The expr in exprMap is analyzed in this function.
     * The smap of slot which belongs to expr will be analyzed by src desc.
     * slotDescByName: the single slot from columns in load stmt
     * exprMap: the expr from column mapping in load stmt.
     *
     * @param context
     * @throws UserException
     */
    private void initColumns(ParamCreateContext context) throws UserException {
        context.srcTupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        context.slotDescByName = Maps.newHashMap();
        context.exprMap = Maps.newHashMap();

        // for load job, column exprs is got from file group
        // for query, there is no column exprs, they will be got from table's schema in "Load.initColumns"
        List<ImportColumnDesc> columnExprs = Lists.newArrayList();
        if (isLoad()) {
            columnExprs = context.fileGroup.getColumnExprList();
            if (mergeType == LoadTask.MergeType.MERGE) {
                columnExprs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(deleteCondition));
            } else if (mergeType == LoadTask.MergeType.DELETE) {
                columnExprs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
            }
            // add columnExpr for sequence column
            if (context.fileGroup.hasSequenceCol()) {
                columnExprs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                        new SlotRef(null, context.fileGroup.getSequenceCol())));
            }
        }

        Load.initColumns(targetTable, columnExprs,
                context.fileGroup.getColumnToHadoopFunction(), context.exprMap, analyzer,
                context.srcTupleDescriptor, context.slotDescByName, context.params);
    }

    private TScanRangeLocations newLocations(TBrokerScanRangeParams params, BrokerDesc brokerDesc)
            throws UserException {

        Backend selectedBackend;
        if (brokerDesc.isMultiLoadBroker()) {
            if (!brokerDesc.getProperties().containsKey(BrokerDesc.MULTI_LOAD_BROKER_BACKEND_KEY)) {
                throw new DdlException("backend not found for multi load.");
            }
            String backendId = brokerDesc.getProperties().get(BrokerDesc.MULTI_LOAD_BROKER_BACKEND_KEY);
            selectedBackend = Catalog.getCurrentSystemInfo().getBackend(Long.valueOf(backendId));
            if (selectedBackend == null) {
                throw new DdlException("backend " + backendId + " not found for multi load.");
            }
        } else {
            selectedBackend = backends.get(nextBe++);
            nextBe = nextBe % backends.size();
        }

        // Generate on broker scan range
        TBrokerScanRange brokerScanRange = new TBrokerScanRange();
        brokerScanRange.setParams(params);
        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            FsBroker broker = null;
            try {
                broker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(brokerDesc.getName(), selectedBackend.getHost());
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            brokerScanRange.addToBrokerAddresses(new TNetworkAddress(broker.ip, broker.port));
        } else {
            brokerScanRange.setBrokerAddresses(new ArrayList<>());
        }

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

    private TBrokerScanRange brokerScanRange(TScanRangeLocations locations) {
        return locations.scan_range.broker_scan_range;
    }

    private void getFileStatusAndCalcInstance() throws UserException {
        if (fileStatusesList == null || filesAdded == -1) {
            // FIXME(cmy): fileStatusesList and filesAdded can be set out of db lock when doing pull load,
            // but for now it is very difficult to set them out of db lock when doing broker query.
            // So we leave this code block here.
            // This will be fixed later.
            fileStatusesList = Lists.newArrayList();
            filesAdded = 0;
            for (BrokerFileGroup fileGroup : fileGroups) {
                boolean isBinaryFileFormat = fileGroup.isBinaryFileFormat();
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (int i = 0; i < fileGroup.getFilePaths().size(); i++) {
                    if (brokerDesc.isMultiLoadBroker()) {
                        TBrokerFileStatus fileStatus = new TBrokerFileStatus(fileGroup.getFilePaths().get(i),
                                false, fileGroup.getFileSize().get(i), false);
                        fileStatuses.add(fileStatus);
                    } else {
                        BrokerUtil.parseFile(fileGroup.getFilePaths().get(i), brokerDesc, fileStatuses);
                    }
                }

                // only get non-empty file or non-binary file
                fileStatuses = fileStatuses.stream().filter(f -> {
                    return f.getSize() > 0 || !isBinaryFileFormat;
                }).collect(Collectors.toList());

                fileStatusesList.add(fileStatuses);
                filesAdded += fileStatuses.size();
                for (TBrokerFileStatus fstatus : fileStatuses) {
                    LOG.info("Add file status is {}", fstatus);
                }
            }
        }
        Preconditions.checkState(fileStatusesList.size() == fileGroups.size());

        if (isLoad() && filesAdded == 0) {
            throw new UserException("No source file in this table(" + targetTable.getName() + ").");
        }

        totalBytes = 0;
        for (List<TBrokerFileStatus> fileStatuses : fileStatusesList) {
            if (!brokerDesc.isMultiLoadBroker()) {
                Collections.sort(fileStatuses, T_BROKER_FILE_STATUS_COMPARATOR);
            }
            for (TBrokerFileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.size;
            }
        }
        numInstances = 1;
        if (!brokerDesc.isMultiLoadBroker()) {
            numInstances = (int) (totalBytes / Config.min_bytes_per_broker_scanner);
            int totalLoadParallelism = loadParallelism * backends.size();
            numInstances = Math.min(totalLoadParallelism, numInstances);
            numInstances = Math.min(numInstances, Config.max_broker_concurrency);
            numInstances = Math.max(1, numInstances);
        }

        bytesPerInstance = totalBytes / numInstances + 1;

        if (bytesPerInstance > Config.max_bytes_per_broker_scanner) {
            throw new UserException(
                    "Scan bytes per broker scanner exceed limit: " + Config.max_bytes_per_broker_scanner);
        }
        LOG.info("number instance of broker scan node is: {}, bytes per instance: {}", numInstances, bytesPerInstance);
    }

    private void assignBackends() throws UserException {
        backends = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAvailable()) {
                backends.add(be);
            }
        }
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
        Collections.shuffle(backends, random);
    }

    private TFileFormatType formatType(String fileFormat, String path) {
        if (fileFormat != null) {
            if (fileFormat.toLowerCase().equals("parquet")) {
                return TFileFormatType.FORMAT_PARQUET;
            } else if (fileFormat.toLowerCase().equals("orc")) {
                return TFileFormatType.FORMAT_ORC;
            } else if (fileFormat.toLowerCase().equals("json")) {
                return TFileFormatType.FORMAT_JSON;
            } else if (fileFormat.toLowerCase().equals("csv")) {
                return TFileFormatType.FORMAT_CSV_PLAIN;
            }
        }

        String lowerCasePath = path.toLowerCase();
        if (lowerCasePath.endsWith(".parquet") || lowerCasePath.endsWith(".parq")) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCasePath.endsWith(".gz")) {
            return TFileFormatType.FORMAT_CSV_GZ;
        } else if (lowerCasePath.endsWith(".bz2")) {
            return TFileFormatType.FORMAT_CSV_BZ2;
        } else if (lowerCasePath.endsWith(".lz4")) {
            return TFileFormatType.FORMAT_CSV_LZ4FRAME;
        } else if (lowerCasePath.endsWith(".lzo")) {
            return TFileFormatType.FORMAT_CSV_LZOP;
        } else if (lowerCasePath.endsWith(".deflate")) {
            return TFileFormatType.FORMAT_CSV_DEFLATE;
        } else {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void processFileGroup(
            ParamCreateContext context,
            List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (fileStatuses  == null || fileStatuses.isEmpty()) {
            return;
        }
        TScanRangeLocations curLocations = newLocations(context.params, brokerDesc);
        long curInstanceBytes = 0;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size(); ) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            long leftBytes = fileStatus.size - curFileOffset;
            long tmpBytes = curInstanceBytes + leftBytes;
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormat(), fileStatus.path);
            List<String> columnsFromPath = BrokerUtil.parseColumnsFromPath(fileStatus.path,
                    context.fileGroup.getColumnsFromPath());
            int numberOfColumnsFromFile = context.slotDescByName.size() - columnsFromPath.size();
            if (tmpBytes > bytesPerInstance) {
                // Now only support split plain text
                if ((formatType == TFileFormatType.FORMAT_CSV_PLAIN && fileStatus.isSplitable)
                        || formatType == TFileFormatType.FORMAT_JSON) {
                    long rangeBytes = bytesPerInstance - curInstanceBytes;
                    TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                            rangeBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc);
                    if (formatType == TFileFormatType.FORMAT_JSON) {
                        rangeDesc.setStripOuterArray(context.fileGroup.isStripOuterArray());
                        rangeDesc.setJsonpaths(context.fileGroup.getJsonPaths());
                        rangeDesc.setJsonRoot(context.fileGroup.getJsonRoot());
                        rangeDesc.setFuzzyParse(context.fileGroup.isFuzzyParse());
                    }
                    brokerScanRange(curLocations).addToRanges(rangeDesc);
                    curFileOffset += rangeBytes;

                } else {
                    TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                            leftBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc);
                    brokerScanRange(curLocations).addToRanges(rangeDesc);
                    curFileOffset = 0;
                    i++;
                }

                // New one scan
                locationsList.add(curLocations);
                curLocations = newLocations(context.params, brokerDesc);
                curInstanceBytes = 0;

            } else {
                TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                        leftBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc);
                if (formatType == TFileFormatType.FORMAT_JSON) {
                    rangeDesc.setStripOuterArray(context.fileGroup.isStripOuterArray());
                    rangeDesc.setJsonpaths(context.fileGroup.getJsonPaths());
                    rangeDesc.setJsonRoot(context.fileGroup.getJsonRoot());
                    rangeDesc.setFuzzyParse(context.fileGroup.isFuzzyParse());
                }
                brokerScanRange(curLocations).addToRanges(rangeDesc);
                curFileOffset = 0;
                curInstanceBytes += leftBytes;
                i++;
            }
        }

        // Put the last file
        if (brokerScanRange(curLocations).isSetRanges()) {
            locationsList.add(curLocations);
        }
    }

    private TBrokerRangeDesc createBrokerRangeDesc(long curFileOffset, TBrokerFileStatus fileStatus,
                                                   TFileFormatType formatType, long rangeBytes,
                                                   List<String> columnsFromPath, int numberOfColumnsFromFile,
                                                   BrokerDesc brokerDesc) {
        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFileType(brokerDesc.getFileType());
        rangeDesc.setFormatType(formatType);
        rangeDesc.setPath(fileStatus.path);
        rangeDesc.setSplittable(fileStatus.isSplitable);
        rangeDesc.setStartOffset(curFileOffset);
        rangeDesc.setSize(rangeBytes);
        rangeDesc.setFileSize(fileStatus.size);
        rangeDesc.setNumOfColumnsFromFile(numberOfColumnsFromFile);
        rangeDesc.setColumnsFromPath(columnsFromPath);
        return rangeDesc;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        locationsList = Lists.newArrayList();

        for (int i = 0; i < fileGroups.size(); ++i) {
            List<TBrokerFileStatus> fileStatuses = fileStatusesList.get(i);
            if (fileStatuses.isEmpty()) {
                continue;
            }
            ParamCreateContext context = paramCreateContexts.get(i);
            try {
                finalizeParams(context.slotDescByName, context.exprMap, context.params,
                        context.srcTupleDescriptor, strictMode, context.fileGroup.isNegative(), analyzer);
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            processFileGroup(context, fileStatuses);
        }
        if (LOG.isDebugEnabled()) {
            for (TScanRangeLocations locations : locationsList) {
                LOG.debug("Scan range is {}", locations);
            }
        }

        if (loadJobId != -1) {
            LOG.info("broker load job {} with txn {} has {} scan range: {}",
                    loadJobId, txnId, locationsList.size(),
                    brokerDesc.isMultiLoadBroker() ? "local"
                            : locationsList.stream().map(loc -> loc.locations.get(0).backend_id).toArray());
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return locationsList;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            BrokerTable brokerTable = (BrokerTable) targetTable;
            output.append(prefix).append("TABLE: ").append(brokerTable.getName()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(Joiner.on(",").join(brokerTable.getPaths())).append("\",\n");
        }
        output.append(prefix).append("BROKER: ").append(brokerDesc.getName()).append("\n");
        return output.toString();
    }
}



