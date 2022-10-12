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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Broker scan node
 *
 * Since https://github.com/apache/doris/pull/5686, Doris can read data from HDFS without broker by
 * broker scan node.
 * Broker scan node is more likely a file scan node for now.
 * With this feature, we can extend BrokerScanNode to query external table which data is stored in HDFS, such as
 * Hive and Iceberg, etc.
 */
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
    protected Table targetTable;
    protected BrokerDesc brokerDesc;
    protected List<BrokerFileGroup> fileGroups;
    private boolean strictMode = false;
    private int loadParallelism = 1;
    private UserIdentity userIdentity;

    protected List<List<TBrokerFileStatus>> fileStatusesList;
    // file num
    protected int filesAdded;

    // Only used for external table in select statement
    private List<Backend> backends;
    private int nextBe = 0;

    private Analyzer analyzer;

    protected static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TBrokerScanRangeParams params;
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
        public String timezone;
    }

    private List<ParamCreateContext> paramCreateContexts;

    // For broker load and external broker table
    public BrokerScanNode(PlanNodeId id, TupleDescriptor destTupleDesc, String planNodeName,
                          List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, destTupleDesc, planNodeName, StatisticalType.BROKER_SCAN_NODE);
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
        if (ConnectContext.get() != null) {
            this.userIdentity = ConnectContext.get().getCurrentUserIdentity();
        }
    }

    // For hive and iceberg scan node
    public BrokerScanNode(PlanNodeId id, TupleDescriptor destTupleDesc, String planNodeName,
            List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded, StatisticalType statisticalType) {
        super(id, destTupleDesc, planNodeName, statisticalType);
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
        if (ConnectContext.get() != null) {
            this.userIdentity = ConnectContext.get().getCurrentUserIdentity();
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        this.analyzer = analyzer;
        if (desc.getTable() != null) {
            this.initFileGroup();
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

    public List<ParamCreateContext> getParamCreateContexts() {
        return paramCreateContexts;
    }

    protected void initFileGroup() throws UserException {
        BrokerTable brokerTable = (BrokerTable) desc.getTable();
        try {
            fileGroups = Lists.newArrayList(new BrokerFileGroup(brokerTable));
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        brokerDesc = new BrokerDesc(brokerTable.getBrokerName(), brokerTable.getBrokerProperties());
        targetTable = brokerTable;
    }

    protected boolean isLoad() {
        return desc.getTable() == null;
    }

    public void setLoadInfo(long loadJobId,
                            long txnId,
                            Table targetTable,
                            BrokerDesc brokerDesc,
                            List<BrokerFileGroup> fileGroups,
                            boolean strictMode,
                            int loadParallelism,
                            UserIdentity userIdentity) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.strictMode = strictMode;
        this.loadParallelism = loadParallelism;
        this.userIdentity = userIdentity;
    }

    // Called from init, construct source tuple information
    private void initParams(ParamCreateContext context)
            throws UserException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        context.params = params;

        BrokerFileGroup fileGroup = context.fileGroup;
        params.setColumnSeparator(fileGroup.getColumnSeparator().getBytes(Charset.forName("UTF-8"))[0]);
        params.setLineDelimiter(fileGroup.getLineDelimiter().getBytes(Charset.forName("UTF-8"))[0]);
        params.setColumnSeparatorStr(fileGroup.getColumnSeparator());
        params.setLineDelimiterStr(fileGroup.getLineDelimiter());
        params.setColumnSeparatorLength(fileGroup.getColumnSeparator().getBytes(Charset.forName("UTF-8")).length);
        params.setLineDelimiterLength(fileGroup.getLineDelimiter().getBytes(Charset.forName("UTF-8")).length);
        params.setStrictMode(strictMode);
        params.setProperties(brokerDesc.getProperties());
        if (params.getSrcSlotIds() == null) {
            params.setSrcSlotIds(new java.util.ArrayList<java.lang.Integer>());
        }
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
        context.slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        // for load job, column exprs is got from file group
        // for query, there is no column exprs, they will be got from table's schema in "Load.initColumns"
        LoadTaskInfo.ImportColumnDescs columnDescs = new LoadTaskInfo.ImportColumnDescs();
        if (isLoad()) {
            columnDescs.descs = context.fileGroup.getColumnExprList();
            if (mergeType == LoadTask.MergeType.MERGE) {
                columnDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(deleteCondition));
            } else if (mergeType == LoadTask.MergeType.DELETE) {
                columnDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
            }
            // add columnExpr for sequence column
            if (context.fileGroup.hasSequenceCol()) {
                columnDescs.descs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                        new SlotRef(null, context.fileGroup.getSequenceCol())));
            }
        }

        if (targetTable != null) {
            Load.initColumns(targetTable, columnDescs, context.fileGroup.getColumnToHadoopFunction(), context.exprMap,
                    analyzer, context.srcTupleDescriptor, context.slotDescByName, context.params.getSrcSlotIds(),
                    formatType(context.fileGroup.getFileFormat(), ""), null, VectorizedUtil.isVectorized());
        }
    }

    protected TScanRangeLocations newLocations(TBrokerScanRangeParams params, BrokerDesc brokerDesc)
            throws UserException {

        Backend selectedBackend;
        if (brokerDesc.isMultiLoadBroker()) {
            if (!brokerDesc.getProperties().containsKey(BrokerDesc.MULTI_LOAD_BROKER_BACKEND_KEY)) {
                throw new DdlException("backend not found for multi load.");
            }
            String backendId = brokerDesc.getProperties().get(BrokerDesc.MULTI_LOAD_BROKER_BACKEND_KEY);
            selectedBackend = Env.getCurrentSystemInfo().getBackend(Long.valueOf(backendId));
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
                broker = Env.getCurrentEnv().getBrokerMgr()
                        .getBroker(brokerDesc.getName(), selectedBackend.getHost());
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

    private void getFileStatusAndCalcInstance() throws UserException {
        if (fileStatusesList == null || filesAdded == -1) {
            // FIXME(cmy): fileStatusesList and filesAdded can be set out of db lock when doing pull load,
            // but for now it is very difficult to set them out of db lock when doing broker query.
            // So we leave this code block here.
            // This will be fixed later.
            fileStatusesList = Lists.newArrayList();
            filesAdded = 0;
            this.getFileStatus();
        }
        // In hudiScanNode, calculate scan range using its own way which do not need fileStatusesList
        if (!(this instanceof HudiScanNode)) {
            Preconditions.checkState(fileStatusesList.size() == fileGroups.size());
        }

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

    protected void getFileStatus() throws UserException {
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

    private void assignBackends() throws UserException {
        Set<Tag> tags = Sets.newHashSet();
        if (userIdentity != null) {
            tags = Env.getCurrentEnv().getAuth().getResourceTags(userIdentity.getQualifiedUser());
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
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (policy.isMatch(be)) {
                backends.add(be);
            }
        }
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
        Collections.shuffle(backends, random);
    }

    private TFileFormatType formatType(String fileFormat, String path) throws UserException {
        if (fileFormat != null) {
            if (fileFormat.toLowerCase().equals("parquet")) {
                return TFileFormatType.FORMAT_PARQUET;
            } else if (fileFormat.toLowerCase().equals("orc")) {
                return TFileFormatType.FORMAT_ORC;
            } else if (fileFormat.toLowerCase().equals("json")) {
                return TFileFormatType.FORMAT_JSON;
                // csv/csv_with_name/csv_with_names_and_types treat as csv format
            } else if (fileFormat.toLowerCase().equals(FeConstants.csv)
                    || fileFormat.toLowerCase().equals(FeConstants.csv_with_names)
                    || fileFormat.toLowerCase().equals(FeConstants.csv_with_names_and_types)
                    // TODO: Add TEXTFILE to TFileFormatType to Support hive text file format.
                    || fileFormat.toLowerCase().equals(FeConstants.text)) {
                return TFileFormatType.FORMAT_CSV_PLAIN;
            } else {
                throw new UserException("Not supported file format: " + fileFormat);
            }
        }

        return Util.getFileFormatType(path);
    }

    public String getHostUri() throws UserException {
        return "";
    }

    private String getHeaderType(String formatType) {
        if (formatType != null) {
            if (formatType.toLowerCase().equals(FeConstants.csv_with_names)
                    || formatType.toLowerCase().equals(FeConstants.csv_with_names_and_types)) {
                return formatType;
            }
        }
        return "";
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void processFileGroup(
            ParamCreateContext context,
            List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (fileStatuses  == null || fileStatuses.isEmpty()) {
            return;
        }
        // set hdfs params, used to Hive and Iceberg scan
        THdfsParams tHdfsParams = new THdfsParams();
        String fsName = getHostUri();
        tHdfsParams.setFsName(fsName);

        TScanRangeLocations curLocations = newLocations(context.params, brokerDesc);
        long curInstanceBytes = 0;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size(); ) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            long leftBytes = fileStatus.size - curFileOffset;
            long tmpBytes = curInstanceBytes + leftBytes;
            //header_type
            String headerType = getHeaderType(context.fileGroup.getFileFormat());
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormat(), fileStatus.path);
            List<String> columnsFromPath = BrokerUtil.parseColumnsFromPath(fileStatus.path,
                    context.fileGroup.getColumnNamesFromPath());
            int numberOfColumnsFromFile = context.slotDescByName.size() - columnsFromPath.size();
            if (tmpBytes > bytesPerInstance) {
                // Now only support split plain text
                if ((formatType == TFileFormatType.FORMAT_CSV_PLAIN && fileStatus.isSplitable)
                        || formatType == TFileFormatType.FORMAT_JSON) {
                    long rangeBytes = bytesPerInstance - curInstanceBytes;
                    TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                            rangeBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc, headerType);
                    if (formatType == TFileFormatType.FORMAT_JSON) {
                        rangeDesc.setStripOuterArray(context.fileGroup.isStripOuterArray());
                        rangeDesc.setJsonpaths(context.fileGroup.getJsonPaths());
                        rangeDesc.setJsonRoot(context.fileGroup.getJsonRoot());
                        rangeDesc.setFuzzyParse(context.fileGroup.isFuzzyParse());
                        rangeDesc.setNumAsString(context.fileGroup.isNumAsString());
                        rangeDesc.setReadJsonByLine(context.fileGroup.isReadJsonByLine());
                    }
                    curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
                    curFileOffset += rangeBytes;

                } else {
                    TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                            leftBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc, headerType);
                    if (rangeDesc.hdfs_params != null && rangeDesc.hdfs_params.getFsName() == null) {
                        rangeDesc.hdfs_params.setFsName(fsName);
                    } else if (rangeDesc.hdfs_params == null) {
                        rangeDesc.setHdfsParams(tHdfsParams);
                    }

                    rangeDesc.setReadByColumnDef(true);
                    curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
                    curFileOffset = 0;
                    i++;
                }

                // New one scan
                locationsList.add(curLocations);
                curLocations = newLocations(context.params, brokerDesc);
                curInstanceBytes = 0;

            } else {
                TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(curFileOffset, fileStatus, formatType,
                        leftBytes, columnsFromPath, numberOfColumnsFromFile, brokerDesc, headerType);
                if (formatType == TFileFormatType.FORMAT_JSON) {
                    rangeDesc.setStripOuterArray(context.fileGroup.isStripOuterArray());
                    rangeDesc.setJsonpaths(context.fileGroup.getJsonPaths());
                    rangeDesc.setJsonRoot(context.fileGroup.getJsonRoot());
                    rangeDesc.setFuzzyParse(context.fileGroup.isFuzzyParse());
                    rangeDesc.setNumAsString(context.fileGroup.isNumAsString());
                    rangeDesc.setReadJsonByLine(context.fileGroup.isReadJsonByLine());
                }
                if (rangeDesc.hdfs_params != null && rangeDesc.hdfs_params.getFsName() == null) {
                    rangeDesc.hdfs_params.setFsName(fsName);
                } else if (rangeDesc.hdfs_params == null) {
                    rangeDesc.setHdfsParams(tHdfsParams);
                }

                rangeDesc.setReadByColumnDef(true);
                curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
                curFileOffset = 0;
                curInstanceBytes += leftBytes;
                i++;
            }
        }

        // Put the last file
        if (curLocations.getScanRange().getBrokerScanRange().isSetRanges()) {
            locationsList.add(curLocations);
        }
    }

    private TBrokerRangeDesc createBrokerRangeDesc(long curFileOffset, TBrokerFileStatus fileStatus,
                                                   TFileFormatType formatType, long rangeBytes,
                                                   List<String> columnsFromPath, int numberOfColumnsFromFile,
                                                   BrokerDesc brokerDesc, String headerType) {
        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFileType(brokerDesc.getFileType());
        rangeDesc.setFormatType(formatType);
        rangeDesc.setPath(fileStatus.path);
        rangeDesc.setSplittable(fileStatus.isSplitable);
        rangeDesc.setStartOffset(curFileOffset);
        rangeDesc.setSize(rangeBytes);
        // fileSize only be used when format is orc or parquet and TFileType is broker
        // When TFileType is other type, it is not necessary
        rangeDesc.setFileSize(fileStatus.size);
        // In Backend, will append columnsFromPath to the end of row after data scanned from file.
        rangeDesc.setNumOfColumnsFromFile(numberOfColumnsFromFile);
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setHeaderType(headerType);
        // set hdfs params for hdfs file type.
        if (brokerDesc.getFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = BrokerUtil.generateHdfsParam(brokerDesc.getProperties());
            rangeDesc.setHdfsParams(tHdfsParams);
        }
        return rangeDesc;
    }

    //TODO(wx):support quantile state column or forbidden it.
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
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            BrokerTable brokerTable = (BrokerTable) targetTable;
            output.append(prefix).append("TABLE: ").append(brokerTable.getName()).append("\n");
            if (detailLevel != TExplainLevel.BRIEF) {
                output.append(prefix).append("PATH: ")
                        .append(Joiner.on(",").join(brokerTable.getPaths())).append("\",\n");
            }
        }
        output.append(prefix).append("BROKER: ").append(brokerDesc.getName()).append("\n");
        return output.toString();
    }
}
