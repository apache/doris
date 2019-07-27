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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanNode;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

// Broker scan node
public class BrokerScanNode extends ScanNode {
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
    private int numInstances;
    private long bytesPerInstance;

    // Parameters need to process
    private long loadJobId = -1; // -1 means this scan node is not for a load job
    private long txnId = -1;
    private Table targetTable;
    private BrokerDesc brokerDesc;
    private List<BrokerFileGroup> fileGroups;
    private boolean strictMode;

    private List<List<TBrokerFileStatus>> fileStatusesList;
    // file num
    private int filesAdded;

    // Only used for external table in select statement
    private List<Backend> backends;
    private int nextBe = 0;

    private Analyzer analyzer;

    private List<DataSplitSink.EtlRangePartitionInfo> partitionInfos;
    private List<Expr> partitionExprs;

    private static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TBrokerScanRangeParams params;
        public TupleDescriptor tupleDescriptor;
        public Map<String, Expr> exprMap;
        public Map<String, SlotDescriptor> slotDescByName;
    }

    private List<ParamCreateContext> paramCreateContexts;

    public BrokerScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, desc, planNodeName);
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
            try {
                initParams(context);
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
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
                            boolean strictMode) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.strictMode = strictMode;
    }

    private void createPartitionInfos() throws AnalysisException {
        if (partitionInfos != null) {
            return;
        }

        Map<String, Expr> exprByName = Maps.newHashMap();

        for (SlotDescriptor slotDesc : desc.getSlots()) {
            exprByName.put(slotDesc.getColumn().getName(), new SlotRef(slotDesc));
        }

        partitionExprs = Lists.newArrayList();
        partitionInfos = DataSplitSink.EtlRangePartitionInfo.createParts(
                (OlapTable) targetTable, exprByName, null, partitionExprs);
    }

    private void parseExprMap(Map<String, Expr> exprMap) throws UserException {
        if (exprMap == null) {
            return;
        }
        for (Map.Entry<String, Expr> entry : exprMap.entrySet()) {
            String colName = entry.getKey();
            Expr originExpr = entry.getValue();

            Column column = targetTable.getColumn(colName);
            if (column == null) {
                throw new UserException("Unknown column(" + colName + ")");
            }

            // To compatible with older load version
            if (originExpr instanceof FunctionCallExpr) {
                FunctionCallExpr funcExpr = (FunctionCallExpr) originExpr;
                String funcName = funcExpr.getFnName().getFunction();

                if (funcName.equalsIgnoreCase("replace_value")) {
                    List<Expr> exprs = Lists.newArrayList();
                    SlotRef slotRef = new SlotRef(null, entry.getKey());
                    // We will convert this to IF(`col` != child0, `col`, child1),
                    // because we need the if return type equal to `col`, we use NE
                    //
                    exprs.add(new BinaryPredicate(BinaryPredicate.Operator.NE, slotRef, funcExpr.getChild(0)));
                    exprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        exprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            exprs.add(new StringLiteral(column.getDefaultValue()));
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + colName + ") has no default value.");
                            }
                        }
                    }
                    FunctionCallExpr newFn = new FunctionCallExpr("if", exprs);
                    entry.setValue(newFn);
                } else if (funcName.equalsIgnoreCase("strftime")) {
                    FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                    List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                    FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                            fromUnixName, new FunctionParams(false, fromUnixArgs));

                    entry.setValue(fromUnixFunc);
                } else if (funcName.equalsIgnoreCase("time_format")) {
                    FunctionName strToDateName = new FunctionName("STR_TO_DATE");
                    List<Expr> strToDateExprs = Lists.newArrayList(funcExpr.getChild(2), funcExpr.getChild(1));
                    FunctionCallExpr strToDateFuncExpr = new FunctionCallExpr(
                            strToDateName, new FunctionParams(false, strToDateExprs));

                    FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                    List<Expr> dateFormatArgs = Lists.newArrayList(strToDateFuncExpr, funcExpr.getChild(0));
                    FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                            dateFormatName, new FunctionParams(false, dateFormatArgs));

                    entry.setValue(dateFormatFunc);
                } else if (funcName.equalsIgnoreCase("alignment_timestamp")) {
                    FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                    List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                    FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                            fromUnixName, new FunctionParams(false, fromUnixArgs));

                    StringLiteral precision = (StringLiteral) funcExpr.getChild(0);
                    StringLiteral format;
                    if (precision.getStringValue().equalsIgnoreCase("year")) {
                        format = new StringLiteral("%Y-01-01 00:00:00");
                    } else if (precision.getStringValue().equalsIgnoreCase("month")) {
                        format = new StringLiteral("%Y-%m-01 00:00:00");
                    } else if (precision.getStringValue().equalsIgnoreCase("day")) {
                        format = new StringLiteral("%Y-%m-%d 00:00:00");
                    } else if (precision.getStringValue().equalsIgnoreCase("hour")) {
                        format = new StringLiteral("%Y-%m-%d %H:00:00");
                    } else {
                        throw new UserException("Unknown precision(" + precision.getStringValue() + ")");
                    }
                    FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                    List<Expr> dateFormatArgs = Lists.newArrayList(fromUnixFunc, format);
                    FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                            dateFormatName, new FunctionParams(false, dateFormatArgs));

                    FunctionName unixTimeName = new FunctionName("UNIX_TIMESTAMP");
                    List<Expr> unixTimeArgs = Lists.newArrayList();
                    unixTimeArgs.add(dateFormatFunc);
                    FunctionCallExpr unixTimeFunc = new FunctionCallExpr(
                            unixTimeName, new FunctionParams(false, unixTimeArgs));

                    entry.setValue(unixTimeFunc);
                } else if (funcName.equalsIgnoreCase("default_value")) {
                    entry.setValue(funcExpr.getChild(0));
                } else if (funcName.equalsIgnoreCase("now")) {
                    FunctionName nowFunctionName = new FunctionName("NOW");
                    FunctionCallExpr newFunc = new FunctionCallExpr(nowFunctionName, new FunctionParams(null));
                    entry.setValue(newFunc);
                }
            }
        }
    }

    // Called from init, construct source tuple information
    private void initParams(ParamCreateContext context) throws AnalysisException, UserException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        context.params = params;

        BrokerFileGroup fileGroup = context.fileGroup;
        params.setColumn_separator(fileGroup.getValueSeparator().getBytes(Charset.forName("UTF-8"))[0]);
        params.setLine_delimiter(fileGroup.getLineDelimiter().getBytes(Charset.forName("UTF-8"))[0]);
        params.setStrict_mode(strictMode);

        // Parse partition information
        List<Long> partitionIds = fileGroup.getPartitionIds();
        if (partitionIds != null && partitionIds.size() > 0) {
            params.setPartition_ids(partitionIds);
            createPartitionInfos();
        }

        params.setProperties(brokerDesc.getProperties());

        // We must create a new map here, because we will change this map later.
        // But fileGroup will be persisted later, so we keep it unchanged.
        if (fileGroup.getExprColumnMap() != null) {
            context.exprMap = Maps.newHashMap(fileGroup.getExprColumnMap());
        } else {
            context.exprMap = null;
        }
        parseExprMap(context.exprMap);

        // Generate expr
        List<String> fileFieldNames = fileGroup.getFileFieldNames();
        if (fileFieldNames == null) {
            fileFieldNames = Lists.newArrayList();
            for (Column column : targetTable.getBaseSchema()) {
                fileFieldNames.add(column.getName());
            }
        } else {
            // change fileFiledName to real column name(case match)
            fileFieldNames = fileFieldNames.stream().map(
                    f -> targetTable.getColumn(f) == null ? f : targetTable.getColumn(f).getName()).collect(
                            Collectors.toList());
        }

        // This tuple descriptor is used for file of
        TupleDescriptor srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor();
        context.tupleDescriptor = srcTupleDesc;

        Map<String, SlotDescriptor> slotDescByName = Maps.newHashMap();
        context.slotDescByName = slotDescByName;
        for (String fieldName : fileFieldNames) {
            SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);
            slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
            slotDesc.setIsMaterialized(true);
            slotDesc.setIsNullable(false);
            slotDesc.setColumn(new Column(fieldName, PrimitiveType.VARCHAR));
            slotDescByName.put(fieldName, slotDesc);

            params.addToSrc_slot_ids(slotDesc.getId().asInt());
        }
        params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
    }

    private void finalizeParams(ParamCreateContext context) throws UserException, AnalysisException {
        Map<String, SlotDescriptor> slotDescByName = context.slotDescByName;
        Map<String, Expr> exprMap = context.exprMap;
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        // Analyze expr map
        if (exprMap != null) {
            for (Map.Entry<String, Expr> entry : exprMap.entrySet()) {
                ExprSubstitutionMap smap = new ExprSubstitutionMap();

                List<SlotRef> slots = Lists.newArrayList();
                entry.getValue().collect(SlotRef.class, slots);

                for (SlotRef slot : slots) {
                    SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                    if (slotDesc == null) {
                        throw new UserException("Unknown slot");
                    }
                    smap.getLhs().add(slot);
                    smap.getRhs().add(new SlotRef(slotDesc));
                }

                Expr expr = entry.getValue().clone(smap);
                expr.analyze(analyzer);
                exprMap.put(entry.getKey(), expr);
            }
        }

        boolean isNegative = context.fileGroup.isNegative();
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
                            throw new UserException("Unknown slot ref("
                                                            + destSlotDesc.getColumn().getName() + ") in source file");
                        }
                    }
                }
            }

            if (isNegative && destSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr.analyze(analyzer);
            }
            expr = castToSlot(destSlotDesc, expr);
            context.params.putToExpr_of_dest_slot(destSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        context.params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
        context.params.setDest_tuple_id(desc.getId().asInt());
        context.params.setStrict_mode(strictMode);
        // Need re compute memory layout after set some slot descriptor to nullable
        context.tupleDescriptor.computeMemLayout();
    }

    private TScanRangeLocations newLocations(TBrokerScanRangeParams params, String brokerName)
            throws UserException {
        List<Backend> candidateBes = Lists.newArrayList();
        // Get backend
        int numBe = Math.min(3, backends.size());
        for (int i = 0; i < numBe; ++i) {
            candidateBes.add(backends.get(nextBe++));
            nextBe = nextBe % backends.size();
        }
        // we shuffle it because if we only has 3 backends
        // we will always choose the same backends without shuffle
        Collections.shuffle(candidateBes);

        // Generate on broker scan range
        TBrokerScanRange brokerScanRange = new TBrokerScanRange();
        brokerScanRange.setParams(params);
        int numBroker = Math.min(3, numBe);
        for (int i = 0; i < numBroker; ++i) {
            FsBroker broker = null;
            try {
                broker = Catalog.getInstance().getBrokerMgr().getBroker(
                        brokerName, candidateBes.get(i).getHost());
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            brokerScanRange.addToBroker_addresses(new TNetworkAddress(broker.ip, broker.port));
        }

        // Scan range
        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScan_range(scanRange);
        for (Backend be : candidateBes) {
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(be.getId());
            location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
            locations.addToLocations(location);
        }

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
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    BrokerUtil.parseBrokerFile(path, brokerDesc, fileStatuses);
                }
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
            Collections.sort(fileStatuses, T_BROKER_FILE_STATUS_COMPARATOR);
            for (TBrokerFileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.size;
            }
        }

        numInstances = (int) (totalBytes / Config.min_bytes_per_broker_scanner);
        numInstances = Math.min(backends.size(), numInstances);
        numInstances = Math.min(numInstances, Config.max_broker_concurrency);
        numInstances = Math.max(1, numInstances);

        bytesPerInstance = totalBytes / numInstances + 1;

        if (bytesPerInstance > Config.max_bytes_per_broker_scanner) {
            throw new UserException(
                    "Scan bytes per broker scanner exceed limit: " + Config.max_bytes_per_broker_scanner);
        }
    }

    private void assignBackends() throws UserException {
        backends = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                backends.add(be);
            }
        }
        if (backends.isEmpty()) {
            throw new UserException("No Alive backends");
        }
        Collections.shuffle(backends, random);
    }

    private TFileFormatType formatType(String fileFormat, String path) {
        if (fileFormat != null && fileFormat.toLowerCase().equals("parquet")) {
            return TFileFormatType.FORMAT_PARQUET;
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
        } else {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    private void processFileGroup(
            String fileFormat,
            TBrokerScanRangeParams params,
            List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (fileStatuses == null || fileStatuses.isEmpty()) {
            return;
        }

        TScanRangeLocations curLocations = newLocations(params, brokerDesc.getName());
        long curInstanceBytes = 0;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size(); ) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            long leftBytes = fileStatus.size - curFileOffset;
            long tmpBytes = curInstanceBytes + leftBytes;
            TFileFormatType formatType = formatType(fileFormat, fileStatus.path);
            if (tmpBytes > bytesPerInstance) {
                // Now only support split plain text
                if (formatType == TFileFormatType.FORMAT_CSV_PLAIN && fileStatus.isSplitable) {
                    long rangeBytes = bytesPerInstance - curInstanceBytes;

                    TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
                    rangeDesc.setFile_type(TFileType.FILE_BROKER);
                    rangeDesc.setFormat_type(formatType);
                    rangeDesc.setPath(fileStatus.path);
                    rangeDesc.setSplittable(fileStatus.isSplitable);
                    rangeDesc.setStart_offset(curFileOffset);
                    rangeDesc.setSize(rangeBytes);
                    brokerScanRange(curLocations).addToRanges(rangeDesc);

                    curFileOffset += rangeBytes;
                } else {
                    TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
                    rangeDesc.setFile_type(TFileType.FILE_BROKER);
                    rangeDesc.setFormat_type(formatType);
                    rangeDesc.setPath(fileStatus.path);
                    rangeDesc.setSplittable(fileStatus.isSplitable);
                    rangeDesc.setStart_offset(curFileOffset);
                    rangeDesc.setSize(leftBytes);
                    brokerScanRange(curLocations).addToRanges(rangeDesc);

                    curFileOffset = 0;
                    i++;
                }

                // New one scan
                locationsList.add(curLocations);
                curLocations = newLocations(params, brokerDesc.getName());
                curInstanceBytes = 0;

            } else {
                TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
                rangeDesc.setFile_type(TFileType.FILE_BROKER);
                rangeDesc.setFormat_type(formatType);
                rangeDesc.setPath(fileStatus.path);
                rangeDesc.setSplittable(fileStatus.isSplitable);
                rangeDesc.setStart_offset(curFileOffset);
                rangeDesc.setSize(leftBytes);
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
                finalizeParams(context);
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            processFileGroup(context.fileGroup.getFileFormat(), context.params, fileStatuses);
        }
        if (LOG.isDebugEnabled()) {
            for (TScanRangeLocations locations : locationsList) {
                LOG.debug("Scan range is {}", locations);
            }
        }

        if (loadJobId != -1) {
            LOG.info("broker load job {} with txn {} has {} scan range: {}",
                    loadJobId, txnId, locationsList.size(),
                    locationsList.stream().map(loc -> loc.locations.get(0).backend_id).toArray());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.BROKER_SCAN_NODE;
        TBrokerScanNode brokerScanNode = new TBrokerScanNode(desc.getId().asInt());
        if (partitionInfos != null) {
            brokerScanNode.setPartition_exprs(Expr.treesToThrift(partitionExprs));
            brokerScanNode.setPartition_infos(DataSplitSink.EtlRangePartitionInfo.listToNonDistThrift(partitionInfos));
        }
        msg.setBroker_scan_node(brokerScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return locationsList;
    }

    @Override
    public int getNumInstances() {
        return numInstances;
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
        if (brokerDesc != null) {
            output.append(prefix).append("BROKER: ").append(brokerDesc.getName()).append("\n");
        }
        return output.toString();
    }

}
