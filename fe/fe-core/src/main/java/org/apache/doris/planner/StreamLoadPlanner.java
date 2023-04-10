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
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

// Used to generate a plan fragment for a streaming load.
// we only support OlapTable now.
// TODO(zc): support other type table
public class StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(StreamLoadPlanner.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private LoadTaskInfo taskInfo;

    private Analyzer analyzer;
    private DescriptorTable descTable;

    private ScanNode scanNode;
    private TupleDescriptor tupleDesc;

    public StreamLoadPlanner(Database db, OlapTable destTable, LoadTaskInfo taskInfo) {
        this.db = db;
        this.destTable = destTable;
        this.taskInfo = taskInfo;
    }

    private void resetAnalyzer() {
        analyzer = new Analyzer(Env.getCurrentEnv(), null);
        // TODO(cmy): currently we do not support UDF in stream load command.
        // Because there is no way to check the privilege of accessing UDF..
        analyzer.setUDFAllowed(false);
        descTable = analyzer.getDescTbl();
    }

    // can only be called after "plan()", or it will return null
    public OlapTable getDestTable() {
        return destTable;
    }

    // create the plan. the plan's query id and load id are same, using the parameter 'loadId'
    public TExecPlanFragmentParams plan(TUniqueId loadId) throws UserException {
        if (destTable.getKeysType() != KeysType.UNIQUE_KEYS
                && taskInfo.getMergeType() != LoadTask.MergeType.APPEND) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (taskInfo.getMergeType() != LoadTask.MergeType.APPEND
                && !destTable.hasDeleteSign()) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }

        if (destTable.hasSequenceCol() && !taskInfo.hasSequenceCol() && destTable.getSequenceMapCol() == null) {
            throw new UserException("Table " + destTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (!destTable.hasSequenceCol() && taskInfo.hasSequenceCol()) {
            throw new UserException("There is no sequence column in the table " + destTable.getName());
        }
        resetAnalyzer();
        // construct tuple descriptor, used for dataSink
        tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        TupleDescriptor scanTupleDesc = tupleDesc;
        // note: we use two tuples separately for Scan and Sink here to avoid wrong nullable info.
        // construct tuple descriptor, used for scanNode
        scanTupleDesc = descTable.createTupleDescriptor("ScanTuple");
        boolean negative = taskInfo.getNegative();
        // here we should be full schema to fill the descriptor table
        for (Column col : destTable.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            SlotDescriptor scanSlotDesc = descTable.addSlotDescriptor(scanTupleDesc);
            scanSlotDesc.setIsMaterialized(true);
            scanSlotDesc.setColumn(col);
            scanSlotDesc.setIsNullable(col.isAllowNull());
            for (ImportColumnDesc importColumnDesc : taskInfo.getColumnExprDescs().descs) {
                try {
                    if (!importColumnDesc.isColumn() && importColumnDesc.getColumnName() != null
                            && importColumnDesc.getColumnName().equals(col.getName())) {
                        scanSlotDesc.setIsNullable(importColumnDesc.getExpr().isNullable());
                        break;
                    }
                } catch (Exception e) {
                    // An exception may be thrown here because the `importColumnDesc.getExpr()` is not analyzed now.
                    // We just skip this case here.
                }
            }
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggregateType. column:" + col.getName());
            }
        }

        // Plan scan tuple of dynamic table
        if (destTable.isDynamicSchema()) {
            descTable.addReferencedTable(destTable);
            scanTupleDesc.setTable(destTable);
            // add a implict container column "DORIS_DYNAMIC_COL" for dynamic columns
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(scanTupleDesc);
            Column col = new Column(Column.DYNAMIC_COLUMN_NAME, Type.VARIANT, false, null, false, "",
                                    "stream load auto dynamic column");
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
            slotDesc.setNullIndicatorBit(-1);
            slotDesc.setNullIndicatorByte(0);
            slotDesc.setIsNullable(false);
            LOG.debug("plan tupleDesc {}", scanTupleDesc.toString());
        }

        // create scan node
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), scanTupleDesc);
        // 1. create file group
        DataDescription dataDescription = new DataDescription(destTable.getName(), taskInfo);
        dataDescription.analyzeWithoutCheckPriv(db.getFullName());
        BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
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
        // The load id will pass to csv reader to find the stream load context from new load stream manager
        fileScanNode.setLoadInfo(loadId, taskInfo.getTxnId(), destTable, BrokerDesc.createForStreamLoad(),
                fileGroup, fileStatus, taskInfo.isStrictMode(), taskInfo.getFileType(), taskInfo.getHiddenColumns());
        scanNode = fileScanNode;

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.convertToVectorized();
        descTable.computeStatAndMemLayout();

        int timeout = taskInfo.getTimeout();
        if (taskInfo instanceof RoutineLoadJob) {
            // For routine load, make the timeout fo plan fragment larger than MaxIntervalS config.
            // So that the execution won't be killed before consuming finished.
            timeout *= 2;
        }

        // create dest sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(destTable, tupleDesc, partitionIds,
                Config.enable_single_replica_load);
        olapTableSink.init(loadId, taskInfo.getTxnId(), db.getId(), timeout,
                taskInfo.getSendBatchParallelism(), taskInfo.isLoadToSingleTablet());
        olapTableSink.complete();

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);

        fragment.finalize(null);

        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDescTbl(analyzer.getDescTbl().toThrift());
        params.setCoord(new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port));

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // user load id (streamLoadTask.id) as query id
        execParams.setQueryId(loadId);
        execParams.setFragmentInstanceId(new TUniqueId(loadId.hi, loadId.lo + 1));
        execParams.per_exch_num_senders = Maps.newHashMap();
        execParams.destinations = Lists.newArrayList();
        Map<Integer, List<TScanRangeParams>> perNodeScanRange = Maps.newHashMap();
        List<TScanRangeParams> scanRangeParams = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            scanRangeParams.add(new TScanRangeParams(locations.getScanRange()));
        }
        // For stream load, only one sender
        execParams.setSenderId(0);
        execParams.setNumSenders(1);
        perNodeScanRange.put(scanNode.getId().asInt(), scanRangeParams);
        execParams.setPerNodeScanRanges(perNodeScanRange);
        params.setParams(execParams);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQueryType(TQueryType.LOAD);
        queryOptions.setQueryTimeout(timeout);
        queryOptions.setMemLimit(taskInfo.getMemLimit());
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setLoadMemLimit(taskInfo.getMemLimit());
        //load
        queryOptions.setEnablePipelineEngine(Config.enable_pipeline_load);
        queryOptions.setBeExecVersion(Config.be_exec_version);
        queryOptions.setIsReportSuccess(taskInfo.getEnableProfile());

        params.setQueryOptions(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setTimeZone(taskInfo.getTimezone());
        queryGlobals.setLoadZeroTolerance(taskInfo.getMaxFilterRatio() <= 0.0);
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());

        params.setQueryGlobals(queryGlobals);

        // LOG.debug("stream load txn id: {}, plan: {}", streamLoadTask.getTxnId(), params);
        return params;
    }

    // get all specified partition ids.
    // if no partition specified, return null
    private List<Long> getAllPartitionIds() throws DdlException, AnalysisException {
        List<Long> partitionIds = Lists.newArrayList();

        PartitionNames partitionNames = taskInfo.getPartitions();
        if (partitionNames != null) {
            for (String partName : partitionNames.getPartitionNames()) {
                Partition part = destTable.getPartition(partName, partitionNames.isTemp());
                if (part == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_PARTITION, partName, destTable.getName());
                }
                partitionIds.add(part.getId());
            }
            return partitionIds;
        }
        List<Expr> conjuncts = scanNode.getConjuncts();
        if (destTable.getPartitionInfo().getType() != PartitionType.UNPARTITIONED && !conjuncts.isEmpty()) {
            PartitionInfo partitionInfo = destTable.getPartitionInfo();
            Map<Long, PartitionItem> itemById = partitionInfo.getIdToItem(false);
            Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
            for (Column column : partitionInfo.getPartitionColumns()) {
                SlotDescriptor slotDesc = tupleDesc.getColumnSlot(column.getName());
                if (null == slotDesc) {
                    continue;
                }
                ColumnRange columnRange = ScanNode.createColumnRange(slotDesc, conjuncts);
                if (columnRange != null) {
                    columnNameToRange.put(column.getName(), columnRange);
                }
            }
            if (columnNameToRange.isEmpty()) {
                return null;
            }

            PartitionPruner partitionPruner = null;
            if (destTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                partitionPruner = new RangePartitionPrunerV2(itemById,
                        partitionInfo.getPartitionColumns(), columnNameToRange);
            } else if (destTable.getPartitionInfo().getType() == PartitionType.LIST) {
                partitionPruner = new ListPartitionPrunerV2(itemById,
                        partitionInfo.getPartitionColumns(), columnNameToRange);
            }
            partitionIds.addAll(partitionPruner.prune());
            return partitionIds;
        }
        return null;
    }
}

