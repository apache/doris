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
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import com.google.common.collect.Range;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TLoadErrorHubInfo;
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
import java.util.Collections;
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

    private long windowIntervalSec;

    public StreamLoadPlanner(Database db, OlapTable destTable, LoadTaskInfo taskInfo) {
        this.db = db;
        this.destTable = destTable;
        this.taskInfo = taskInfo;
        this.windowIntervalSec = -1;
    }

    private void resetAnalyzer() {
        analyzer = new Analyzer(Catalog.getCurrentCatalog(), null);
        // TODO(cmy): currently we do not support UDF in stream load command.
        // Because there is no way to check the privilege of accessing UDF..
        analyzer.setUDFAllowed(false);
        descTable = analyzer.getDescTbl();
    }

    public void setWindowIntervalSec(long windowIntervalSec) {
        this.windowIntervalSec = windowIntervalSec;
    }

    public long getWindowIntervalSec() {
        return windowIntervalSec;
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
                && !destTable.hasDeleteSign() ) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }

        if (destTable.hasSequenceCol() && !taskInfo.hasSequenceCol()) {
            throw new UserException("Table " + destTable.getName() + " has sequence column, need to specify the sequence column");
        }
        if (!destTable.hasSequenceCol() && taskInfo.hasSequenceCol()) {
            throw new UserException("There is no sequence column in the table " + destTable.getName());
        }
        resetAnalyzer();
        // construct tuple descriptor, used for scanNode and dataSink
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        boolean negative = taskInfo.getNegative();
        // here we should be full schema to fill the descriptor table
        for (Column col : destTable.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggregateType. column:" + col.getName());
            }
        }

        // create scan node
        StreamLoadScanNode scanNode = new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable, taskInfo);
        scanNode.init(analyzer);
        descTable.computeMemLayout();
        scanNode.finalize(analyzer);

        // create dest sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(destTable, tupleDesc, partitionIds);
        olapTableSink.init(loadId, taskInfo.getTxnId(), db.getId(), taskInfo.getTimeout());
        olapTableSink.complete();

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);

        fragment.finalize(null, false);

        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDescTbl(analyzer.getDescTbl().toThrift());

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
        queryOptions.setQueryTimeout(taskInfo.getTimeout());
        queryOptions.setMemLimit(taskInfo.getMemLimit());
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setLoadMemLimit(taskInfo.getMemLimit());
        params.setQueryOptions(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        queryGlobals.setTimestampMs(new Date().getTime());
        queryGlobals.setTimeZone(taskInfo.getTimezone());
        params.setQueryGlobals(queryGlobals);

        // set load error hub if exist
        LoadErrorHub.Param param = Catalog.getCurrentCatalog().getLoadInstance().getLoadErrorHubInfo();
        if (param != null) {
            TLoadErrorHubInfo info = param.toThrift();
            if (info != null) {
                params.setLoadErrorHubInfo(info);
            }
        }

        // LOG.debug("stream load txn id: {}, plan: {}", streamLoadTask.getTxnId(), params);
        return params;
    }

    // get all specified partition ids.
    // if no partition specified, return all partitions
    private List<Long> getAllPartitionIds() throws DdlException {
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
        } else if (windowIntervalSec > 0) {
            if (destTable.getPartitionInfo().getType().equals(PartitionType.RANGE)) {
                RangePartitionInfo partitionInfo = (RangePartitionInfo) destTable.getPartitionInfo();
                if (partitionInfo.getPartitionColumns().size() != 1) {
                    for (Partition partition : destTable.getPartitions()) {
                        partitionIds.add(partition.getId());
                    }
                } else {
                    Date now = new Date();
                    Date upperDate = new Date(now.getTime() + 60*1000*2);
                    Date lowerDate = new Date(upperDate.getTime() - windowIntervalSec*1000);
                    PartitionValue lowerValue = new PartitionValue(DATE_FORMAT.format(lowerDate));
                    PartitionValue upperValue = new PartitionValue(DATE_FORMAT.format(upperDate));
                    Range<PartitionKey> partitionKeyRange;
                    try {
                        PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue), partitionInfo.getPartitionColumns());
                        PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperValue), partitionInfo.getPartitionColumns());
                        partitionKeyRange = Range.closedOpen(lowerBound, upperBound);
                        for (Map.Entry<Long, Range<PartitionKey>> entry : partitionInfo.getIdToRange(false).entrySet()) {
                            Range<PartitionKey> range = entry.getValue();
                            try {
                                if (!range.intersection(partitionKeyRange).isEmpty()) {
                                    partitionIds.add(entry.getKey());
                                }
                            } catch (Exception e) {
                                // do nothing
                            }
                        }
                    } catch (AnalysisException e) {
                        // keys.size is always equal to column.size, cannot reach this exception
                        LOG.warn("Keys size is not equal to column size. Error={}, db: {}, table: {}", e.getMessage(),
                                db.getFullName(), destTable.getName());
                    }
                }
            } else {
                for (Partition partition : destTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
            }
        } else {
            for (Partition partition : destTable.getPartitions()) {
                partitionIds.add(partition.getId());
            }
            if (partitionIds.isEmpty()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_EMPTY_PARTITION_IN_TABLE, destTable.getName());
            }
        }

        return partitionIds;
    }
}
