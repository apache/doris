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
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;

public abstract class LoadPlanner {

    private static final Logger LOG = LogManager.getLogger(LoadPlanner.class);

    public List<List<TBrokerFileStatus>> fileStatusesList;

    public int filesAdded;

    protected LoadTaskInfo taskInfo;

    protected OlapTable table;

    // Something useful
    // ConnectContext here is just a dummy object to avoid some NPE problem, like ctx.getDatabase()
    protected Analyzer analyzer;

    protected DescriptorTable descTable;

    public void preparePlan(List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
    }

    public abstract TExecPlanFragmentParams plan(TUniqueId loadId) throws UserException;

    public SlotDescriptor slotDescriptorBuilder(DescriptorTable descTable,
            TupleDescriptor destTupleDesc,
            TupleDescriptor scanTupleDesc,
            Column col) {
        SlotDescriptor slotDesc = descTable.addSlotDescriptor(destTupleDesc);
        slotDesc.setIsMaterialized(true);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(col.isAllowNull());
        SlotDescriptor scanSlotDesc = descTable.addSlotDescriptor(scanTupleDesc);
        scanSlotDesc.setIsMaterialized(true);
        scanSlotDesc.setColumn(col);
        scanSlotDesc.setIsNullable(col.isAllowNull());
        return scanSlotDesc;
    }

    public void addAndSetSlotDescriptor(DescriptorTable descTable, TupleDescriptor scanTupleDesc) {
        // Add an implicit container column "DORIS_DYNAMIC_COL" for dynamic columns
        SlotDescriptor slotDesc = descTable.addSlotDescriptor(scanTupleDesc);
        Column col = new Column(Column.DYNAMIC_COLUMN_NAME, Type.VARIANT, false, null, false, "",
                "stream load auto dynamic column");
        slotDesc.setIsMaterialized(true);
        // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
        slotDesc.setNullIndicatorBit(-1);
        slotDesc.setNullIndicatorByte(0);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(false);
        LOG.debug("plan scanTupleDesc{}", scanTupleDesc.toString());
    }

    public ScanNode scanNodeBuilder(TupleDescriptor scanTupleDesc, Database db, TUniqueId loadId) throws UserException {
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), scanTupleDesc);
        // 1. create file group
        DataDescription dataDescription = new DataDescription(table.getName(), taskInfo);
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
        fileScanNode.setLoadInfo(loadId, taskInfo.getTxnId(), table, BrokerDesc.createForStreamLoad(),
                fileGroup, fileStatus, taskInfo.isStrictMode(), taskInfo.getFileType(), taskInfo.getHiddenColumns(),
                taskInfo.isPartialUpdate());
        ScanNode scanNode = fileScanNode;
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.convertToVectorized();
        return scanNode;
    }

    public ScanNode scanNodeBuilder(int nextNodeId,
            TupleDescriptor scanTupleDesc,
            long loadJobId,
            long txnId,
            BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups,
            boolean strictMode,
            int loadParallelism,
            UserIdentity userInfo) throws UserException {
        ScanNode scanNode;
        scanNode = new FileLoadScanNode(new PlanNodeId(nextNodeId), scanTupleDesc);
        if (fileStatusesList == null) {
            LOG.warn("fileStatusesList should be set.");
        }
        ((FileLoadScanNode) scanNode).setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups,
                fileStatusesList, filesAdded, strictMode, loadParallelism, userInfo);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.convertToVectorized();
        return scanNode;
    }

    public OlapTableSink olapTableSinkBuilder(TupleDescriptor destTupleDesc,
            TUniqueId loadId,
            long txnId,
            long dbId,
            long timeoutS,
            int sendBatchParallelism) throws UserException {
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, destTupleDesc, partitionIds,
                Config.enable_single_replica_load);
        olapTableSink.init(loadId, txnId, dbId, timeoutS, sendBatchParallelism, false);
        olapTableSink.complete();
        return olapTableSink;
    }

    public OlapTableSink olapTableSinkBuilder(TupleDescriptor tupleDesc,
            TUniqueId loadId,
            Database db,
            long timeout,
            boolean isPartialUpdate,
            HashSet<String> partialUpdateInputColumns) throws UserException {
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, tupleDesc, partitionIds,
                Config.enable_single_replica_load);
        olapTableSink.init(loadId, taskInfo.getTxnId(), db.getId(), timeout,
                taskInfo.getSendBatchParallelism(), taskInfo.isLoadToSingleTablet());
        olapTableSink.setPartialUpdateInputColumns(isPartialUpdate, partialUpdateInputColumns);
        olapTableSink.complete();
        return olapTableSink;
    }

    public PlanFragment planFragmentBuilder(ScanNode scanNode,
            int loadParallelism,
            OlapTableSink olapTableSink) {
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        sinkFragment.setParallelExecNum(loadParallelism);
        sinkFragment.setSink(olapTableSink);
        return sinkFragment;
    }

    public PlanFragment planFragmentBuilder(ScanNode scanNode, OlapTableSink olapTableSink) {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);
        fragment.finalize(null);
        return fragment;
    }

    protected abstract List<Long> getAllPartitionIds() throws UserException;
}
