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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * NereidsLoadingTaskPlanner
 */
public class NereidsLoadingTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(NereidsLoadingTaskPlanner.class);

    // Input params
    private final long loadJobId;
    private final long txnId;
    private final long dbId;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<NereidsBrokerFileGroup> fileGroups;
    private final boolean strictMode;
    private final boolean isPartialUpdate;
    private final TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy;
    private final String timezone;
    private final long timeoutS; // timeout of load job, in second
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean singleTabletLoadPerSink;
    private final boolean enableMemtableOnSinkNode;
    private UserIdentity userInfo;
    private DescriptorTable descTable;

    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    /**
     * NereidsLoadingTaskPlanner
     */
    public NereidsLoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
            BrokerDesc brokerDesc, List<NereidsBrokerFileGroup> brokerFileGroups,
            boolean strictMode, boolean isPartialUpdate, TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            String timezone, long timeoutS, int loadParallelism,
            int sendBatchParallelism, UserIdentity userInfo,
            boolean singleTabletLoadPerSink, boolean enableMemtableOnSinkNode) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateNewKeyPolicy = partialUpdateNewKeyPolicy;
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.userInfo = userInfo;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.enableMemtableOnSinkNode = enableMemtableOnSinkNode;
    }

    /**
     * create a plan for broker loading task
     */
    public void plan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        if (isPartialUpdate && !table.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Only unique key merge on write support partial update");
        }
        if (isPartialUpdate && table.isUniqKeyMergeOnWriteWithClusterKeys()) {
            throw new UserException("Only unique key merge on write without cluster keys support partial update");
        }

        HashSet<String> partialUpdateInputColumns = new HashSet<>();
        if (isPartialUpdate) {
            for (Column col : table.getFullSchema()) {
                boolean existInExpr = false;
                for (NereidsImportColumnDesc importColumnDesc : fileGroups.get(0).getColumnExprList()) {
                    if (importColumnDesc.getColumnName() != null
                            && importColumnDesc.getColumnName().equals(col.getName())) {
                        if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                            throw new UserException("Partial update should not include invisible column except"
                                    + " delete sign column: " + col.getName());
                        }
                        partialUpdateInputColumns.add(col.getName());
                        existInExpr = true;
                        break;
                    }
                }
                if (col.isKey() && !existInExpr) {
                    throw new UserException("Partial update should include all key columns, missing: " + col.getName());
                }
            }
        }

        Preconditions.checkState(!fileGroups.isEmpty() && fileGroups.size() == fileStatusesList.size());
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(loadJobId, txnId, table, brokerDesc,
                fileGroups.get(0), fileStatusesList.get(0), filesAdded, strictMode, loadParallelism);
        NereidsLoadScanProvider loadScanProvider = new NereidsLoadScanProvider(fileGroupInfo,
                partialUpdateInputColumns);
        NereidsParamCreateContext context = loadScanProvider.createLoadContext();
        PartitionNames partitionNames = getPartitionNames();
        LogicalPlan streamLoadPlan = NereidsLoadUtils.createLoadPlan(fileGroupInfo, partitionNames, context,
                isPartialUpdate, partialUpdateNewKeyPolicy);
        long txnTimeout = timeoutS == 0 ? ConnectContext.get().getExecTimeoutS() : timeoutS;
        if (txnTimeout > Integer.MAX_VALUE) {
            txnTimeout = Integer.MAX_VALUE;
        }
        NereidsBrokerLoadTask nereidsBrokerLoadTask = new NereidsBrokerLoadTask(txnId, (int) txnTimeout,
                sendBatchParallelism,
                strictMode, enableMemtableOnSinkNode, partitionNames);
        NereidsLoadPlanInfoCollector planInfoCollector = new NereidsLoadPlanInfoCollector(table, nereidsBrokerLoadTask,
                loadId, dbId, isPartialUpdate ? TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS : TUniqueKeyUpdateMode.UPSERT,
                partialUpdateNewKeyPolicy, partialUpdateInputColumns, context.exprMap);
        NereidsLoadPlanInfoCollector.LoadPlanInfo loadPlanInfo = planInfoCollector.collectLoadPlanInfo(streamLoadPlan);
        descTable = loadPlanInfo.getDescriptorTable();
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), loadPlanInfo.getDestTuple());
        List<NereidsFileGroupInfo> fileGroupInfos = new ArrayList<>(fileGroups.size());
        List<NereidsParamCreateContext> contexts = new ArrayList<>(fileGroups.size());
        fileGroupInfos.add(fileGroupInfo);
        contexts.add(context);
        for (int i = 1; i < fileGroups.size(); ++i) {
            fileGroupInfos.add(new NereidsFileGroupInfo(loadJobId, txnId, table, brokerDesc,
                    fileGroups.get(i), fileStatusesList.get(i), filesAdded, strictMode, loadParallelism));
            NereidsParamCreateContext paramCreateContext = new NereidsParamCreateContext();
            paramCreateContext.fileGroup = fileGroups.get(i);
            contexts.add(paramCreateContext);
        }
        fileScanNode.finalizeForNereids(loadId, fileGroupInfos, contexts, loadPlanInfo);
        scanNodes.add(fileScanNode);

        // 3. Plan fragment
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), fileScanNode, DataPartition.RANDOM);
        sinkFragment.setParallelExecNum(loadParallelism);
        sinkFragment.setSink(loadPlanInfo.getOlapTableSink());

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            fragment.finalize(null);
        }
        Collections.reverse(fragments);
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public String getTimezone() {
        return timezone;
    }

    private PartitionNames getPartitionNames() throws LoadException {
        PartitionNames partitionNames = null;
        List<String> partitions = Lists.newArrayList();
        boolean isTemp = false;
        for (NereidsBrokerFileGroup brokerFileGroup : fileGroups) {
            if (brokerFileGroup.getPartitionIds() != null) {
                for (long partitionId : brokerFileGroup.getPartitionIds()) {
                    if (!table.getPartitionInfo().getIsMutable(partitionId)) {
                        throw new LoadException("Can't load data to immutable partition, table: "
                                + table.getName() + ", partition: " + table.getPartition(partitionId));
                    }
                    if (table.isTemporaryPartition(partitionId)) {
                        isTemp = true;
                    }
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        throw new LoadException(String.format("partition id %d not found", partitionId));
                    }
                    partitions.add(partition.getName());
                }
            }
            // all file group in fileGroups should have same partitions, so only need to get partition ids
            // from one of these file groups
            break;
        }
        if (!partitions.isEmpty()) {
            partitionNames = new PartitionNames(isTemp, partitions);
        }
        return partitionNames;

    }

    /**
     * when retry load by reusing this plan in load process, the load_id should be changed
     */
    public void updateLoadId(TUniqueId loadId) {
        for (PlanFragment planFragment : fragments) {
            if (!(planFragment.getSink() instanceof OlapTableSink)) {
                continue;
            }
            OlapTableSink olapTableSink = (OlapTableSink) planFragment.getSink();
            olapTableSink.updateLoadId(loadId);
        }

        LOG.info("update olap table sink's load id to {}, job: {}", DebugUtil.printId(loadId), loadJobId);
    }
}
