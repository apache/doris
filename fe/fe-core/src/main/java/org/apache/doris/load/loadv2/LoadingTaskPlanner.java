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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LoadingTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(LoadingTaskPlanner.class);

    // Input params
    private final long loadJobId;
    private final long txnId;
    private final long dbId;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final boolean strictMode;
    private final boolean isPartialUpdate;
    private final long timeoutS;    // timeout of load job, in second
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean useNewLoadScanNode;
    private final boolean singleTabletLoadPerSink;
    private final boolean enableMemtableOnSinkNode;
    private UserIdentity userInfo;
    // Something useful
    // ConnectContext here is just a dummy object to avoid some NPE problem, like ctx.getDatabase()
    private Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), new ConnectContext());
    private DescriptorTable descTable = analyzer.getDescTbl();

    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    public LoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups,
            boolean strictMode, boolean isPartialUpdate, String timezone, long timeoutS, int loadParallelism,
            int sendBatchParallelism, boolean useNewLoadScanNode, UserIdentity userInfo,
            boolean singleTabletLoadPerSink, boolean enableMemtableOnSinkNode) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.analyzer.setTimezone(timezone);
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.useNewLoadScanNode = useNewLoadScanNode;
        this.userInfo = userInfo;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.enableMemtableOnSinkNode = enableMemtableOnSinkNode;
        if (Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(userInfo, InternalCatalog.INTERNAL_CATALOG_NAME,
                        Env.getCurrentInternalCatalog().getDbNullable(dbId).getFullName(),
                        PrivPredicate.SELECT)) {
            this.analyzer.setUDFAllowed(true);
        } else {
            this.analyzer.setUDFAllowed(false);
        }
    }

    public void plan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        // Generate tuple descriptor
        TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
        TupleDescriptor scanTupleDesc = destTupleDesc;
        scanTupleDesc = descTable.createTupleDescriptor("ScanTuple");
        if (isPartialUpdate && !table.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Only unique key merge on write support partial update");
        }

        HashSet<String> partialUpdateInputColumns = new HashSet<>();
        if (isPartialUpdate) {
            for (Column col : table.getFullSchema()) {
                boolean existInExpr = false;
                for (ImportColumnDesc importColumnDesc : fileGroups.get(0).getColumnExprList()) {
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

        // use full schema to fill the descriptor table
        for (Column col : table.getFullSchema()) {
            if (isPartialUpdate && !partialUpdateInputColumns.contains(col.getName())) {
                continue;
            }
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(destTupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            slotDesc.setAutoInc(col.isAutoInc());
            SlotDescriptor scanSlotDesc = descTable.addSlotDescriptor(scanTupleDesc);
            scanSlotDesc.setIsMaterialized(true);
            scanSlotDesc.setColumn(col);
            scanSlotDesc.setIsNullable(col.isAllowNull());
            scanSlotDesc.setAutoInc(col.isAutoInc());
            if (col.isAutoInc()) {
                // auto-increment column should be non-nullable
                // however, here we use `NullLiteral` to indicate that a cell should
                // be filled with generated value in `VOlapTableSink::_fill_auto_inc_cols()`
                scanSlotDesc.setIsNullable(true);
            }
            if (fileGroups.size() > 0) {
                for (ImportColumnDesc importColumnDesc : fileGroups.get(0).getColumnExprList()) {
                    try {
                        if (!importColumnDesc.isColumn() && importColumnDesc.getColumnName() != null
                                && importColumnDesc.getColumnName().equals(col.getName())) {
                            scanSlotDesc.setIsNullable(importColumnDesc.getExpr().isNullable());
                            break;
                        }
                    } catch (Exception e) {
                        // An exception may be thrown here because the `importColumnDesc.getExpr()` is not analyzed
                        // now. We just skip this case here.
                    }
                }
            }
        }

        // analyze expr in whereExpr before rewrite
        scanTupleDesc.setTable(table);
        analyzer.registerTupleDescriptor(scanTupleDesc);
        for (BrokerFileGroup fileGroup : fileGroups) {
            if (fileGroup.getWhereExpr() != null) {
                fileGroup.getWhereExpr().analyze(analyzer);
            }
        }

        // Generate plan trees
        // 1. Broker scan node
        ScanNode scanNode;
        scanNode = new FileLoadScanNode(new PlanNodeId(nextNodeId++), scanTupleDesc);
        ((FileLoadScanNode) scanNode).setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups,
                fileStatusesList, filesAdded, strictMode, loadParallelism, userInfo);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNodes.add(scanNode);
        descTable.computeStatAndMemLayout();

        // 2. Olap table sink
        List<Long> partitionIds = getAllPartitionIds();
        final boolean enableSingleReplicaLoad = this.enableMemtableOnSinkNode
                ? false : Config.enable_single_replica_load;
        OlapTableSink olapTableSink = new OlapTableSink(table, destTupleDesc, partitionIds,
                enableSingleReplicaLoad);
        long txnTimeout = timeoutS == 0 ? ConnectContext.get().getExecTimeout() : timeoutS;
        olapTableSink.init(loadId, txnId, dbId, timeoutS, sendBatchParallelism, singleTabletLoadPerSink, strictMode,
                txnTimeout);
        olapTableSink.setPartialUpdateInputColumns(isPartialUpdate, partialUpdateInputColumns);

        olapTableSink.complete(analyzer);

        // 3. Plan fragment
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        sinkFragment.setParallelExecNum(loadParallelism);
        sinkFragment.setSink(olapTableSink);

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
        return analyzer.getTimezone();
    }

    private List<Long> getAllPartitionIds() throws LoadException, MetaNotFoundException {
        Set<Long> specifiedPartitionIds = Sets.newHashSet();
        for (BrokerFileGroup brokerFileGroup : fileGroups) {
            if (brokerFileGroup.getPartitionIds() != null) {
                for (long partitionId : brokerFileGroup.getPartitionIds()) {
                    if (!table.getPartitionInfo().getIsMutable(partitionId)) {
                        throw new LoadException("Can't load data to immutable partition, table: "
                            + table.getName() + ", partition: " + table.getPartition(partitionId));
                    }
                }
                specifiedPartitionIds.addAll(brokerFileGroup.getPartitionIds());
            }
            // all file group in fileGroups should have same partitions, so only need to get partition ids
            // from one of these file groups
            break;
        }
        if (specifiedPartitionIds.isEmpty()) {
            return null;
        }
        return Lists.newArrayList(specifiedPartitionIds);
    }

    // when retry load by reusing this plan in load process, the load_id should be changed
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
