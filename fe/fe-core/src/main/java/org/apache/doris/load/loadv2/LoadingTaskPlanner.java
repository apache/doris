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
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.BrokerScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
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
    private final long timeoutS;    // timeout of load job, in second
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean useNewLoadScanNode;
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
            boolean strictMode, String timezone, long timeoutS, int loadParallelism,
            int sendBatchParallelism, boolean useNewLoadScanNode, UserIdentity userInfo) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
        this.analyzer.setTimezone(timezone);
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.useNewLoadScanNode = useNewLoadScanNode;
        this.userInfo = userInfo;
        if (Env.getCurrentEnv().getAuth()
                .checkDbPriv(userInfo, Env.getCurrentInternalCatalog().getDbNullable(dbId).getFullName(),
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
        if (Config.enable_vectorized_load) {
            scanTupleDesc = descTable.createTupleDescriptor("ScanTuple");
        }
        // use full schema to fill the descriptor table
        for (Column col : table.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(destTupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            if (Config.enable_vectorized_load) {
                SlotDescriptor scanSlotDesc = descTable.addSlotDescriptor(scanTupleDesc);
                scanSlotDesc.setIsMaterialized(true);
                scanSlotDesc.setColumn(col);
                scanSlotDesc.setIsNullable(col.isAllowNull());
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
        }

        // Generate plan trees
        // 1. Broker scan node
        ScanNode scanNode;
        boolean useNewScanNode = Config.enable_new_load_scan_node || useNewLoadScanNode;
        if (useNewScanNode) {
            scanNode = new ExternalFileScanNode(new PlanNodeId(nextNodeId++), scanTupleDesc);
            ((ExternalFileScanNode) scanNode).setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups,
                    fileStatusesList, filesAdded, strictMode, loadParallelism, userInfo);
        } else {
            scanNode = new BrokerScanNode(new PlanNodeId(nextNodeId++), scanTupleDesc, "BrokerScanNode",
                    fileStatusesList, filesAdded);
            ((BrokerScanNode) scanNode).setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups, strictMode,
                    loadParallelism, userInfo);
        }
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        if (Config.enable_vectorized_load) {
            scanNode.convertToVectorized();
        }
        scanNodes.add(scanNode);
        descTable.computeStatAndMemLayout();

        // 2. Olap table sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, destTupleDesc, partitionIds,
                Config.enable_single_replica_load);
        olapTableSink.init(loadId, txnId, dbId, timeoutS, sendBatchParallelism, false);
        olapTableSink.complete();

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
