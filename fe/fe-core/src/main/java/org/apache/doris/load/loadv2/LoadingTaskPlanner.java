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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.external.LoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LoadingTaskPlanner extends LoadPlanner {
    private static final Logger LOG = LogManager.getLogger(LoadingTaskPlanner.class);

    // Input params
    private final long loadJobId;
    private final long txnId;
    private final long dbId;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final boolean strictMode;
    private final long timeoutS;    // timeout of load job, in second
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean useNewLoadScanNode;
    private UserIdentity userInfo;
    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    public LoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
                              BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups,
                              boolean strictMode, String timezone, long timeoutS, int loadParallelism,
                              int sendBatchParallelism, boolean useNewLoadScanNode, UserIdentity userInfo) {
        this.analyzer = new Analyzer(Env.getCurrentEnv(), new ConnectContext());
        this.descTable = analyzer.getDescTbl();
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
        if (Env.getCurrentEnv().getAccessManager()
            .checkDbPriv(userInfo, Env.getCurrentInternalCatalog().getDbNullable(dbId).getFullName(),
                PrivPredicate.SELECT)) {
            this.analyzer.setUDFAllowed(true);
        } else {
            this.analyzer.setUDFAllowed(false);
        }
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId) throws UserException {
        // Generate tuple descriptor
        TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
        TupleDescriptor scanTupleDesc = descTable.createTupleDescriptor("ScanTuple");
        // Using full schema to fill the descriptor table
        for (Column col : table.getFullSchema()) {
            SlotDescriptor scanSlotDesc = slotDescriptorBuilder(descTable, destTupleDesc, scanTupleDesc, col);
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

        if (table.isDynamicSchema()) {
            // Dynamic table for s3load ...
            descTable.addReferencedTable(table);
            // For reference table
            scanTupleDesc.setTableId((int) table.getId());
            addAndSetSlotDescriptor(descTable, scanTupleDesc);
        }

        // Generate plan trees
        // 1. Broker scan node
        nextNodeId++;
        ScanNode scanNode = scanNodeBuilder(nextNodeId, scanTupleDesc, loadJobId, txnId, brokerDesc, fileGroups, strictMode, loadParallelism, userInfo);
        descTable.computeStatAndMemLayout();

        // 2. Olap table sink
        OlapTableSink olapTableSink = olapTableSinkBuilder(destTupleDesc,loadId,txnId,dbId,timeoutS,sendBatchParallelism);

        // 3. Plan fragment
        fragments.add(planFragmentBuilder(scanNode, loadParallelism, olapTableSink));

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            fragment.finalize(null);
        }
        Collections.reverse(fragments);
        return null;
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

    protected List<Long> getAllPartitionIds() throws LoadException, MetaNotFoundException {
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
