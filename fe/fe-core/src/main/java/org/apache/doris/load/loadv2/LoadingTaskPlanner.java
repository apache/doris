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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.NotImplementedException;
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
    private UserIdentity userInfo;
    // Something useful
    // ConnectContext here is just a dummy object to avoid some NPE problem, like ctx.getDatabase()
    private Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), new ConnectContext());
    private DescriptorTable descTable = analyzer.getDescTbl();

    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    public LoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
                              BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups,
                              boolean strictMode, String timezone, long timeoutS, int loadParallelism,
                              UserIdentity userInfo) {
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
        this.userInfo = userInfo;
        if (Catalog.getCurrentCatalog().getAuth().checkDbPriv(userInfo,
                Catalog.getCurrentCatalog().getDb(dbId).getFullName(), PrivPredicate.SELECT)) {
            this.analyzer.setUDFAllowed(true);
        } else {
            this.analyzer.setUDFAllowed(false);
        }
    }

    public void plan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        // Generate tuple descriptor
        TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
        // use full schema to fill the descriptor table
        for (Column col : table.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(destTupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            if (col.isAllowNull()) {
                slotDesc.setIsNullable(true);
            } else {
                slotDesc.setIsNullable(false);
            }
        }

        // Generate plan trees
        // 1. Broker scan node
        BrokerScanNode scanNode = new BrokerScanNode(new PlanNodeId(nextNodeId++), destTupleDesc, "BrokerScanNode",
                fileStatusesList, filesAdded);
        scanNode.setLoadInfo(loadJobId, txnId, table, brokerDesc, fileGroups, strictMode, loadParallelism);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNodes.add(scanNode);
        descTable.computeMemLayout();

        // 2. Olap table sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink = new OlapTableSink(table, destTupleDesc, partitionIds);
        olapTableSink.init(loadId, txnId, dbId, timeoutS);
        olapTableSink.complete();

        // 3. Plan fragment
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        sinkFragment.setParallelExecNum(loadParallelism);
        sinkFragment.setSink(olapTableSink);

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            try {
                fragment.finalize(analyzer, false);
            } catch (NotImplementedException e) {
                LOG.info("Fragment finalize failed.{}", e.getMessage());
                throw new UserException("Fragment finalize failed.");
            }
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
        Set<Long> partitionIds = Sets.newHashSet();
        for (BrokerFileGroup brokerFileGroup : fileGroups) {
            if (brokerFileGroup.getPartitionIds() != null) {
                partitionIds.addAll(brokerFileGroup.getPartitionIds());
            }
            // all file group in fileGroups should have same partitions, so only need to get partition ids
            // from one of these file groups
            break;
        }

        if (partitionIds.isEmpty()) {
            for (Partition partition : table.getPartitions()) {
                partitionIds.add(partition.getId());
            }
        }

        // If this is a dynamic partitioned table, it will take some time to create the partition after the
        // table is created, a exception needs to be thrown here
        if (partitionIds.isEmpty()) {
            throw new LoadException("data cannot be inserted into table with empty partition. " +
                    "Use `SHOW PARTITIONS FROM " + table.getName() + "` to see the currently partitions of this table. ");
        }

        return Lists.newArrayList(partitionIds);
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
