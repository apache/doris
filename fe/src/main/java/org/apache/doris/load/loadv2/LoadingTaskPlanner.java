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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.BrokerScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LoadingTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(LoadingTaskPlanner.class);

    // Input params
    private final long txnId;
    private final long dbId;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final boolean strictMode;

    // Something useful
    private Analyzer analyzer = new Analyzer(Catalog.getInstance(), null);
    private DescriptorTable descTable = analyzer.getDescTbl();

    // Output params
    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    private int nextNodeId = 0;

    public LoadingTaskPlanner(long txnId, long dbId, OlapTable table,
                              BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups,
                              boolean strictMode) {
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
    }

    public void plan(List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) throws UserException {
        // Generate tuple descriptor
        List<Expr> slotRefs = Lists.newArrayList();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
        for (Column col : table.getBaseSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            if (col.isAllowNull()) {
                slotDesc.setIsNullable(true);
            } else {
                slotDesc.setIsNullable(false);
            }
            slotRefs.add(new SlotRef(slotDesc));
        }

        // Generate plan trees
        // 1. Broker scan node
        BrokerScanNode scanNode = new BrokerScanNode(new PlanNodeId(nextNodeId++), tupleDesc, "BrokerScanNode",
                                                     fileStatusesList, filesAdded);
        scanNode.setLoadInfo(table, brokerDesc, fileGroups, strictMode);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNodes.add(scanNode);
        descTable.computeMemLayout();

        // 2. Olap table sink
        String partitionNames = convertBrokerDescPartitionInfo();
        OlapTableSink olapTableSink = new OlapTableSink(table, tupleDesc, partitionNames);
        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        olapTableSink.init(loadId, txnId, dbId);
        olapTableSink.finalize();

        // 3. Plan fragment
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        sinkFragment.setSink(olapTableSink);

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            try {
                fragment.finalize(analyzer, false);
            } catch (NotImplementedException e) {
                LOG.info("Fragment finalize failed.{}", e);
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

    private String convertBrokerDescPartitionInfo() throws LoadException, MetaNotFoundException {
        String result = "";
        for (BrokerFileGroup brokerFileGroup : fileGroups) {
            List<String> partitionNames = getPartitionNames(brokerFileGroup);
            if (partitionNames == null) {
                continue;
            }
            result += Joiner.on(",").join(partitionNames);
            result += ",";
        }
        if (Strings.isNullOrEmpty(result)) {
            return null;
        }
        result = result.substring(0, result.length() - 1);
        return result;
    }

    private List<String> getPartitionNames(BrokerFileGroup brokerFileGroup)
            throws MetaNotFoundException, LoadException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + " has been deleted when broker loading");
        }
        Table table = database.getTable(brokerFileGroup.getTableId());
        if (table == null) {
            throw new MetaNotFoundException("Table " + brokerFileGroup.getTableId()
                                                    + " has been deleted when broker loading");
        }
        if (!(table instanceof OlapTable)) {
            throw new LoadException("Only olap table is supported in broker load");
        }
        OlapTable olapTable = (OlapTable) table;
        List<Long> partitionIds = brokerFileGroup.getPartitionIds();
        if (partitionIds == null || partitionIds.isEmpty()) {
            return null;
        }
        List<String> result = Lists.newArrayList();
        for (long partitionId : brokerFileGroup.getPartitionIds()) {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("Unknown partition(" + partitionId + ") in table("
                                                        + table.getName() + ")");
            }
            result.add(partition.getName());
        }
        return result;
    }
}
