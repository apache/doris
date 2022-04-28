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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TMetaScanNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;


public class OlapMetaScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(OlapMetaScanNode.class);

    private static final String NODE_NAME = "META_SCAN";

    private final Map<Integer, Integer> slotIdToDictId = new HashMap<>();

    private final OlapTable olapTable;

    public OlapMetaScanNode(PlanNodeId id,
                            TupleDescriptor tupleDesc) {
        super(id, tupleDesc, NODE_NAME);
        this.olapTable = (OlapTable) tupleDesc.getTable();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        List<TScanRangeLocations> resultList = Lists.newArrayList();
        Collection<Partition> partitionCollection = olapTable.getPartitions();
        for (Partition partition : partitionCollection) {
            long visibleVersion = partition.getVisibleVersion();
            String visibleVersionStr = String.valueOf(visibleVersion);
            MaterializedIndex index = partition.getBaseIndex();
            List<Tablet> tablets = index.getTablets();
            for (Tablet tablet : tablets) {
                long tabletId = tablet.getId();
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                TPaloScanRange paloRange = new TPaloScanRange();
                paloRange.setDbName("");
                paloRange.setSchemaHash("");
                paloRange.setVersion(visibleVersionStr);
                paloRange.setVersionHash("");
                paloRange.setTabletId(tabletId);

                // random shuffle List && only collect one copy
                List<Replica> replicas = tablet.getQueryableReplicas(visibleVersion);
                if (replicas.isEmpty()) {
                    LOG.error("no queryable replica found in tablet {}. visible version {}",
                        tabletId, visibleVersion);
                    if (LOG.isDebugEnabled()) {
                        for (Replica replica : tablet.getReplicas()) {
                            LOG.debug("tablet {}, replica: {}", tabletId, replica.toString());
                        }
                    }
                    throw new RuntimeException("Failed to get scan range, no queryable replica found in tablet: " + tabletId);
                }

                Collections.shuffle(replicas);
                boolean tabletIsNull = true;
                List<String> errs = Lists.newArrayList();
                for (Replica replica : replicas) {
                    Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
                    if (backend == null || !backend.isAlive()) {
                        LOG.debug("backend {} not exists or is not alive for replica {}",
                            replica.getBackendId(), replica.getId());
                        errs.add(replica.getId() + "'s backend " + replica.getBackendId() + " does not exist or not alive");
                        continue;
                    }
                    String ip = backend.getHost();
                    int port = backend.getBePort();
                    TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                    scanRangeLocation.setBackendId(replica.getBackendId());
                    scanRangeLocations.addToLocations(scanRangeLocation);
                    paloRange.addToHosts(new TNetworkAddress(ip, port));
                    tabletIsNull = false;
                }
                if (tabletIsNull) {
                    throw new RuntimeException(tabletId + " have no queryable replicas. err: " + Joiner.on(", ").join(errs));
                }
                TScanRange scanRange = new TScanRange();
                scanRange.setPaloScanRange(paloRange);
                scanRangeLocations.setScanRange(scanRange);

                resultList.add(scanRangeLocations);
            }
        }
        return resultList;
    }

    public void pullDictSlots(AggregateInfo aggInfo) {
        Preconditions.checkState(aggInfo.getGroupingExprs().isEmpty());
        List<FunctionCallExpr> funcExprList = aggInfo.getAggregateExprs();
        for (FunctionCallExpr funcExpr : funcExprList) {
            FunctionParams funcParams = funcExpr.getFnParams();
            if (funcParams == null) {
                continue;
            }
            for (Expr expr : funcParams.exprs()) {
                if (expr instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) expr;
                    checkSlot(slotRef);
                    // We will set the value to dict id when we support incremental dict update.
                    // For now, set -1 as a placeholder only which indicates BE that there doesn't exist
                    // a dict right now
                    slotIdToDictId.put(slotRef.getSlotId().asInt(), -1);
                }
            }
        }
    }

    private void checkSlot(SlotRef slotRef) {
        Table slotRefTable = slotRef.getTable();
        Preconditions.checkState(slotRefTable.getId() == olapTable.getId());
        SlotDescriptor slotDesc = slotRef.getDesc();
        Preconditions.checkState(getTupleDesc().getSlots().contains(slotDesc));
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.META_SCAN_NODE;
        TupleId tupleId = getTupleDesc().getId();
        msg.meta_scan_node = new TMetaScanNode();
        msg.meta_scan_node.tuple_id = tupleId.asInt();
        msg.meta_scan_node.slot_to_dict = this.slotIdToDictId;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");

        output.append(prefix).append("slotIdToDictId: ");
        StringJoiner reqDictInfo = new StringJoiner(",", "", "");
        for (Map.Entry<Integer, Integer> entry : slotIdToDictId.entrySet()) {
            reqDictInfo.add(String.format("<%d, %d> ", entry.getKey(), entry.getValue()));
        }
        output.append(reqDictInfo.toString());
        output.append("\n");
        return output.toString();
    }

}
