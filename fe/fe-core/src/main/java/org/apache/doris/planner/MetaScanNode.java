//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

package org.apache.doris.planner;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.*;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class MetaScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(MetaScanNode.class);

    private static final String NODE_NAME = "META_SCAN";

    private List<SlotId> slotIdList;

    private OlapTable olapTable;

    public MetaScanNode(PlanNodeId id,
                        TupleDescriptor tupleDesc,
                        List<SlotId> slotIdList,
                        OlapTable table) {
        super(id, tupleDesc, NODE_NAME);
        this.slotIdList = slotIdList;
        this.olapTable = table;
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

    @Override
    protected void toThrift(TPlanNode msg) {

    }
}
