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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.MetadataTableValuedFunction;
import org.apache.doris.thrift.TMetaScanNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.collect.Lists;

import java.util.List;

public class MetadataScanNode extends ExternalScanNode {

    private final MetadataTableValuedFunction tvf;

    private final List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();

    public MetadataScanNode(PlanNodeId id, TupleDescriptor desc, MetadataTableValuedFunction tvf) {
        super(id, desc, "METADATA_SCAN_NODE", StatisticalType.METADATA_SCAN_NODE, false);
        this.tvf = tvf;
    }

    // for Nereids
    @Override
    public void init() throws UserException {
        super.init();
        createScanRangeLocations();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.META_SCAN_NODE);
        TMetaScanNode metaScanNode = new TMetaScanNode();
        metaScanNode.setTupleId(desc.getId().asInt());
        metaScanNode.setMetadataType(this.tvf.getMetadataType());
        TUserIdentity tCurrentUser = ConnectContext.get().getCurrentUserIdentity().toThrift();
        metaScanNode.setCurrentUserIdent(tCurrentUser);
        planNode.setMetaScanNode(metaScanNode);
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        TScanRange scanRange = new TScanRange();
        scanRange.setMetaScanRange(tvf.getMetaScanRange());
        // set location
        TScanRangeLocation location = new TScanRangeLocation();
        Backend backend = backendPolicy.getNextBe();
        location.setBackendId(backend.getId());
        location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));

        TScanRangeLocations locations = new TScanRangeLocations();
        locations.addToLocations(location);
        locations.setScanRange(scanRange);

        scanRangeLocations.add(locations);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        createScanRangeLocations();
    }

    @Override
    public boolean needToCheckColumnPriv() {
        return false;
    }
}
