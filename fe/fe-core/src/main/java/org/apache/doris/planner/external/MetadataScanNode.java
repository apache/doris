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
import org.apache.doris.planner.ScanNode;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.IcebergTableValuedFunction;
import org.apache.doris.tablefunction.MetadataTableValuedFunction;
import org.apache.doris.thrift.TIcebergMetadataParams;
import org.apache.doris.thrift.TIcebergMetadataType;
import org.apache.doris.thrift.TMetaScanNode;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;

import java.util.List;

public class MetadataScanNode extends ScanNode {

    private MetadataTableValuedFunction tvf;

    private List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();

    private final FederationBackendPolicy backendPolicy = new FederationBackendPolicy();

    public MetadataScanNode(PlanNodeId id, TupleDescriptor desc, MetadataTableValuedFunction tvf) {
        super(id, desc, "METADATA_SCAN_NODE", StatisticalType.METADATA_SCAN_NODE);
        this.tvf = tvf;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        backendPolicy.init();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.META_SCAN_NODE);
        TMetaScanNode metaScanNode = new TMetaScanNode();
        metaScanNode.setCatalog(tvf.getMetadataTableName().getCtl());
        metaScanNode.setDatabase(tvf.getMetadataTableName().getDb());
        metaScanNode.setTable(tvf.getMetadataTableName().getTbl());
        metaScanNode.setTupleId(desc.getId().asInt());
        planNode.setMetaScanNode(metaScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        buildScanRanges();
    }

    @Override
    public boolean needToCheckColumnPriv() {
        return super.needToCheckColumnPriv();
    }

    private void buildScanRanges() {
        if (tvf.getMetaType() == MetadataTableValuedFunction.MetaType.ICEBERG) {
            IcebergTableValuedFunction icebergTvf = (IcebergTableValuedFunction) tvf;
            // todo: split
            TScanRangeLocations locations = createIcebergTvfLocations(icebergTvf);
            scanRangeLocations.add(locations);
        }
    }

    private TScanRangeLocations createIcebergTvfLocations(IcebergTableValuedFunction icebergTvf) {
        TScanRange scanRange = new TScanRange();
        TMetaScanRange metaScanRange = new TMetaScanRange();
        // set iceberg metadata params
        TIcebergMetadataParams icebergMetadataParams = new TIcebergMetadataParams();
        int metadataType = icebergTvf.getMetaQueryType().ordinal();
        icebergMetadataParams.setMetadataType(TIcebergMetadataType.findByValue(metadataType));

        metaScanRange.setIcebergParams(icebergMetadataParams);
        scanRange.setMetaScanRange(metaScanRange);
        // set location
        TScanRangeLocation location = new TScanRangeLocation();
        Backend backend = backendPolicy.getNextBe();
        location.setBackendId(backend.getId());
        location.setServer(new TNetworkAddress(backend.getIp(), backend.getBePort()));

        TScanRangeLocations result = new TScanRangeLocations();
        result.addToLocations(location);
        result.setScanRange(scanRange);
        return result;
    }
}
