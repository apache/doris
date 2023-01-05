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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.tablefunction.IcebergTableValuedFunction;
import org.apache.doris.tablefunction.MetadataTableValuedFunction;
import org.apache.doris.thrift.TMetaScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import java.util.List;

public class MetadataScanNode extends ScanNode {

    private MetadataTableValuedFunction tvf;

    public MetadataScanNode(PlanNodeId id, TupleDescriptor desc, MetadataTableValuedFunction tvf) {
        super(id, desc, "METADATA_SCAN_NODE", StatisticalType.METADATA_SCAN_NODE);
        this.tvf = tvf;
        if (tvf.getMetaType() == MetadataTableValuedFunction.MetaType.ICEBERG) {
            IcebergTableValuedFunction icebergTvf = (IcebergTableValuedFunction) tvf;
            icebergTvf.getMetaQueryType();
            // ignored
        }
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.META_SCAN_NODE);
        TMetaScanNode metaScanNode = new TMetaScanNode();
        metaScanNode.setTupleId(desc.getId().asInt());
        planNode.setMetaScanNode(metaScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        // j
        return null;
    }
}
