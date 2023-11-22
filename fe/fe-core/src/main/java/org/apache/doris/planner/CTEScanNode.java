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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScanRangeLocations;

import java.util.List;

/**
 * The cte scan node is just a cte consumer wrapper which is convenient for collecting
 * cte target information.
 */
public class CTEScanNode extends ScanNode {
    private static final PlanNodeId UNINITIAL_PLANNODEID = new PlanNodeId(-1);

    public CTEScanNode(TupleDescriptor desc) {
        super(UNINITIAL_PLANNODEID, desc, "CTEScanNode", StatisticalType.CTE_SCAN_NODE);
    }

    public CTEScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "CTEScanNode", StatisticalType.CTE_SCAN_NODE);
    }

    public void setPlanNodeId(PlanNodeId id) {
        this.id = id;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        // NO real action to be taken, just a wrapper
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        // NO real action to be taken, just a wrapper
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        // NO real action to be taken, just a wrapper
        return null;
    }
}
