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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TGroupCommitScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;

import java.util.List;

public class GroupCommitScanNode extends ExternalScanNode {

    long tableId;

    public GroupCommitScanNode(PlanNodeId id, TupleDescriptor desc, long tableId) {
        super(id, desc, "GROUP_COMMIT_SCAN_NODE",
                StatisticalType.GROUP_COMMIT_SCAN_NODE, false);
        this.tableId = tableId;
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return Lists.newArrayList();
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNodeType(TPlanNodeType.GROUP_COMMIT_SCAN_NODE);
        TGroupCommitScanNode scanNode = new TGroupCommitScanNode();
        scanNode.setTableId(tableId);
        planNode.setGroupCommitScanNode(scanNode);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "GroupCommitScanNode";
    }
}
