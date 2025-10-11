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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

// Full scan of recursive cte temp table
public class RecursiveCteScanNode extends ScanNode {

    public RecursiveCteScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "RECURSIVE_CTE_SCAN", StatisticalType.CTE_SCAN_NODE);
    }

    public void initScanRangeLocations() throws UserException {
        createScanRangeLocations();
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList();
        // randomly select 1 backend
        List<Backend> backendList = Lists.newArrayList();
        for (Backend be : Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values()) {
            if (be.isAlive()) {
                backendList.add(be);
            }
        }
        if (backendList.isEmpty()) {
            throw new UserException("No Alive backends");
        }
        Collections.shuffle(backendList);
        Backend selectedBackend = backendList.get(0);

        // create scan range locations
        TScanRangeLocation location = new TScanRangeLocation();
        location.setBackendId(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.addToLocations(location);
        locations.setScanRange(new TScanRange());
        scanRangeLocations.add(locations);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public int getScanRangeNum() {
        return 1;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Recursive Cte: ").append(getTableIf().getName()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REC_CTE_SCAN_NODE;
    }

    @Override
    public boolean isSerialOperator() {
        return true;
    }
}
