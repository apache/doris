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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNumbersScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

// TODO yiguolei: not accept any pushdown filters
public class NumbersTableValuedFunctionScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(NumbersTableValuedFunctionScanNode.class);

    private List<Backend> backendList;
    private List<TScanRangeLocations> shardScanRanges = Lists.newArrayList();
    private int numTablets;
    private long numbers;

    boolean isFinalized = false;

    public NumbersTableValuedFunctionScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, long numbers, int tabletNum) {
        super(id, desc, planNodeName);
        this.numbers = numbers;
        this.numTablets = tabletNum;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        assignBackends();
        computeStats(analyzer);
    }

    @Override
    public int getNumInstances() {
        return shardScanRanges.size();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return shardScanRanges;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        try {
            shardScanRanges = getShardLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        isFinalized = true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.NUMBERS_SCAN_NODE;
        TNumbersScanNode numbersScanNode = new TNumbersScanNode();
        numbersScanNode.setTotalNumbers(numbers);
        numbersScanNode.setTupleId(desc.getId().asInt());
        msg.numbers_scan_node = numbersScanNode;
    }

    private void assignBackends() throws UserException {
        backendList = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                backendList.add(be);
            }
        }
        if (backendList.isEmpty()) {
            throw new UserException("No Alive backends");
        }
        Collections.shuffle(backendList);
    }

    private List<TScanRangeLocations> getShardLocations() throws UserException {
        List<TScanRangeLocations> result = Lists.newArrayList();
        for (int i = 0, j = 0; i < numTablets; ++i, ++j) {
            // Locations
            TScanRangeLocations locations = new TScanRangeLocations();
            TScanRangeLocation location = new TScanRangeLocation();
            Backend be = backendList.get(j % backendList.size());
            location.setBackendId(be.getId());
            location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
            locations.addToLocations(location);
            // Set an empty scan range here, it is useless actually. But TScanRangeParams need it
            TScanRange scanRange = new TScanRange();
            locations.setScanRange(scanRange);
            result.add(locations);
        }
        System.out.println("number of scan ranges: " + result.size());
        return result;
    }
}
