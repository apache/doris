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
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.ExternalScanNode;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.tablefunction.DataGenTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionTask;
import org.apache.doris.thrift.TDataGenScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This scan node is used for data source generated from memory.
 */
public class DataGenScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(DataGenScanNode.class.getName());

    private DataGenTableValuedFunction tvf;
    private boolean isFinalized = false;

    public DataGenScanNode(PlanNodeId id, TupleDescriptor desc, DataGenTableValuedFunction tvf) {
        super(id, desc, "DataGenScanNode", StatisticalType.TABLE_VALUED_FUNCTION_NODE, false);
        this.tvf = tvf;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }
        createScanRangeLocations();
        isFinalized = true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DATA_GEN_SCAN_NODE;
        TDataGenScanNode dataGenScanNode = new TDataGenScanNode();
        dataGenScanNode.setTupleId(desc.getId().asInt());
        dataGenScanNode.setFuncName(tvf.getDataGenFunctionName());
        msg.data_gen_scan_node = dataGenScanNode;
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList();
        for (TableValuedFunctionTask task : tvf.getTasks()) {
            TScanRangeLocations locations = new TScanRangeLocations();
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(task.getBackend().getId());
            location.setServer(new TNetworkAddress(task.getBackend().getHost(), task.getBackend().getBePort()));
            locations.addToLocations(location);
            locations.setScanRange(task.getExecParams());
            scanRangeLocations.add(locations);
        }
    }

    @Override
    public void finalizeForNereids() {
        try {
            createScanRangeLocations();
        } catch (UserException e) {
            throw new NereidsException("Can not compute shard locations for DataGenScanNode: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean needToCheckColumnPriv() {
        return false;
    }

    // Currently DataGenScanNode is only used by DataGenTableValuedFunction, which is
    // inherited by NumbersTableValuedFunction.
    // NumbersTableValuedFunction is not a complete implementation for now, since its
    // function signature do not support us to split total numbers, so it can not be executed
    // by multi-processes or multi-threads. So we assign instance number to 1.
    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }

        StringBuilder output = new StringBuilder();

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }

        output.append(prefix).append("table value function: ").append(tvf.getDataGenFunctionName()).append("\n");



        return output.toString();
    }
}
